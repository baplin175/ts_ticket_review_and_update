"""
activity_cleaner.py — Unified ticket-activity text cleaning.

Combines the best of:
  - text_cleaning.py   (mojibake repair, boilerplate/header/signature pattern removal,
                         reply-chain splitting, line deduplication)
  - powman.py / comment_cleanser.py  (HTML→text, bilingual EN/FR caution-banner stripping,
                         transport-prefix removal, quoted-reply truncation, trailing
                         signature-cluster heuristic)

Public API
----------
    clean_activity(text, *, is_html=False, strip_signatures=True) -> str
    html_to_text(html, *, strip_signatures=True) -> str
    strip_email_signature(text) -> str
    normalize_text(text) -> str
    strip_boilerplate(text) -> str
"""

import re
import unicodedata
from html import unescape

__all__ = [
    "clean_activity",
    "html_to_text",
    "strip_email_signature",
    "normalize_text",
    "strip_boilerplate",
]


# ====================================================================
# 1. Mojibake / encoding repair  (from text_cleaning.py)
# ====================================================================
MOJIBAKE_REPLACEMENTS = {
    "\u00e2\u0080\u0099": "\u2019",   # '
    "\u00e2\u0080\u009c": "\u201c",   # "
    "\u00e2\u0080\u009d": "\u201d",   # "
    "\u00e2\u0080\u0093": "\u2013",   # –
    "\u00e2\u0080\u0094": "\u2014",   # —
    "\u00e2\u0080\u00a6": "\u2026",   # …
    "\u00c2": "",                       # stray Â from double-decoded UTF-8
}

def normalize_text(text: str) -> str:
    """Fix common mojibake artifacts."""
    if not text:
        return ""
    for bad, good in MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(bad, good)
    return text


def _fold_for_match(text: str) -> str:
    """Unicode-normalize + strip combining marks + lowercase for fuzzy matching."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


# ====================================================================
# 2. Boilerplate / header / inline-noise patterns  (from text_cleaning.py)
# ====================================================================
BOILERPLATE_SUBSTRINGS = [
    "caution: this email originated from outside of the organization",
    "attention:ce courriel provient de",
    "courriel provient de l'exterieur de l'organisation",
    "ne pas cliquer sur les liens ou ouvrir les pieces jointes",
    "do not type below this line",
    "this is an automated response",
    "confidentiality notice",
    "confidentiality/public record statement",
    "this message has been sent on behalf of",
    "please reply above this line",
    "ticket created via e-mail",
    "action added via e-mail",
    "manage submissions",
    "does this submission look like spam",
    "report it here",
    "append the message with the disclaimer",
]

HEADER_PATTERNS = [
    r"^From:.*",
    r"^Sent:.*",
    r"^To:.*",
    r"^Subject:.*",
    r"^Ticket #[0-9]+.*",
    r"^Ticket Update:.*",
    r"^Ticket closed\..*",
]

SIGNATURE_PATTERNS = [
    r"^thanks[ ,].*",
    r"^best[ ,].*",
    r"^sincerely[ ,].*",
    r"^regards[ ,].*",
    r"^thank you[ ,].*",
    r"^\d{3}[\.-]\d{3}[\.-]\d{4}.*",
    r"^\(\d{3}\) \d{3}[\.-]\d{4}.*",
    r"^\d{3}-\d{4}.*",
    r"^\d+\s+\w+\s+(street|st\.|road|rd\.|avenue|ave\.|blvd\.|boulevard|lane|ln\.|drive|dr\.|court|ct\.|p\.o\.|po box).*$",
    r"^www\..*$",
]

INLINE_PATTERNS = [
    r"\[cid:.*?\]",
    r"http[s]?://\S+",
    r"\bAttachment:\s*",
]

INLINE_BOILERPLATE_PATTERNS = [
    r"CAUTION:\s*This email originated from outside of the organization\.?\s*Do not click links or open attachments unless you recognize the sender and know the content is safe\.?",
    r"CAUTION:\s*This email originated from outside of [^\.]+\.?\s*DO NOT CLICK links or attachments unless you recognize the sender and/or know the content is safe\.?",
    r"/ATTENTION:.*",
]


def _split_markers(text: str) -> str:
    """Insert newlines before known reply-chain / sign-off markers so they land on their own lines."""
    text = re.sub(r"(\bOn [A-Z][a-z]{2} \d{2}, \d{4} @)", r"\n\1", text)
    text = re.sub(r"(\bFrom:)", r"\n\1", text)
    text = re.sub(r"(\bSent:)", r"\n\1", text)
    text = re.sub(r"(\bSubject:)", r"\n\1", text)
    text = re.sub(r"(\bTicket #\d+)", r"\n\1", text)
    text = re.sub(r"(\bTicket Update:)", r"\n\1", text)
    text = re.sub(r"(\bThanks[,\s])", r"\n\1", text, flags=re.IGNORECASE)
    text = re.sub(r"(\bBest[,\s])", r"\n\1", text, flags=re.IGNORECASE)
    text = re.sub(r"(\bRegards[,\s])", r"\n\1", text, flags=re.IGNORECASE)
    return text


def strip_boilerplate(text: str) -> str:
    """Remove boilerplate headers, inline noise, and known sign-off patterns (line-level)."""
    text = _split_markers(text)
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
        # Remove inline caution banners
        for pat in INLINE_BOILERPLATE_PATTERNS:
            line_stripped = re.sub(pat, "", line_stripped, flags=re.IGNORECASE).strip()
        if not line_stripped:
            continue
        # Substring boilerplate match (unicode-folded)
        line_folded = _fold_for_match(line_stripped)
        if any(sub in line_folded for sub in BOILERPLATE_SUBSTRINGS):
            continue
        # Header lines
        if any(re.match(pat, line_stripped, re.IGNORECASE) for pat in HEADER_PATTERNS):
            continue
        # Trailing sign-off words
        line_stripped = re.sub(
            r"\b(Thanks|Best|Regards|Sincerely|Thank you)[^\n]*$",
            "", line_stripped, flags=re.IGNORECASE,
        ).strip()
        if not line_stripped:
            continue
        # Signature-style lines (phone, address, website)
        if any(re.match(pat, line_stripped, re.IGNORECASE) for pat in SIGNATURE_PATTERNS):
            continue
        # Standalone "Attachment:" lines
        if line_stripped.lower() == "attachment:" or line_stripped.lower().startswith("attachment:"):
            continue
        if "new office hours" in line_stripped.lower():
            continue
        # Inline CID refs and URLs
        for pat in INLINE_PATTERNS:
            line_stripped = re.sub(pat, "", line_stripped)
        cleaned.append(line_stripped)
    return "\n".join(cleaned)


def _dedupe_lines(text: str) -> str:
    """Drop duplicate lines (case-insensitive)."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    seen: set[str] = set()
    kept: list[str] = []
    for line in lines:
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append(line)
    return "\n".join(kept)


# ====================================================================
# 3. Email-signature / banner regex patterns  (from powman.py & comment_cleanser.py)
# ====================================================================
SIG_DELIM_RE = re.compile(r'(?m)^\s*--\s*$')
REPLY_HDR_BLOCK_RE = re.compile(
    r'(?mi)^(On .+?wrote:|From: .+|Sent: .+|To: .+|Subject: .+)\s*$'
)
QUOTED_LINE_RE = re.compile(r'(?m)^\s*>')
MOBILE_SENT_RE = re.compile(r'(?mi)^\s*Sent from my .+$')
DISCLAIMER_RE = re.compile(
    r'(?is)\b(confidential|privileged|intended recipient|unauthorized|disseminat|'
    r'legal notice|disclaimer|virus|intercepted|monitor(ed|ing)|important notice)\b'
)
EXTERNAL_WARNING_RE = re.compile(
    r'(?is)(?:^|\n)\s*(?:'
    r'CAUTION:\s*This email originated from outside.*?(?=\n\s*\n|$)'
    r"|ATTENTION:Ce courriel provient de l\u2019ext\u00e9rieur.*?(?=\n\\s*\n|$)"
    r'|This Message Is From an External Sender.*?(?:ZjQcmQRYFpfptBannerEnd|\n\s*\n|$)'
    r'|This message came from outside your organization\.?.*?(?=\n\s*\n|$)'
    r')'
)
BILINGUAL_CAUTION_BLOCK_RE = re.compile(
    r"""(?is)
    (?:^|[\s\u201c\u201d""])
    (?:(?:Action\s+added\s+via\s+e-?mail\.?\s*)?(?:Sender:\s*\S+@\S+\s*)?)?
    (?:CAUTION:\s*)?This\s+email\s+originated\s+from\s+outside.*?
    /\s*ATTENTION:Ce\s+courriel\s+provient\s+de\s+l(?:'|\u2019|')ext\u00e9rieur\s+de\s+l(?:'|\u2019|')organisation\..*?
    s(?:u|\u00fb)r
    (?:[.!?])?
    """, re.X | re.UNICODE,
)
INLINE_CAUTION_RE = re.compile(
    r'(?is)Sender:\s*\S+@gmail\.com.*?(CAUTION:|ATTENTION:)', re.MULTILINE,
)
TRANSPORT_PREFIX_RE = re.compile(
    r'(?is)^\s*(?:action\s+added\s+via\s+e-?mail\.?\s*)?'
    r'(?:sender\s*:\s*\S+@\S+)\s*(?:[,:-]\s*)?'
    r'(?=(?:CAUTION:|This\s+email\s+originated|ATTENTION:|/\s*ATTENTION:))'
)
FR_WARN_FRAGMENT_RE = re.compile(
    r'(?is)\bles\s+liens\s+ou\s+ouvrir\s+les\s+pi[e\u00e8]ces\s+jointes.*?s(?:u|\u00fb)r(?=[\s\.!?]|$)'
)

EMAIL_RE = re.compile(r'\b[\w\.-]+@[\w\.-]+\.\w{2,}\b')
PHONE_RE = re.compile(r'\b(?:\+?\d[\d().\-\s]{7,})\b')
URL_RE   = re.compile(r'(https?://|www\.)\S+', re.I)
TITLE_HINT_RE = re.compile(
    r'(?i)\b(CEO|CTO|CFO|COO|VP|Vice President|Director|Manager|Engineer|Consultant|'
    r'Administrator|Support|Customer Success|Sales|Marketing)\b'
)
PRONOUNS_RE = re.compile(r'(?i)\b(he/him|she/her|they/them)\b')
ADDRESS_HINT_RE = re.compile(
    r'(?i)\b(Suite|Ste\.|Street|St\.|Avenue|Ave\.|Road|Rd\.|Boulevard|Blvd\.|Drive|Dr\.|'
    r'Vancouver|Toronto|Seattle|BC|ON|WA|CA|USA|Canada)\b'
)


def _is_signaturey_line(line: str) -> bool:
    """Heuristic: does this line look like part of an email signature/footer?"""
    stripped = line.strip()
    if not stripped:
        return True
    if re.match(r'(?i)^(thanks|thank you|regards|cheers|best|sincerely)[,!\s]*$', stripped):
        return True
    if EMAIL_RE.search(stripped) or PHONE_RE.search(stripped) or URL_RE.search(stripped):
        return True
    if TITLE_HINT_RE.search(stripped) or PRONOUNS_RE.search(stripped) or ADDRESS_HINT_RE.search(stripped):
        return True
    if re.search(r'(?i)\b(company|corp|inc\.?|llc|lp|harris)\b', stripped):
        return True
    return False


def strip_email_signature(text: str) -> str:
    """Remove email signatures, quoted replies, banners, and footers from plain text."""
    if not text:
        return text
    t = text.strip()

    # Bilingual CAUTION/ATTENTION inline block
    while BILINGUAL_CAUTION_BLOCK_RE.search(t):
        t = BILINGUAL_CAUTION_BLOCK_RE.sub(' ', t).strip()

    # External-warning banners (line-anchored)
    if EXTERNAL_WARNING_RE.search(t):
        t = EXTERNAL_WARNING_RE.sub('\n\n', t).strip()
        t = re.sub(r'\s*/\s*$', '', t)
        t = re.sub(r'\s*/\s*(?=\n|$)', ' ', t)
        t = re.sub(r'\s{2,}', ' ', t)

    # Inline "Sender: …gmail.com" + caution/attention
    if INLINE_CAUTION_RE.search(t):
        t = INLINE_CAUTION_RE.sub('', t).strip()

    # Transport prefix (only when a banner follows)
    t = TRANSPORT_PREFIX_RE.sub('', t).strip()

    # Orphaned French caution fragment
    t = FR_WARN_FRAGMENT_RE.sub('', t).strip()

    t = re.sub(r'\s{2,}', ' ', t).strip()

    # Quoted reply block ("> …")
    m = QUOTED_LINE_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # Reply header lines ("On … wrote:", Outlook From/Sent/To/Subject)
    m = REPLY_HDR_BLOCK_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # RFC 3676 sig delimiter ("-- ")
    m = SIG_DELIM_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # "Sent from my …" mobile footer
    m = MOBILE_SENT_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # Legal disclaimers (only when far enough into the text)
    m = DISCLAIMER_RE.search(t)
    if m and m.start() > max(120, int(len(t) * 0.45)):
        t = t[:m.start()].rstrip()

    # Trailing signature-cluster heuristic (bottom-up scan; cut at ≥3 consecutive sig-like lines)
    lines = t.splitlines()
    sig_run = 0
    cut_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if _is_signaturey_line(lines[i]):
            sig_run += 1
        else:
            if sig_run >= 3:
                cut_idx = i + 1
                break
            sig_run = 0
    if cut_idx is not None:
        lines = lines[:cut_idx]
        t = "\n".join(lines).rstrip()

    t = re.sub(r'[ \t]+\n', '\n', t)
    t = re.sub(r'\n{3,}', '\n\n', t).strip()
    return t


# ====================================================================
# 4. HTML → plain text  (from powman.py & comment_cleanser.py)
# ====================================================================
def html_to_text(html: str, *, strip_signatures: bool = True) -> str:
    """Flatten HTML to plain text and optionally strip signatures/footers/replies."""
    if not html:
        return ""
    s = html
    for _ in range(3):
        new_s = unescape(s)
        if new_s == s:
            break
        s = new_s

    s = s.replace("\xa0", " ").replace("\u00a0", " ").replace("\u202f", " ")
    s = re.sub(r"&(nbsp|NonBreakingSpace);", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<\s*br\s*/?>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"</\s*p\s*>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s).strip()

    if strip_signatures:
        s = strip_email_signature(s)
    return s


# ====================================================================
# 5. Combined pipeline  (text_cleaning + signature stripping)
# ====================================================================
def clean_activity(text: str, *, is_html: bool = False, strip_signatures: bool = True) -> str:
    """Full cleaning pipeline for a single ticket-activity string.

    Steps:
        1. If *is_html*, flatten HTML to plain text (with optional sig stripping).
        2. Repair mojibake.
        3. Remove boilerplate, headers, inline noise, and sign-off lines.
        4. Collapse duplicate reply-chain blocks.
        5. Deduplicate lines.
        6. If *strip_signatures* and input was NOT html (html path already stripped),
           run the regex-based signature stripper.
        7. Final whitespace normalization.
    """
    if not text:
        return ""

    if is_html:
        t = html_to_text(text, strip_signatures=strip_signatures)
    else:
        t = text

    # Mojibake repair
    t = normalize_text(t)

    # Boilerplate / header / inline-noise removal
    t = strip_boilerplate(t)

    # Collapse duplicate On … wrote: blocks
    blocks = re.split(r"(?=\bOn [A-Z][a-z]{2} \d{2}, \d{4} @)", t)
    blocks = [block.strip() for block in blocks if block.strip()]
    t = "\n".join(blocks)
    t = re.sub(
        r"(On [A-Z][a-z]{2} \d{2}, \d{4} @ [0-9: ]+(?:am|pm), [^:]+ wrote:)(?:\s*\1)+",
        r"\1",
        t,
    )

    # Line deduplication
    t = _dedupe_lines(t)

    # Signature stripping for plain-text inputs (html_to_text already did it)
    if strip_signatures and not is_html:
        t = strip_email_signature(t)

    # Final normalization
    t = re.sub(r"\s+", " ", t).strip()
    return t
