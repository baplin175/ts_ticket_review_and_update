"""
Activity text cleaning — HTML→text conversion, boilerplate removal,
signature stripping, and mojibake repair for TeamSupport ticket activities.
"""

import re
import unicodedata
from html import unescape
from typing import Dict

# ── Mojibake repair ──────────────────────────────────────────────────
MOJIBAKE_REPLACEMENTS = {
    "\u00e2\u0080\u0099": "\u2019",  # '
    "\u00e2\u0080\u009c": "\u201c",  # "
    "\u00e2\u0080\u009d": "\u201d",  # "
    "\u00e2\u0080\u0093": "\u2013",  # –
    "\u00e2\u0080\u0094": "\u2014",  # —
    "\u00e2\u0080\u00a6": "\u2026",  # …
    "\u00c2": "",
}


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    for bad, good in MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(bad, good)
    return text


def _fold_for_match(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


# ── Boilerplate / header / signature patterns ────────────────────────
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

SIGNATURE_LINE_PATTERNS = [
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

# ── Email signature / banner regex patterns ──────────────────────────
SIG_DELIM_RE = re.compile(r"(?m)^\s*--\s*$")
REPLY_HDR_BLOCK_RE = re.compile(
    r"(?mi)^(On .+?wrote:|From: .+|Sent: .+|To: .+|Subject: .+)\s*$"
)
QUOTED_LINE_RE = re.compile(r"(?m)^\s*>")
MOBILE_SENT_RE = re.compile(r"(?mi)^\s*Sent from my .+$")
DISCLAIMER_RE = re.compile(
    r"(?is)\b(confidential|privileged|intended recipient|unauthorized|disseminat|"
    r"legal notice|disclaimer|virus|intercepted|monitor(ed|ing)|important notice)\b"
)
EXTERNAL_WARNING_RE = re.compile(
    r"(?is)(?:^|\n)\s*(?:"
    r"CAUTION:\s*This email originated from outside.*?(?=\n\s*\n|$)"
    r"|ATTENTION:Ce courriel provient de l\u2019ext\u00e9rieur.*?(?=\n\s*\n|$)"
    r"|This Message Is From an External Sender.*?(?:ZjQcmQRYFpfptBannerEnd|\n\s*\n|$)"
    r"|This message came from outside your organization\.?.*?(?=\n\s*\n|$)"
    r")"
)
BILINGUAL_CAUTION_BLOCK_RE = re.compile(
    r"""(?is)
    (?:^|[\s\u201c\u201d""])
    (?:(?:Action\s+added\s+via\s+e-?mail\.?\s*)?(?:Sender:\s*\S+@\S+\s*)?)?
    (?:CAUTION:\s*)?This\s+email\s+originated\s+from\s+outside.*?
    /\s*ATTENTION:Ce\s+courriel\s+provient\s+de\s+l(?:'|\u2019|')ext\u00e9rieur\s+de\s+l(?:'|\u2019|')organisation\..*?
    s(?:u|\u00fb)r
    (?:[.!?])?
    """,
    re.X | re.UNICODE,
)
TRANSPORT_PREFIX_RE = re.compile(
    r"(?is)^\s*(?:action\s+added\s+via\s+e-?mail\.?\s*)?"
    r"(?:sender\s*:\s*\S+@\S+)\s*(?:[,:-]\s*)?"
    r"(?=(?:CAUTION:|This\s+email\s+originated|ATTENTION:|/\s*ATTENTION:))"
)
FR_WARN_FRAGMENT_RE = re.compile(
    r"(?is)\bles\s+liens\s+ou\s+ouvrir\s+les\s+pi[e\u00e8]ces\s+jointes.*?s(?:u|\u00fb)r(?=[\s\.!?]|$)"
)

EMAIL_RE = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w{2,}\b")
PHONE_RE = re.compile(r"\b(?:\+?\d[\d().\-\s]{7,})\b")
URL_RE = re.compile(r"(https?://|www\.)\S+", re.I)
TITLE_HINT_RE = re.compile(
    r"(?i)\b(CEO|CTO|CFO|COO|VP|Vice President|Director|Manager|Engineer|Consultant|"
    r"Administrator|Support|Customer Success|Sales|Marketing)\b"
)
PRONOUNS_RE = re.compile(r"(?i)\b(he/him|she/her|they/them)\b")
ADDRESS_HINT_RE = re.compile(
    r"(?i)\b(Suite|Ste\.|Street|St\.|Avenue|Ave\.|Road|Rd\.|Boulevard|Blvd\.|Drive|Dr\.|"
    r"Vancouver|Toronto|Seattle|BC|ON|WA|CA|USA|Canada)\b"
)


# ── Internal helpers ─────────────────────────────────────────────────

def _is_signaturey_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if re.match(r"(?i)^(thanks|thank you|regards|cheers|best|sincerely)[,!\s]*$", stripped):
        return True
    if EMAIL_RE.search(stripped) or PHONE_RE.search(stripped) or URL_RE.search(stripped):
        return True
    if TITLE_HINT_RE.search(stripped) or PRONOUNS_RE.search(stripped) or ADDRESS_HINT_RE.search(stripped):
        return True
    if re.search(r"(?i)\b(company|corp|inc\.?|llc|lp|harris)\b", stripped):
        return True
    return False


def _split_markers(text: str) -> str:
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


def _strip_boilerplate(text: str) -> str:
    text = _split_markers(text)
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        ls = line.strip()
        if not ls:
            continue
        for pat in INLINE_BOILERPLATE_PATTERNS:
            ls = re.sub(pat, "", ls, flags=re.IGNORECASE).strip()
        if not ls:
            continue
        folded = _fold_for_match(ls)
        if any(sub in folded for sub in BOILERPLATE_SUBSTRINGS):
            continue
        if any(re.match(pat, ls, re.IGNORECASE) for pat in HEADER_PATTERNS):
            continue
        ls = re.sub(
            r"\b(Thanks|Best|Regards|Sincerely|Thank you)[^\n]*$",
            "", ls, flags=re.IGNORECASE,
        ).strip()
        if not ls:
            continue
        if any(re.match(pat, ls, re.IGNORECASE) for pat in SIGNATURE_LINE_PATTERNS):
            continue
        if ls.lower() == "attachment:" or ls.lower().startswith("attachment:"):
            continue
        if "new office hours" in ls.lower():
            continue
        for pat in INLINE_PATTERNS:
            ls = re.sub(pat, "", ls)
        cleaned.append(ls)
    return "\n".join(cleaned)


def _dedupe_lines(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    seen: set = set()
    kept = []
    for line in lines:
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append(line)
    return "\n".join(kept)


def _strip_email_signature(text: str) -> str:
    if not text:
        return text
    t = text.strip()

    while BILINGUAL_CAUTION_BLOCK_RE.search(t):
        t = BILINGUAL_CAUTION_BLOCK_RE.sub(" ", t).strip()

    if EXTERNAL_WARNING_RE.search(t):
        t = EXTERNAL_WARNING_RE.sub("\n\n", t).strip()
        t = re.sub(r"\s*/\s*$", "", t)
        t = re.sub(r"\s*/\s*(?=\n|$)", " ", t)
        t = re.sub(r"\s{2,}", " ", t)

    t = TRANSPORT_PREFIX_RE.sub("", t).strip()
    t = FR_WARN_FRAGMENT_RE.sub("", t).strip()
    t = re.sub(r"\s{2,}", " ", t).strip()

    m = QUOTED_LINE_RE.search(t)
    if m:
        t = t[: m.start()].rstrip()
    m = REPLY_HDR_BLOCK_RE.search(t)
    if m:
        t = t[: m.start()].rstrip()
    m = SIG_DELIM_RE.search(t)
    if m:
        t = t[: m.start()].rstrip()
    m = MOBILE_SENT_RE.search(t)
    if m:
        t = t[: m.start()].rstrip()
    m = DISCLAIMER_RE.search(t)
    if m and m.start() > max(120, int(len(t) * 0.45)):
        t = t[: m.start()].rstrip()

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

    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t


# ── Public API ───────────────────────────────────────────────────────

def html_to_text(html: str) -> str:
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
    s = _strip_email_signature(s)
    return s


def clean_activity(text: str, *, is_html: bool = False) -> str:
    """Full cleaning pipeline for a single activity string."""
    if not text:
        return ""
    if is_html:
        t = html_to_text(text)
    else:
        t = text

    t = _normalize_text(t)
    t = _strip_boilerplate(t)

    # Collapse duplicate "On … wrote:" blocks
    blocks = re.split(r"(?=\bOn [A-Z][a-z]{2} \d{2}, \d{4} @)", t)
    blocks = [b.strip() for b in blocks if b.strip()]
    t = "\n".join(blocks)
    t = re.sub(
        r"(On [A-Z][a-z]{2} \d{2}, \d{4} @ [0-9: ]+(?:am|pm), [^:]+ wrote:)(?:\s*\1)+",
        r"\1",
        t,
    )

    t = _dedupe_lines(t)
    if not is_html:
        t = _strip_email_signature(t)

    t = re.sub(r"\s+", " ", t).strip()
    return t


_NAME_KEYS = ("CreatorName", "CreatedBy", "CreatedByName", "UserName", "Author")


def clean_activity_dict(action: Dict) -> Dict:
    """Clean a raw TS action dict and return a normalised record.

    Returns a dict with keys: action_id, created_at, action_type,
    creator_id, creator_name, party, description.
    """
    from ts_client import is_inhance_user

    raw_desc = action.get("Description") or action.get("Text") or ""
    # Unescape HTML entities first so double-encoded content is detected properly
    unescaped = raw_desc
    for _ in range(3):
        new = unescape(unescaped)
        if new == unescaped:
            break
        unescaped = new
    is_html = bool(re.search(r"<[a-zA-Z][^>]*>", unescaped))

    creator_id = str(
        action.get("CreatorID") or action.get("CreatorId") or ""
    ).strip()

    creator_name = ""
    for k in _NAME_KEYS:
        v = action.get(k)
        if isinstance(v, str) and v.strip():
            creator_name = v.strip()
            break

    party = "inh" if is_inhance_user(creator_id) else "cust"

    return {
        "action_id": str(
            action.get("ID") or action.get("Id") or action.get("ActionID") or ""
        ).strip(),
        "created_at": str(action.get("DateCreated") or action.get("CreatedOn") or "").strip(),
        "action_type": str(action.get("ActionType") or action.get("Type") or "").strip(),
        "creator_id": creator_id,
        "creator_name": creator_name,
        "party": party,
        "description": clean_activity(unescaped, is_html=is_html),
    }
