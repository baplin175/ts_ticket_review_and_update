import os
import sys
import json
import csv
from typing import Dict, List, Any, Tuple, Set

import requests
from datetime import datetime, timedelta, timezone
from html import unescape
import re

# All logic is self-contained in this file — no local imports.


# ====== Runtime settings (env overrides) ======
def _env_flag(name: str, default: str = "1") -> bool:
    return os.getenv(name, default) != "0"


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name, str(default)).strip()
    return int(val) if val else 0


MATCHA_COMPLETIONS_URL = os.getenv(
    "MATCHA_COMPLETIONS_URL",
    "https://matcha.harriscomputer.com/rest/api/v1/completions",
)  # Matcha AI API endpoint URL
MATCHA_TIMEOUT = _env_int("MATCHA_TIMEOUT", 600)  # HTTP timeout in seconds for Matcha requests
MATCHA_MISSION_ID = os.getenv("MATCHA_MISSION_ID", os.getenv("MATCHA_MISSION", "12848"))  # Matcha mission/model identifier
MATCHA_API_KEY = os.getenv("MATCHA_API_KEY", "")  # API key for Matcha authentication

TS_INPUT_MODE = os.getenv("TS_INPUT_MODE", "csv").strip().lower()  # Input source: "csv" or "api"
TS_CSV_PATH = os.getenv("TS_CSV_PATH", "").strip()  # Path to CSV file when using CSV input mode
TS_TARGET_TICKET_ID = os.getenv("TS_TARGET_TICKET_ID", "").strip()  # Process only this specific ticket ID
TARGET_TICKET_ID_DEFAULT = "108663"  # Default ticket to process if none specified
TS_OUTPUT_DIR = os.getenv("TS_OUTPUT_DIR", "").strip()  # Directory for output files

TS_MATCHA_MAX_TICKETS = _env_int("TS_MATCHA_MAX_TICKETS", 1)  # Maximum number of tickets to send to Matcha
TS_MATCHA_BATCH_SIZE = _env_int("TS_MATCHA_BATCH_SIZE", 30)  # Number of tickets per Matcha batch request

TS_PARALLEL = _env_flag("TS_PARALLEL", "1")  # Enable parallel processing of tickets
TS_PARALLEL_WORKERS = _env_int("TS_PARALLEL_WORKERS", 6)  # Number of concurrent worker threads

TS_WRITE_ACTIVITIES = _env_flag("TS_WRITE_ACTIVITIES", "1")  # Save ticket activities to JSON file
TS_WRITE_MATCHA_REQUEST = _env_flag("TS_WRITE_MATCHA_REQUEST", "1")  # Save Matcha request payload to file
TS_WRITE_MATCHA_RESPONSE = _env_flag("TS_WRITE_MATCHA_RESPONSE", "1")  # Save Matcha response to JSON file
TS_WRITE_MATCHA_TEXT = _env_flag("TS_WRITE_MATCHA_TEXT", "1")  # Save Matcha output as formatted text
TS_WRITE_MATCHA_CSV = _env_flag("TS_WRITE_MATCHA_CSV", "1")  # Save Matcha output as CSV file
TS_WRITE_BACK_AI = _env_flag("TS_WRITE_BACK_AI", "1")  # Write AI results back to TeamSupport tickets

TS_ECHO_COMMENT = os.getenv("TS_ECHO_COMMENT")  # CLI mode: just echo latest comment and exit
TS_EXTERNAL_ONLY = _env_flag("TS_EXTERNAL_ONLY", "1")  # Filter to external (non-employee) comments only
TS_CUSTOMER_ONLY = _env_flag("TS_CUSTOMER_ONLY", "1")  # Filter to customer-authored actions only
TS_STRIP_SIGNATURES = _env_flag("TS_STRIP_SIGNATURES", "1")  # Remove email signatures from comment text
TS_ACCOUNT_TZ = os.getenv("TS_ACCOUNT_TZ", "America/New_York")  # Timezone for date formatting

TS_EMPLOYEE_DOMAINS_RAW = os.getenv(
    "TS_EMPLOYEE_DOMAINS",
    "harriscomputer.com,csisoftware.com,constellationsoftware.com",
)  # Comma-separated list of employee email domains
TS_EMPLOYEE_NAMES_RAW = os.getenv("TS_EMPLOYEE_NAMES", "")  # Comma-separated list of employee names

DEFAULT_MATCHA_URL = MATCHA_COMPLETIONS_URL  # Fallback Matcha URL

# Incoming Webhook URL for Teams channel
# TEAMS_WEBHOOK_URL = os.getenv(
#     "TEAMS_WEBHOOK_URL",
#     "https://harriscomputer.webhook.office.com/webhookb2/052fc9f8-6fb1-41c5-adad-8b5c0ed28188@d0748657-f5a8-4d8b-8a16-b848c23ea8cb/IncomingWebhook/7b2d1d04e9324a3a8c7236286dfa6ef8/b48bc8ae-31dd-4451-8647-978f592c53a8/V2W3qnuzy3ZIs-gzD5_mrA9IWS1i0szNAtqtPH7cgfTP81-XXXX",
# )  # Microsoft Teams webhook URL for notifications (commented out)


def post_completion(
    mission_id: str,
    input_text: str,
    *,
    api_key: str,
    url: str = DEFAULT_MATCHA_URL,
    timeout: int = MATCHA_TIMEOUT,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Send input_text to the Matcha completions endpoint."""
    headers = {
        "Content-Type": "application/json",
        "MATCHA-API-KEY": api_key,
    }
    payload = {"mission_id": mission_id, "input": input_text}

    if verbose:
        print(f"[mai] POST {url}")
        print(f"[mai] Headers:\n{json.dumps(headers, indent=2, ensure_ascii=False)}")
        print(f"[mai] Body:\n{json.dumps(payload, indent=2, ensure_ascii=False)}")

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if verbose:
            print(f"[mai] Status: {resp.status_code}")
            print(f"[mai] Response text (first 500):\n{resp.text[:500]}")
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        body = getattr(e.response, "text", "") if getattr(e, "response", None) else ""
        raise RuntimeError(f"Matcha request failed: {e}. Body: {body}") from e


# ====== LLM structured output extraction ======
def extract_llm_struct(result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the first JSON object from the Matcha response's output_text field.
    Expected shape:
    {
      "status":"success",
      "output":[{"role":"assistant","content":[{"type":"output_text","text":"{...json...}"}]}]
    }
    Returns {} on failure.
    """
    try:
        outs = result.get("output") or []
        if not outs:
            return {}
        contents = outs[0].get("content") or []
        for item in contents:
            if item.get("type") == "output_text":
                txt = item.get("text") or ""
                txt = txt.strip()
                # Some models wrap JSON in code fences; strip them
                if txt.startswith("```"):
                    txt = txt.strip('`')
                    # remove leading language tag if present
                    if "\n" in txt:
                        txt = txt.split("\n", 1)[1]
                # Parse the JSON payload
                return json.loads(txt)
        return {}
    except Exception as e:
        print(f"[mai] Failed to parse LLM struct: {e}")
        return {}


# ====== TeamSupport ENV ======
TS_BASE = os.getenv("TS_BASE", "https://app.na2.teamsupport.com/api/json")
TS_KEY = os.getenv("TS_KEY", "")  # Your API key
TS_USER_ID = os.getenv("TS_USER_ID", "1189708")

if not all([TS_BASE, TS_KEY, TS_USER_ID]):
    raise ValueError("Missing TS creds. Set TS_BASE, TS_KEY, TS_USER_ID env vars.")


# ====== Output directory ======
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = TS_OUTPUT_DIR or os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ====== Time helpers (Account Time for display; UTC for TS filters) ======
# NOTE: Account TZ may be overridden with env var TS_ACCOUNT_TZ (e.g., "America/New_York")
def central_tz():
    try:
        # Python 3.9+; allow override via env var (e.g., "America/New_York")
        from zoneinfo import ZoneInfo
        return ZoneInfo(TS_ACCOUNT_TZ)
    except Exception:
        # Fallback fixed offset close to Eastern; does not adjust for DST
        return timezone(timedelta(hours=-4))


def iso_central(dt: datetime) -> str:
    tz = central_tz()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    # Drop microseconds for readability
    try:
        return dt.isoformat(timespec='seconds')
    except TypeError:
        # Older Python without timespec
        return dt.replace(microsecond=0).isoformat()


def one_hour_ago_iso() -> str:
    tz = central_tz()
    return iso_central(datetime.now(tz) - timedelta(hours=1))


def ts_compact_timestamp(dt: datetime) -> str:
    """TeamSupport 'between' format: YYYYMMDDHHMMSS (no separators) in UTC."""
    # TS docs require compact UTC datetimes for [bt]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S")


def iso_utc(dt: datetime) -> str:
    """ISO timestamp in UTC with 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_csv_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (key or "").strip().lower())


def _parse_csv_datetime(value: str):
    if not value:
        return None
    v = value.strip()
    for fmt in (
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M",
    ):
        try:
            dt = datetime.strptime(v, fmt)
            return dt.replace(tzinfo=central_tz())
        except Exception:
            continue
    return None


def build_payload_tickets_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Build per-ticket payloads from a prefiltered CSV export."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Normalize headers so minor casing/spacing changes don't break parsing.
    normalized_rows = []
    for row in rows:
        normalized = {_normalize_csv_key(k): (v or "").strip() for k, v in row.items()}
        normalized_rows.append(normalized)

    tickets: Dict[str, Dict[str, Any]] = {}
    actions_by_ticket: Dict[str, List[Dict[str, Any]]] = {}

    def _row_val(row: Dict[str, str], keys: Tuple[str, ...]) -> str:
        for k in keys:
            v = row.get(k, "")
            if v:
                return v
        return ""

    for row in normalized_rows:
        ticket_number = _row_val(row, ("ticketnumber",)).strip()
        if not ticket_number:
            continue
        if ticket_number not in tickets:
            tickets[ticket_number] = {
                "ticket_number": ticket_number,
                "ticket_name": _row_val(row, ("ticketname",)),
                "id": ticket_number,
                "date_created": _row_val(row, ("dateticketcreated",)),
                "date_modified": _row_val(row, ("dateticketmodified",)),
                "days_opened": _row_val(row, ("daysopened",)),
                "days_since_modified": _row_val(row, ("dayssinceticketwaslastmodified",)),
                "status": _row_val(row, ("status",)),
                "severity": _row_val(row, ("severity",)),
                "user_name": _row_val(row, ("assignedto",)),
            }
            actions_by_ticket[ticket_number] = []

        created_raw = _row_val(row, ("dateactioncreated", "actiondate"))
        desc = html_to_text(_row_val(row, ("actiondescription", "historydescription")))
        actions_by_ticket[ticket_number].append({
            "created_at": created_raw,
            "description": desc,
            "_dt": _parse_csv_datetime(created_raw) or datetime.min.replace(tzinfo=central_tz()),
        })

    payload_tickets: List[Dict[str, Any]] = []
    for ticket_number, meta in tickets.items():
        actions = actions_by_ticket.get(ticket_number, [])
        actions.sort(key=lambda a: a["_dt"], reverse=True)

        activities: List[Dict[str, Any]] = []
        if actions:
            for a in actions[:3]:
                activities.append({
                    "created_at": a.get("created_at", ""),
                    "description": a.get("description", ""),
                })
            latest_activity = activities[0]
        else:
            latest_activity = {
                "created_at": meta.get("date_modified", ""),
                "description": "No history rows found in CSV for this ticket.",
            }

        payload_tickets.append({
            **meta,
            "activities": activities,
            "latest_activity": latest_activity,
        })

    return payload_tickets


# ====== HTTP (TS) ======
def ts_get(url: str, params: Dict = None):
    import base64

    auth_bytes = f"{TS_USER_ID}:{TS_KEY}".encode("ascii")
    base64_auth = base64.b64encode(auth_bytes).decode("ascii")
    headers = {"Authorization": f"Basic {base64_auth}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


def ts_put(url: str, payload: Dict):
    import base64

    auth_bytes = f"{TS_USER_ID}:{TS_KEY}".encode("ascii")
    base64_auth = base64.b64encode(auth_bytes).decode("ascii")
    headers = {
        "Authorization": f"Basic {base64_auth}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    r = requests.put(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ====== Teams notification helper (disabled) ======
# def post_to_teams_card(webhook_url: str, title: str, fields: Dict[str, Any]) -> None:
#     """Post a simple Office 365 Connector card with key/value fields to Teams."""
#     if not webhook_url:
#         print("[teams] No TEAMS_WEBHOOK_URL set; skipping Teams post.")
#         return
#     # Build a simple markdown body listing key: value
#     lines = []
#     for k, v in fields.items():
#         lines.append(f"**{k}**: {v if v is not None else '—'}")
#     text = "\n\n".join(lines)
#
#     payload = {
#         "@type": "MessageCard",
#         "@context": "http://schema.org/extensions",
#         "summary": title,
#         "themeColor": "C43E1C",
#         "title": title,
#         "text": text,
#     }
#     try:
#         r = requests.post(webhook_url, json=payload, timeout=15)
#         print(f"[teams] Post status: {r.status_code}")
#         if r.status_code >= 400:
#             print(f"[teams] Response: {r.text[:400]}")
#     except Exception as e:
#         print(f"[teams] Error posting to Teams: {e}")


# ====== Parse/clean helpers ======
# ====== Signature stripping helpers ======
SIG_DELIM_RE = re.compile(r'(?m)^\s*--\s*$')                      # RFC 3676-style sig delimiter
REPLY_HDR_BLOCK_RE = re.compile(
    r'(?mi)^(On .+?wrote:|From: .+|Sent: .+|To: .+|Subject: .+)\s*$'
)
QUOTED_LINE_RE = re.compile(r'(?m)^\s*>')                         # quoted replies
MOBILE_SENT_RE = re.compile(r'(?mi)^\s*Sent from my .+$')
DISCLAIMER_RE = re.compile(
    r'(?is)\b(confidential|privileged|intended recipient|unauthorized|disseminat|'
    r'legal notice|disclaimer|virus|intercepted|monitor(ed|ing)|important notice)\b'
)
# External email warning banners (line-anchored single-language variants only)
EXTERNAL_WARNING_RE = re.compile(
    r'(?is)(?:^|\n)\s*(?:'
    r'CAUTION:\s*This email originated from outside.*?(?=\n\s*\n|$)'
    r'|ATTENTION:Ce courriel provient de l’extérieur.*?(?=\n\s*\n|$)'
    r'|This Message Is From an External Sender.*?(?:ZjQcmQRYFpfptBannerEnd|\n\s*\n|$)'
    r'|This message came from outside your organization\.?.*?(?=\n\s*\n|$)'
    r')'
)

# Precise bilingual EN/FR inline block (optional transport prefix + Sender: ... + EN/FR joined by "/")
BILINGUAL_CAUTION_BLOCK_RE = re.compile(
    r'''(?is)
    (?:^|[\s"“”])                             # start, whitespace, or quote
    (?:
        (?:Action\s+added\s+via\s+e-?mail\.?\s*)? # optional TS transport text
        (?:Sender:\s*\S+@\S+\s*)?                 # optional Sender: email
    )?
    (?:CAUTION:\s*)?This\s+email\s+originated\s+from\s+outside.*? # EN part
    /\s*ATTENTION:Ce\s+courriel\s+provient\s+de\s+l(?:’|'|’)extérieur\s+de\s+l(?:’|'|’)organisation\..*? # FR part
    s(?:u|û)r                                        # ends at "sûr/sur"
    (?:[.!?])?                                       # optional trailing punctuation
    ''', re.X | re.UNICODE)

# Extra: inline "Sender: ... gmail.com" with caution/attention
INLINE_CAUTION_RE = re.compile(
    r'(?is)Sender:\s*\S+@gmail\.com.*?(CAUTION:|ATTENTION:)', re.MULTILINE
)
# Remove TS transport prefixes like "Action added via e-mail. Sender: user@example.com"
# ...but ONLY when immediately followed by a known banner (EN/FR or bilingual)
TRANSPORT_PREFIX_RE = re.compile(
    r'(?is)^\s*(?:action\s+added\s+via\s+e-?mail\.?\s*)?'
    r'(?:sender\s*:\s*\S+@\S+)\s*(?:[,:-]\s*)?'
    r'(?=(?:CAUTION:|This\s+email\s+originated|ATTENTION:|/\s*ATTENTION:))'
)
# Catch orphaned French caution fragment, but stop strictly at the end-word ("sur/sûr")
FR_WARN_FRAGMENT_RE = re.compile(
    r'(?is)\bles\s+liens\s+ou\s+ouvrir\s+les\s+pi[eè]ces\s+jointes.*?s(?:u|û)r(?=[\s\.!?]|$)'
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
    """Heuristic: does this line look like part of a signature/footer?"""
    L = line.strip()
    if not L:                                      # blank lines inside sigs
        return True
    # very short “Thanks,” “Regards,” lines often mark start of sig block
    if re.match(r'(?i)^(thanks|thank you|regards|cheers|best|sincerely)[,!\s]*$', L):
        return True
    # contact + identity hints
    if EMAIL_RE.search(L) or PHONE_RE.search(L) or URL_RE.search(L):
        return True
    if TITLE_HINT_RE.search(L) or PRONOUNS_RE.search(L) or ADDRESS_HINT_RE.search(L):
        return True
    # common “company line” fragments
    if re.search(r'(?i)\b(company|corp|inc\.?|llc|lp|harris)\b', L):
        return True
    return False

def strip_email_signature(text: str) -> str:
    """Remove common email signatures / footers / quoted blocks from plain text."""
    if not text:
        return text
    t = text.strip()

    # First: surgically remove the inline bilingual CAUTION/ATTENTION block (keeps surrounding message text)
    while BILINGUAL_CAUTION_BLOCK_RE.search(t):
        t = BILINGUAL_CAUTION_BLOCK_RE.sub(' ', t).strip()

    # 0) Strip known external warning banners regardless of their position (now line-anchored only)
    if EXTERNAL_WARNING_RE.search(t):
        t = EXTERNAL_WARNING_RE.sub('\n\n', t).strip()
        # 0b) Clean up artifacts from bilingual inline banners (e.g., trailing "/" or double spaces)
        t = re.sub(r'\s*/\s*$', '', t)              # trailing slash at end
        t = re.sub(r'\s*/\s*(?=\n|$)', ' ', t)      # orphaned slash before newline/end
        t = re.sub(r'\s{2,}', ' ', t)               # collapse multiple spaces

    # Extra: strip inline "Sender:" Gmail + caution/attention fragments
    if INLINE_CAUTION_RE.search(t):
        t = INLINE_CAUTION_RE.sub('', t).strip()

    # Strip transport prefix only when a banner follows (guarded via lookahead in TRANSPORT_PREFIX_RE)
    t = TRANSPORT_PREFIX_RE.sub('', t).strip()

    # Remove orphaned French caution fragment when it survives without ATTENTION:
    t = FR_WARN_FRAGMENT_RE.sub('', t).strip()

    t = re.sub(r'\s{2,}', ' ', t).strip()

    # 1) Strip quoted reply content (keep only the new content above it)
    m = QUOTED_LINE_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # 2) Cut at reply header lines (e.g., "On Tue … wrote:", or Outlook headers)
    m = REPLY_HDR_BLOCK_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # 3) Standard sig delimiter (“--” on its own line)
    m = SIG_DELIM_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # 4) Mobile “Sent from my …”
    m = MOBILE_SENT_RE.search(t)
    if m:
        t = t[:m.start()].rstrip()

    # 5) Long legal/disclaimer blocks, but only if they appear toward the end
    m = DISCLAIMER_RE.search(t)
    if m and m.start() > max(120, int(len(t) * 0.45)):
        t = t[:m.start()].rstrip()
    # 6) Trim trailing signature-y cluster (bottom-up scan). We cut when we see
    #    >=3 consecutive “signature-like” lines near the end.
    lines = t.splitlines()
    sig_run = 0
    cut_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if _is_signaturey_line(lines[i]):
            sig_run += 1
        else:
            if sig_run >= 3:                       # a decent signature block
                cut_idx = i + 1
                break
            sig_run = 0
    if cut_idx is not None:
        lines = lines[:cut_idx]
        t = "\n".join(lines).rstrip()

    # normalize whitespace again (you use single-space collapsing, keep that style)
    t = re.sub(r'[ \t]+\n', '\n', t)
    t = re.sub(r'\n{3,}', '\n\n', t).strip()
    return t

# ====== Author filtering (customers vs employees) ======
def _split_env_list(val: str) -> List[str]:
    return [x.strip().lower() for x in (val or "").split(",") if x.strip()]

# Comma-separated corp domains and internal names (case-insensitive)
EMPLOYEE_DOMAINS = set(_split_env_list(TS_EMPLOYEE_DOMAINS_RAW))
EMPLOYEE_NAMES   = set(_split_env_list(TS_EMPLOYEE_NAMES_RAW))  # e.g., "shaena robertson, tony williams, susan thomas"

POSSIBLE_EMAIL_KEYS = ("CreatedByEmail","UserEmail","Email","From","ContactEmail","ReporterEmail")
POSSIBLE_NAME_KEYS  = ("CreatedBy","CreatedByName","UserName","OwnerName","AssignedToName","ActionBy","Author","Reporter")
POSSIBLE_FLAG_KEYS  = ("IsCustomer","Customer","IsPortalUser","IsExternal","IsInternal")

def _extract_email(a: Dict) -> str:
    for k in POSSIBLE_EMAIL_KEYS:
        v = a.get(k)
        if isinstance(v, str) and "@" in v:
            return v.strip()
    return ""

def _extract_name(a: Dict) -> str:
    for k in POSSIBLE_NAME_KEYS:
        v = a.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _email_domain(email: str) -> str:
    try:
        return email.split("@", 1)[1].lower()
    except Exception:
        return ""

def is_customer_actor(a: Dict) -> bool:
    """Heuristics to decide if an action was authored by a customer (not an employee)."""
    # 1) Trust explicit flags if present
    for k in POSSIBLE_FLAG_KEYS:
        if k in a:
            try:
                val = a.get(k)
                # Accept a variety of truthy encodings
                if isinstance(val, str):
                    val_norm = val.strip().lower()
                    if val_norm in {"true","yes","1"}:
                        return True
                    if val_norm in {"false","no","0"}:
                        return False
                return bool(val)
            except Exception:
                pass
    # 2) Email domain heuristic
    email = _extract_email(a)
    if email:
        dom = _email_domain(email)
        if dom and dom in EMPLOYEE_DOMAINS:
            return False
    # 3) Name list heuristic
    name = _extract_name(a).lower()
    if name and name in EMPLOYEE_NAMES:
        return False
    # 4) Default: treat as customer if we can't prove internal
    return True

def customer_only_enabled() -> bool:
    return TS_CUSTOMER_ONLY

# ====== inHANCE org users & external-comment filter ======
INHANCE_USER_IDS = None  # lazy-loaded set of string IDs for users in Organization=inHANCE

def _normalize_users_list(data: Dict) -> List[Dict]:
    if not isinstance(data, dict):
        return []
    u = data.get("Users") or data.get("User")
    if isinstance(u, list):
        return u
    if isinstance(u, dict):
        # Empty page shape like {"RecordsReturned":"0", ...}
        if u.get("RecordsReturned") == "0" and set(u.keys()) <= {"RecordsReturned", "NextPage", "TotalRecords"}:
            return []
        # Heuristic: single user object
        if any(k in u for k in ("ID", "Id", "UserID", "UserId", "Name", "Email")):
            return [u]
    return []

def fetch_inhance_user_ids() -> set:
    """Call /api/json/Users?Organization=inHANCE and cache the IDs."""
    try:
        data = ts_get(f"{TS_BASE}/Users", params={"Organization": "inHANCE"})
    except Exception as e:
        print(f"[ts] Failed to fetch inHANCE users: {e}")
        return set()
    users = _normalize_users_list(data)
    ids = set()
    for u in users:
        if isinstance(u, dict):
            val = u.get("ID") or u.get("Id") or u.get("UserID") or u.get("UserId")
            if val is not None:
                ids.add(str(val).strip())
    try:
        print(f"[dbg] Loaded inHANCE user IDs: {len(ids)}")
        print(f"[dbg] inHANCE user IDs list: {sorted(ids)}")
    except Exception:
        pass
    return ids

def inhance_user_ids() -> set:
    global INHANCE_USER_IDS
    if INHANCE_USER_IDS is None:
        INHANCE_USER_IDS = fetch_inhance_user_ids()
    return INHANCE_USER_IDS


def dedupe_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop duplicate action dicts based on stable identifiers."""
    seen: Set[Tuple[str, str, str]] = set()
    unique: List[Dict[str, Any]] = []
    for action in actions or []:
        if not isinstance(action, dict):
            continue
        aid = str(
            action.get("ID")
            or action.get("Id")
            or action.get("ActionID")
            or action.get("ActionId")
            or ""
        ).strip()
        created = str(action.get("DateCreated") or action.get("CreatedOn") or "").strip()
        desc = str(action.get("Description") or action.get("Text") or "").strip()
        key = (aid, created, desc)
        if key in seen:
            continue
        seen.add(key)
        unique.append(action)
    return unique

def _creator_id(a: Dict) -> str:
    # Primary per spec: CreatorID; fallbacks included defensively and with more casing variants
    for k in (
        "CreatorID", "CreatorId",
        "CreatedByID", "CreatedById",
        "AuthorID", "AuthorId",
        "UserID", "UserId"
    ):
        v = a.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""

def is_external_creator(a: Dict) -> bool:
    """Return True if the action's CreatorID is NOT in inHANCE user IDs."""
    cid = _creator_id(a)
    if not cid:
        return False  # cannot verify; exclude
    return cid not in inhance_user_ids()

def is_inhance_creator(a: Dict) -> bool:
    """Return True if the action's CreatorID is in inHANCE user IDs."""
    cid = _creator_id(a)
    if not cid:
        return False
    return cid in inhance_user_ids()
def parse_ts_datetime(value: str):
    if not value:
        return None
    v = value.strip()
    try:
        if v.endswith("Z"):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=central_tz())
        return dt.astimezone(central_tz())
    except Exception:
        pass
    try:
        dt = datetime.strptime(v, "%m/%d/%Y %I:%M %p")
        return dt.replace(tzinfo=central_tz())
    except Exception:
        return None


def action_created_dt(a: Dict):
    return (
        parse_ts_datetime(a.get("CreatedOn"))
        or parse_ts_datetime(a.get("DateCreated"))
        or parse_ts_datetime(a.get("DateModified"))
    )


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
    # Strip signatures/footers/replies after flattening to text (env-toggle supported)
    if TS_STRIP_SIGNATURES:
        s = strip_email_signature(s)
    return s


# ====== TS fetchers ======
def fetch_ticket(ticket_id: str) -> Dict:
    data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}")
    if isinstance(data, dict):
        if data.get("Tickets"):
            return data["Tickets"][0]
        if data.get("Ticket"):
            return data["Ticket"]
    return data or {}


def fetch_tickets_updated_since(since_iso: str) -> List[Dict]:
    items, page, page_size = [], 1, 500

    def normalize_ticket_list(data: Dict) -> List[Dict]:
        if not isinstance(data, dict):
            return []
        t = data.get("Tickets")
        if isinstance(t, list):
            return t
        if isinstance(t, dict):
            # Empty page shape: {"RecordsReturned":"0", ...}
            if t.get("RecordsReturned") == "0" and set(t.keys()) <= {"RecordsReturned", "NextPage", "TotalRecords"}:
                return []
            # Heuristic: single ticket object
            if any(k in t for k in ("ID", "TicketID", "TicketNumber", "DateModified", "Name")):
                return [t]
        if "Ticket" in data:
            tt = data["Ticket"]
            return tt if isinstance(tt, list) else [tt]
        return []

    def matches_open_filters(t: Dict) -> bool:
        product = t.get("ProductName") or t.get("Product") or ""
        name = t.get("Name") or ""
        status = t.get("Status") or t.get("StatusName") or ""
        due_raw = t.get("DueDate") or ""
        due_dt = parse_ts_datetime(str(due_raw)) if due_raw else None
        if due_dt:
            today = datetime.now(central_tz()).date()
            if due_dt.date() > today:
                return False

        is_power_manager = product == "PowerManager" or "PM" in product
        is_customer_info = "Customer Information" in name
        is_excluded_status = status in {
            "Pending (Customer Action Required)",
            "Confirm Resolution",
        }
        return is_power_manager and not is_customer_info and not is_excluded_status

    while True:
        params = {
            "isClosed": "False",
            "pageNumber": page,
            "pageSize": page_size,
            "GroupName": "Customer Support (CS)",
        }
        data = ts_get(f"{TS_BASE}/Tickets", params=params)
        if page == 1:
            try:
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"ts_open_tickets_{ts_label}.json"
                out_path = os.path.join(OUTPUT_DIR, fname)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"[ts] Wrote open-tickets response: {out_path}")
            except Exception as e:
                print(f"[ts] Failed to write open-tickets response: {e}")
        page_items = normalize_ticket_list(data)
        if not page_items:
            break
        for obj in page_items:
            if isinstance(obj, dict) and matches_open_filters(obj):
                items.append(obj)
        if len(page_items) < page_size:
            break
        page += 1

    try:
        print(f"[dbg] Tickets kept after open/filtered query: {len(items)}")
    except Exception:
        pass
    try:
        ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        fname = f"ts_open_tickets_filtered_{ts_label}.json"
        out_path = os.path.join(OUTPUT_DIR, fname)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"[ts] Wrote filtered open tickets: {out_path}")
    except Exception as e:
        print(f"[ts] Failed to write filtered open tickets: {e}")
    return items


def fetch_actions_since(ticket_id: str, since_iso: str) -> List[Dict]:
    items, page, page_size = [], 1, 100
    # Calculate between window in UTC: [now-60m, now]
    end_utc = datetime.now(timezone.utc).replace(microsecond=0)
    start_utc = end_utc - timedelta(hours=1)
    start_compact = ts_compact_timestamp(start_utc)
    end_compact = ts_compact_timestamp(end_utc)
    # Log the computed window for this ticket
    try:
        print(
            f"[ts] Actions window for ticket {ticket_id} (UTC): from {iso_utc(start_utc)} to {iso_utc(end_utc)} "
            f"(compact {start_compact} → {end_compact})"
        )
    except Exception:
        pass
    try:
        print(f"[dbg] Actions query for {ticket_id}: using datecreated[bt] and createdon[bt] between {start_compact} → {end_compact}")
    except Exception:
        pass
    while True:
        # Prefer server-side between filters on both possible field names
        params = [
            ("page", page),
            ("pageSize", page_size),
            ("datecreated[bt]", start_compact),
            ("datecreated[bt]", end_compact),
            ("createdon[bt]", start_compact),
            ("createdon[bt]", end_compact),
        ]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        page_items = data.get("Actions") if isinstance(data, dict) else data

        # Fallback: if nothing came back, retry without server filter and local-filter
        if not page_items:
            params_fallback = [("page", page), ("pageSize", page_size)]
            data_fb = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params_fallback)
            page_items = data_fb.get("Actions") if isinstance(data_fb, dict) else data_fb

        if not page_items:
            break

        for obj in page_items:
            if isinstance(obj, dict):
                items.append(obj)
        if len(page_items) < page_size:
            break
        page += 1
    # Oldest → newest
    items.sort(key=lambda a: action_created_dt(a) or datetime.min.replace(tzinfo=timezone.utc))
    # Local safety filter using the computed start_utc
    return [a for a in items if (action_created_dt(a) and action_created_dt(a) >= start_utc)]


# ====== Latest Action helper ======


def fetch_latest_action(ticket_id: str) -> Dict:
    """Return the most recent action dict for a ticket, or {} if none.
    Tries server-side ordering on DateCreated/CreatedOn; falls back to grabbing a page and local sorting.
    """
    # Try server-side order by DateCreated desc
    try:
        params = [("page", 1), ("pageSize", 1), ("OrderBy", "DateCreated desc")]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if isinstance(items, list) and items:
            return items[0]
    except Exception:
        pass

    # Try server-side order by CreatedOn desc
    try:
        params = [("page", 1), ("pageSize", 1), ("OrderBy", "CreatedOn desc")]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if isinstance(items, list) and items:
            return items[0]
    except Exception:
        pass

    # Fallback: fetch a small page, then local sort
    try:
        params = [("page", 1), ("pageSize", 50)]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if not isinstance(items, list) or not items:
            return {}
        items.sort(key=lambda a: action_created_dt(a) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items[0]
    except Exception:
        return {}


# Helper: fetch last N most recent actions for a ticket, server-side ordering, fallback to local sort
def fetch_recent_actions(ticket_id: str, limit: int = 3) -> List[Dict]:
    """Return up to `limit` most recent actions for a ticket (newest first)."""
    if limit <= 0:
        return []

    # Try server-side ordering by DateCreated desc
    try:
        params = [("page", 1), ("pageSize", limit), ("OrderBy", "DateCreated desc")]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if isinstance(items, list) and items:
            return items[:limit]
    except Exception:
        pass

    # Try server-side ordering by CreatedOn desc
    try:
        params = [("page", 1), ("pageSize", limit), ("OrderBy", "CreatedOn desc")]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if isinstance(items, list) and items:
            return items[:limit]
    except Exception:
        pass

    # Fallback: fetch a bigger page and local-sort
    try:
        params = [("page", 1), ("pageSize", max(50, limit))]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        items = data.get("Actions") if isinstance(data, dict) else data
        if not isinstance(items, list) or not items:
            return []
        items.sort(key=lambda a: action_created_dt(a) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items[:limit]
    except Exception:
        return []

# ====== Public API: fetch comments for a given ticket ======

def _is_comment_action(a: Dict) -> bool:
    """Return True if the TS action looks like a comment."""
    tval = (a.get("ActionType") or a.get("Type") or "").strip().lower()
    return tval == "comment" or "comment" in tval


def get_comments_texts(ticket_id: str, *, limit: int = 1, external_only: bool = True) -> List[str]:
    """
    Return up to `limit` most recent comment texts for a ticket (newest-first).
    - `external_only=True` filters out actions authored by inHANCE users (using CreatorID).
    - Text is cleaned to plain text and signatures are stripped via `html_to_text`.
    """
    if limit <= 0:
        return []

    # Grab more than we need so filters/dedup have room to operate
    recent_raw = fetch_recent_actions(ticket_id, limit=max(20, limit))
    if not recent_raw:
        return []

    # Filter to comments (and optionally external creators), then dedupe
    comments = [a for a in recent_raw if _is_comment_action(a)]
    if external_only:
        comments = [a for a in comments if is_external_creator(a)]

    comments = dedupe_actions(comments)
    if not comments:
        return []

    # fetch_recent_actions returns newest-first already
    out: List[str] = []
    for a in comments[:limit]:
        desc = html_to_text(a.get("Description") or a.get("Text") or "")
        out.append(desc)
    return out


def get_latest_comment_text(ticket_id: str, *, external_only: bool = True) -> str:
    """Return the most recent comment's cleaned text for the given ticket, or "" if none."""
    texts = get_comments_texts(ticket_id, limit=1, external_only=external_only)
    return texts[0] if texts else ""


def get_latest_comment(ticket_id: str, *, external_only: bool = True) -> Dict[str, Any]:
    """
    Return a dict with the most recent comment for the ticket:
      {"created_at": <str>, "description": <cleaned text>} or {} if none.
    """
    recent_raw = fetch_recent_actions(ticket_id, limit=20)
    if not recent_raw:
        return {}

    comments = [a for a in recent_raw if _is_comment_action(a)]
    if external_only:
        comments = [a for a in comments if is_external_creator(a)]
    comments = dedupe_actions(comments)
    if not comments:
        return {}

    latest = comments[0]  # newest-first
    created_raw = latest.get("DateCreated") or latest.get("CreatedOn") or ""
    desc = html_to_text(latest.get("Description") or latest.get("Text") or "")
    return {"created_at": created_raw, "description": desc}


# ====== Single-ticket payload (last three comment actions) ======

def build_single_ticket_payload_recent_comments(tid: str) -> Dict[str, Any]:
    """Build a compact payload for one ticket containing status/severity/user, and the last three **comment** actions (newest first)."""
    ticket = fetch_ticket(tid) or {}
    # Try to get more than 3 so we can filter to comments
    recent_raw = fetch_recent_actions(tid, limit=200)

    def is_commentish(a: Dict) -> bool:
        """
        Consider as a user comment if:
        - ActionType/Type contains any of: comment, email, message, portal, customer
        - OR there is non-empty Description/Text (freeform body)
        This broadens beyond strict 'Comment' so we include 'Action added via e-mail', portal messages, etc.
        """
        tval = (a.get("ActionType") or a.get("Type") or "").strip().lower()
        if any(key in tval for key in ("comment", "email", "message", "portal", "customer")):
            return True
        body = (a.get("Description") or a.get("Text") or "")
        return bool(isinstance(body, str) and body.strip())

    ticket_number = str(ticket.get("TicketNumber") or "").strip()
    date_created = str(ticket.get("DateCreated") or "").strip()
    date_modified = str(ticket.get("DateModified") or "").strip()
    days_opened = ticket.get("DaysOpened") or ""
    days_since_modified = ""
    try:
        dm_dt = parse_ts_datetime(date_modified)
        if dm_dt:
            now_dt = datetime.now(central_tz())
            days_since_modified = str((now_dt - dm_dt).days)
    except Exception:
        days_since_modified = ""
    ticket_name = str(ticket.get("Name") or "").strip()
    primary_customer = str(ticket.get("PrimaryCustomer") or "").strip()
    status = str(ticket.get("Status") or "").strip()
    severity = str(ticket.get("Severity") or "").strip()
    user_name = (
        ticket.get("UserName")
        or ticket.get("AssignedTo")
        or ticket.get("AssignedToName")
        or ticket.get("Assignee")
        or ticket.get("AssigneeName")
        or ticket.get("OwnerName")
        or ticket.get("Owner")
        or ticket.get("AssignedToUserName")
        or ""
    )
    user_name = str(user_name).strip()

    # Collect up to the last three external (non-inHANCE) comments, newest-first
    activities: List[Dict[str, Any]] = []
    for a in dedupe_actions(recent_raw or []):
        if not is_commentish(a):
            continue
        # if not is_external_creator(a):
        #     continue
        created_raw = a.get("DateCreated") or a.get("CreatedOn")
        desc = html_to_text(a.get("Description") or a.get("Text") or "")
        activities.append({"created_at": created_raw, "description": desc})
        if len(activities) >= 3:
            break
    # Ensure newest-first ordering (sort by created_at descending)
    try:
        activities.sort(key=lambda x: parse_ts_datetime(x.get("created_at") or ""), reverse=True)
    except Exception:
        pass

    # Determine latest activity; keep the newest at index 0 AND also expose it as latest_activity.
    if activities:
        # Newest-first already; keep the newest at index 0 AND also expose it as latest_activity.
        latest_activity = activities[0]
    else:
        # No external comment actions found in the recent set.
        # As a fallback, try to find the most recent EXTERNAL action among recent_raw (any type),
        # but prefer real message-like actions (commentish).
        latest_activity = None
        for a in (recent_raw or []):
            try:
                # Only accept if authored by a non-inHANCE (external) creator AND is commentish
                if is_external_creator(a) and is_commentish(a):
                    created_raw = a.get("DateCreated") or a.get("CreatedOn")
                    desc = html_to_text(a.get("Description") or a.get("Text") or "")
                    latest_activity = {"created_at": created_raw, "description": desc}
                    break
            except Exception:
                continue

        if latest_activity is None:
            latest_activity = {
                "created_at": ticket.get("DateModified") or "",
                "description": "No external (non‑inHANCE) customer comments found in the recent history."
            }
        # With no external comments, keep `activities` empty.
        activities = []

    # Optional debug: print context for latest_activity and activities
    try:
        la_dbg = latest_activity.get("created_at") if isinstance(latest_activity, dict) else ""
        print(f"[dbg] build_single_ticket_payload_recent_comments: latest_activity at {la_dbg}; activities_count={len(activities)}; external_only_enforced=True")
    except Exception:
        pass

    return {
        "ticket_number": ticket_number or tid,
        "ticket_name": ticket_name,
        "id": tid,
        "date_created": date_created,
        "date_modified": date_modified,
        "days_opened": days_opened,
        "days_since_modified": days_since_modified,
        "status": status,
        "severity": severity,
        "customer": primary_customer,
        "primary_customer": primary_customer,
        "PrimaryCustomer": primary_customer,
        "user_name": user_name,
        "activities": activities,  # last up to 2 (after removing latest)
        "latest_activity": latest_activity,
    }

# ====== Orchestration ======
def build_actions_json(since_iso: str) -> Dict[str, Any]:
    tickets = fetch_tickets_updated_since(since_iso)
    if not tickets:
        return {"tickets": [], "window": {"since": since_iso, "generated_at": iso_central(datetime.now(central_tz()))}}

    results: List[Dict[str, Any]] = []
    for t in tickets:
        tid = str(t.get("Id") or t.get("TicketId") or t.get("ID") or t.get("TicketID") or "").strip()
        if not tid:
            continue
        try:
            ticket = fetch_ticket(tid)
            actions = fetch_actions_since(tid, since_iso)
            # Resolve ticket number from ticket object or actions
            ticket_number = str(ticket.get("TicketNumber") or (actions[0].get("TicketNumber") if actions else "") or "").strip()

            # Pull status/severity/UserName from the ticket with safe fallbacks
            status = str(ticket.get("Status") or t.get("Status") or "").strip()
            severity = str(ticket.get("Severity") or t.get("Severity") or "").strip()
            user_name = (
                ticket.get("UserName")
                or ticket.get("AssignedTo")
                or ticket.get("AssignedToName")
                or ticket.get("Assignee")
                or ticket.get("AssigneeName")
                or ticket.get("OwnerName")
                or ticket.get("Owner")
                or ticket.get("AssignedToUserName")
                or ""
            )
            user_name = str(user_name).strip()
            try:
                print(f"[dbg] Ticket meta for {ticket_number}: status=\"{status}\" severity=\"{severity}\" user_name=\"{user_name}\"")
            except Exception:
                pass

            activities = []

            latest_created = ""
            latest_desc = ""

            if actions:
                # Select only comment actions authored by non-inHANCE users
                def _is_comment(a: Dict) -> bool:
                    tval = (a.get("ActionType") or a.get("Type") or "").strip().lower()
                    return tval == "comment" or "comment" in tval
                ext_comments = [a for a in actions if _is_comment(a) and is_external_creator(a)]
                # actions from fetch_actions_since are oldest→newest; take the last three and show newest-first
                last_three_ext = list(reversed(ext_comments[-3:]))
                for a in last_three_ext:
                    created_raw = a.get("DateCreated") or a.get("CreatedOn")
                    desc = html_to_text(a.get("Description") or a.get("Text") or "")
                    activities.append({
                        "created_at": created_raw,
                        "description": desc,
                    })
                # Keep newest item at index 0 and ALSO expose it via latest_* fields
                if activities:
                    latest_created = activities[0].get("created_at") or ""
                    latest_desc = activities[0].get("description") or ""
                else:
                    activities.append({
                        "created_at": ticket.get("DateModified") or "",
                        "description": "No external (non‑inHANCE) customer comments in the last hour.",
                    })
            else:
                # No explicit actions returned, but ticket was in modified window — include synthetic activity
                dm = ticket.get("DateModified") or ""
                activities.append({
                    "created_at": dm,
                    "description": "Ticket updated (no explicit action rows in last hour)",
                })

            # Latest and recent, using non-inHANCE author filter
            recent_raw = fetch_recent_actions(tid, 20)  # grab more to allow filtering
            def _is_comment(a: Dict) -> bool:
                tval = (a.get("ActionType") or a.get("Type") or "").strip().lower()
                return tval == "comment" or "comment" in tval
            recent_filtered = dedupe_actions([
                a for a in (recent_raw or []) if _is_comment(a) and is_external_creator(a)
            ])

            # If we didn't already pick a latest from activities, pick it from recent_filtered
            if not (latest_created or latest_desc):
                if recent_filtered:
                    latest = recent_filtered[0]  # fetch_recent_actions returns newest-first
                    latest_created = latest.get("DateCreated") or latest.get("CreatedOn") or ""
                    latest_desc = html_to_text(latest.get("Description") or latest.get("Text") or "")

            # Build recent_actions excluding anything already shown (activities and latest)
            seen_activity_pairs = {
                (
                    str(act.get("created_at") or "").strip(),
                    str(act.get("description") or "").strip(),
                )
                for act in activities
            }
            if latest_created or latest_desc:
                seen_activity_pairs.add((str(latest_created).strip(), str(latest_desc).strip()))

            recent_actions = []
            seen_recent = set(seen_activity_pairs)
            for ra in recent_filtered:
                rc = ra.get("DateCreated") or ra.get("CreatedOn") or ""
                rd = html_to_text(ra.get("Description") or ra.get("Text") or "")
                key = (str(rc).strip(), str(rd).strip())
                if key in seen_recent:
                    continue
                recent_actions.append({"created_at": rc, "description": rd})
                seen_recent.add(key)
                if len(recent_actions) >= 3:
                    break

            try:
                print(f"[dbg] Latest action for {ticket_number}: created=\"{latest_created}\"; recent_count={len(recent_actions)}")
            except Exception:
                pass

            results.append({
                "ticket_number": ticket_number,
                "id": tid,
                "status": status,
                "severity": severity,
                "assigned": user_name,
                "activities": activities,
                "latest_activity": {
                    "created_at": latest_created,
                    "description": latest_desc,
                },
                "recent_actions": recent_actions
            })
        except Exception as e:
            print(f"[poll] Error building actions for ticket {tid}: {e}")

    return {
        "tickets": results,
        "window": {"since": since_iso, "generated_at": iso_central(datetime.now(central_tz()))},
    }


def build_ticket_payload(tid: str, since_iso: str) -> Dict[str, Any]:
    """Build a minimal per-ticket payload with the last three comment actions in the window."""
    ticket = fetch_ticket(tid)
    actions = fetch_actions_since(tid, since_iso)
    if not actions:
        return {}
    # Filter to comments only when possible
    def is_comment(a: Dict) -> bool:
        tval = (a.get("ActionType") or a.get("Type") or "").strip().lower()
        return tval == "comment" or "comment" in tval

    comments = dedupe_actions([a for a in actions if is_comment(a)]) or dedupe_actions(actions)
    comments = [a for a in comments if is_external_creator(a)]
    last_three = comments[-3:]  # oldest→newest slice
    ticket_number = str(ticket.get("TicketNumber") or (actions[0].get("TicketNumber") if actions else "") or "").strip()
    activities: List[Dict[str, Any]] = []
    for a in reversed(last_three):  # newest-first
        created_raw = a.get("DateCreated") or a.get("CreatedOn")
        desc = html_to_text(a.get("Description") or a.get("Text") or "")
        activities.append({
            "created_at": created_raw,
            "description": desc,
        })
    if not activities:
        activities.append({
            "created_at": ticket.get("DateModified") or "",
            "description": "No external (non‑inHANCE) customer comments in the window.",
        })
    return {
        "ticket_number": ticket_number or tid,
        "activities": activities,
        "window": {"since": since_iso, "generated_at": iso_central(datetime.now(central_tz()))},
    }

def resolve_ticket_id(ticket_number: str) -> str:
    """Resolve a TeamSupport TicketNumber to its internal ID."""
    if not ticket_number:
        return ""
    try:
        data = ts_get(f"{TS_BASE}/Tickets", params={"TicketNumber": ticket_number})
    except Exception as e:
        print(f"[ts] Failed to resolve TicketNumber {ticket_number}: {e}")
        return ""

    def normalize_ticket_list(data_obj: Dict) -> List[Dict]:
        if not isinstance(data_obj, dict):
            return []
        t = data_obj.get("Tickets")
        if isinstance(t, list):
            return t
        if isinstance(t, dict):
            if t.get("RecordsReturned") == "0" and set(t.keys()) <= {"RecordsReturned", "NextPage", "TotalRecords"}:
                return []
            if any(k in t for k in ("ID", "TicketID", "TicketNumber", "DateModified", "Name")):
                return [t]
        if "Ticket" in data_obj:
            tt = data_obj["Ticket"]
            return tt if isinstance(tt, list) else [tt]
        return []

    items = normalize_ticket_list(data)
    if not items:
        return ""
    tid = str(items[0].get("ID") or items[0].get("Id") or items[0].get("TicketID") or "").strip()
    return tid


def _ai_update_timestamp() -> str:
    return datetime.now(central_tz()).strftime("%Y-%m-%d %H:%M:%S")

def _inhance_last_comment_timestamp(ticket_id: str) -> str:
    """Return timestamp of the most recent inHANCE-authored action, or "" if none."""
    recent_raw = fetch_recent_actions(ticket_id, limit=200)
    if not recent_raw:
        return ""
    for a in recent_raw:  # newest-first
        if is_inhance_creator(a):
            dt = action_created_dt(a)
            if dt:
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(a.get("DateCreated") or a.get("CreatedOn") or "").strip()
    return ""


def _matcha_header_index(header: List[str], name: str) -> int:
    name_norm = name.strip().lower()
    for i, col in enumerate(header):
        if col.strip().lower() == name_norm:
            return i
    return -1


def write_back_ai_fields(rows: List[List[str]]) -> None:
    """Write AIPriority/AIPriExpln/AILastUpdate/LastInhComment back to TeamSupport tickets."""
    if not rows or len(rows) < 2:
        print("[ts] No Matcha rows to write back.")
        return

    header = rows[0]
    idx_ticket = _matcha_header_index(header, "Ticket Number")
    idx_priority = _matcha_header_index(header, "Priority")
    idx_expl = _matcha_header_index(header, "Priority Explanation")
    if min(idx_ticket, idx_priority, idx_expl) < 0:
        print("[ts] Matcha header missing required columns; skipping write-back.")
        return

    updated = 0
    writeback_payloads: List[Dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) <= max(idx_ticket, idx_priority, idx_expl):
            continue
        ticket_number = str(row[idx_ticket]).strip()
        priority = str(row[idx_priority]).strip()
        explanation = str(row[idx_expl]).strip()
        if not ticket_number or not priority or not explanation:
            continue
        ticket_id = resolve_ticket_id(ticket_number)
        if not ticket_id:
            print(f"[ts] Could not resolve TicketNumber {ticket_number}; skipping.")
            continue
        last_inh_comment = _inhance_last_comment_timestamp(ticket_id)
        payload = {
            "Ticket": {
                "AIPriority": priority,
                "AIPriExpln": explanation,
                "AILastUpdate": _ai_update_timestamp(),
            }
        }
        if last_inh_comment:
            payload["Ticket"]["LastInhComment"] = last_inh_comment
        writeback_payloads.append(
            {
                "ticket_number": ticket_number,
                "ticket_id": ticket_id,
                "payload": payload,
            }
        )
        try:
            ts_put(f"{TS_BASE}/Tickets/{ticket_id}", payload)
            updated += 1
        except Exception as e:
            print(f"[ts] Failed to write AI fields for {ticket_number}: {e}")

    if writeback_payloads:
        try:
            ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            fname = f"ts_writeback_{ts_label}.json"
            out_path = os.path.join(OUTPUT_DIR, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(writeback_payloads, f, ensure_ascii=False, indent=2)
            print(f"[ts] Wrote write-back payloads: {out_path}")
        except Exception as e:
            print(f"[ts] Failed to write write-back payloads: {e}")

    print(f"[ts] AI fields written to {updated} ticket(s).")


def main():
    # since_iso = one_hour_ago_iso()
    input_mode = TS_INPUT_MODE
    csv_path = TS_CSV_PATH
    use_csv = input_mode == "csv" or bool(csv_path)
    target_ticket_id = TS_TARGET_TICKET_ID
    if not target_ticket_id:
        target_ticket_id = TARGET_TICKET_ID_DEFAULT
    target_ticket_id_specified = bool(target_ticket_id) and (
        "TS_TARGET_TICKET_ID" in os.environ or bool(TARGET_TICKET_ID_DEFAULT)
    )

    mission_id = MATCHA_MISSION_ID
    api_key = MATCHA_API_KEY
    if not api_key:
        print("[mai] Missing MATCHA_API_KEY; set it to send.")
        return

    max_matcha_tickets = TS_MATCHA_MAX_TICKETS
    if use_csv:
        if not csv_path:
            csv_path = "PMAN Actions.csv"
        payload_tickets = build_payload_tickets_from_csv(csv_path)
        if not payload_tickets:
            print("[mai] No tickets found in CSV; nothing to send.")
            return
        if target_ticket_id_specified:
            payload_tickets = [
                t for t in payload_tickets
                if str(t.get("ticket_number") or t.get("TicketNumber") or "").strip() == target_ticket_id
            ]
            if not payload_tickets:
                print(f"[mai] Ticket {target_ticket_id} not found in CSV; nothing to send.")
                return
        if max_matcha_tickets > 0:
            payload_tickets = payload_tickets[:max_matcha_tickets]
    else:
        tickets: List[Dict[str, Any]]
        if target_ticket_id:
            # In single-ticket mode, avoid extra TS calls (e.g., Users lookup).
            global INHANCE_USER_IDS
            INHANCE_USER_IDS = set()
            try:
                print(f"[mai] Targeting ticket number {target_ticket_id} (TS_TARGET_TICKET_ID).")
            except Exception:
                pass
            tickets = [{"TicketNumber": target_ticket_id}]
        else:
            # Eagerly load & log inHANCE user IDs so they appear at the top of the log
            try:
                _ = inhance_user_ids()
            except Exception as e:
                print(f"[dbg] Unable to preload inHANCE user IDs: {e}")
            # tickets = fetch_tickets_updated_since(since_iso)
            tickets = fetch_tickets_updated_since("")
            if not tickets:
                print("[mai] No open tickets matched filters; nothing to send.")
                return

        payload_tickets = []
        ticket_ids: List[str] = []
        for t in tickets:
            if max_matcha_tickets > 0 and len(ticket_ids) >= max_matcha_tickets:
                break
            tid = str(t.get("Id") or t.get("TicketId") or t.get("ID") or t.get("TicketID") or "").strip()
            if not tid:
                ticket_number = str(t.get("TicketNumber") or "").strip()
                if ticket_number:
                    tid = resolve_ticket_id(ticket_number)
            if not tid:
                continue
            ticket_ids.append(tid)

        def _build_single(tid: str) -> Dict[str, Any]:
            return build_single_ticket_payload_recent_comments(tid)

        use_parallel = TS_PARALLEL
        if use_parallel and ticket_ids:
            max_workers = TS_PARALLEL_WORKERS
            try:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futures = {ex.submit(_build_single, tid): tid for tid in ticket_ids}
                    for fut in as_completed(futures):
                        tid = futures[fut]
                        try:
                            single = fut.result()
                            if single:
                                payload_tickets.append(single)
                        except Exception as e:
                            print(f"[mai] Error building payload for ticket {tid}: {e}")
            except Exception as e:
                print(f"[mai] Parallel build failed; falling back to sequential. Error: {e}")
                use_parallel = False

        if not use_parallel:
            for tid in ticket_ids:
                try:
                    single = _build_single(tid)
                    if single:
                        payload_tickets.append(single)
                except Exception as e:
                    print(f"[mai] Error building payload for ticket {tid}: {e}")

    if not payload_tickets:
        print("[mai] No tickets to send to Matcha.")
        return

    def _extract_matcha_lines(result_obj: Dict[str, Any]) -> List[str]:
        if not isinstance(result_obj, dict):
            return []
        out = result_obj.get("output") or []
        if not out or not isinstance(out, list):
            return []
        content = out[0].get("content") if isinstance(out[0], dict) else []
        if not content or not isinstance(content, list):
            return []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "output_text":
                raw = item.get("text") or ""
                return [ln for ln in raw.splitlines() if ln.strip()]
        return []

    def _priority_key(row: List[str]) -> int:
        if len(row) < 4:
            return 9999
        pval = str(row[3]).strip()
        m = re.match(r"^(\d+)", pval)
        return int(m.group(1)) if m else 9999

    def _parse_matcha_rows(lines: List[str]) -> List[List[str]]:
        rows_out: List[List[str]] = []
        if not lines:
            return rows_out
        header_parts = lines[0].split(" | ")
        rows_out.append(header_parts)
        has_customer = any(p.strip().lower() == "customer" for p in header_parts)
        tail_len = 4 if has_customer or len(header_parts) >= 9 else 3
        for line in lines[1:]:
            parts = line.split(" | ")
            if len(parts) < (4 + 1 + tail_len):
                continue
            if len(parts) == (4 + 1 + tail_len):
                rows_out.append(parts)
                continue
            head = parts[:4]
            tail = parts[-tail_len:]
            explanation = " | ".join(parts[4:-tail_len])
            rows_out.append(head + [explanation] + tail)
        return rows_out

    def _process_batch(batch_tickets: List[Dict[str, Any]], batch_idx: int, batch_total: int) -> None:
        batch_tag = f"_b{batch_idx:03d}" if batch_total > 1 else ""
        payload = {
            "tickets": batch_tickets,
            # "window": {"since": since_iso, "generated_at": iso_central(datetime.now(central_tz()))},
            "window": {"generated_at": iso_central(datetime.now(central_tz()))},
            "format_hint": "Ticket Number | Ticket Name | Severity | Priority | Priority Explanation | Days Opened | Days Since Ticket Was Last Modified | Assignee | Customer (Ticket Name must come from the ticket Name field; Assignee uses user_name, or 'Null' if missing; Customer uses PrimaryCustomer from the ticket)",
        }
        if TS_WRITE_ACTIVITIES:
            try:
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"ts_ticket_activities_{ts_label}{batch_tag}.json"
                out_path = os.path.join(OUTPUT_DIR, fname)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(batch_tickets, f, ensure_ascii=False, indent=2)
                print(f"[ts] Wrote ticket activities: {out_path}")
            except Exception as e:
                print(f"[ts] Failed to write ticket activities: {e}")
        if TS_WRITE_MATCHA_REQUEST:
            try:
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"matcha_request_{ts_label}{batch_tag}.json"
                out_path = os.path.join(OUTPUT_DIR, fname)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                print(f"[mai] Wrote Matcha request: {out_path}")
            except Exception as e:
                print(f"[mai] Failed to write Matcha request: {e}")

        if batch_total > 1:
            print(f"[mai] Sending Matcha batch {batch_idx}/{batch_total} ({len(batch_tickets)} tickets).")

        result = post_completion(mission_id, json.dumps(payload, ensure_ascii=False), api_key=api_key)
        if TS_WRITE_MATCHA_RESPONSE:
            try:
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"matcha_response_{ts_label}{batch_tag}.json"
                out_path = os.path.join(OUTPUT_DIR, fname)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                print(f"[mai] Wrote Matcha response: {out_path}")
            except Exception as e:
                print(f"[mai] Failed to write Matcha response: {e}")

        text_lines = _extract_matcha_lines(result)
        rows: List[List[str]] = _parse_matcha_rows(text_lines) if text_lines else []

        if rows:
            header, data_rows = rows[0], rows[1:]
            data_rows.sort(key=_priority_key)
            rows = [header] + data_rows

        if TS_WRITE_MATCHA_TEXT:
            try:
                text_out = "\n".join(" | ".join(r) for r in rows) if rows else ""
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"matcha_output_{ts_label}{batch_tag}.txt"
                out_path = os.path.join(OUTPUT_DIR, fname)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(text_out)
                print(f"[mai] Wrote Matcha text output: {out_path}")
            except Exception as e:
                print(f"[mai] Failed to write Matcha text output: {e}")
        if TS_WRITE_MATCHA_CSV:
            try:
                ts_label = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fname = f"matcha_output_{ts_label}{batch_tag}.csv"
                out_path = os.path.join(OUTPUT_DIR, fname)
                import csv
                with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(rows)
                print(f"[mai] Wrote Matcha CSV output: {out_path}")
            except Exception as e:
                print(f"[mai] Failed to write Matcha CSV output: {e}")
        if TS_WRITE_BACK_AI:
            try:
                write_back_ai_fields(rows)
            except Exception as e:
                print(f"[ts] AI write-back failed: {e}")
        if len(batch_tickets) <= 20:
            print("[mai] LLM response:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print("[mai] LLM response suppressed (more than 20 tickets). See output files.")

    batch_size = TS_MATCHA_BATCH_SIZE
    batches = [payload_tickets[i:i + batch_size] for i in range(0, len(payload_tickets), batch_size)]
    for idx, batch in enumerate(batches, start=1):
        _process_batch(batch, idx, len(batches))


if __name__ == "__main__":
    # If TS_ECHO_COMMENT is set, act as a tiny CLI to fetch and print the latest comment
    # for the ticket specified by TS_TARGET_TICKET_ID, then exit. This keeps the module usable
    # as a script without invoking the LLM/Teams workflow when the caller only wants a comment.
    if TS_ECHO_COMMENT:
        tid = TS_TARGET_TICKET_ID
        if not tid:
            print("[cli] TS_TARGET_TICKET_ID is required when TS_ECHO_COMMENT is set.")
            sys.exit(2)
        external_only = TS_EXTERNAL_ONLY
        latest = get_latest_comment(tid, external_only=external_only)
        if latest:
            print(json.dumps(latest, ensure_ascii=False))
            sys.exit(0)
        else:
            print("{}")
            sys.exit(1)

    # Default behavior: run the existing orchestration (Teams posting disabled).
    main()
