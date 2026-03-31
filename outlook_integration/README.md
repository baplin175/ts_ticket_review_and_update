# outlook_integration

Self-contained Microsoft Graph API client for Outlook on Office 365.  
Drop this folder into any Python project and go.

## Features

| Area | Operations |
|------|-----------|
| **Auth** | Device-code flow (delegated), client-credentials (app-only), persistent token cache |
| **Mail** | List inbox, get message, full-text search (KQL), send, reply-all, list folders |
| **Calendar** | List events, get event, calendarView (time window), create, update, delete, list calendars, Teams meeting links |

## Setup

### 1. Azure AD App Registration

1. Go to [Azure Portal → App registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade).
2. Click **New registration**.
3. Name it (e.g. "Graph Outlook Integration").
4. Under **Redirect URIs**, add `https://login.microsoftonline.com/common/oauth2/nativeclient` (type: Mobile/Desktop) for device-code flow.
5. Note the **Application (client) ID** and **Directory (tenant) ID**.
6. For client-credentials flow: go to **Certificates & secrets → New client secret** and copy the value.

### 2. API Permissions

Add these under **API permissions → Microsoft Graph**:

| Permission | Type | Needed for |
|-----------|------|-----------|
| `Mail.ReadWrite` | Delegated | Read/search email |
| `Mail.Send` | Delegated | Send email |
| `Calendars.ReadWrite` | Delegated | Read/create events |
| `User.Read` | Delegated | Basic profile |
| `Mail.ReadWrite` | Application | App-only mail access |
| `Mail.Send` | Application | App-only send |
| `Calendars.ReadWrite` | Application | App-only calendar |

Click **Grant admin consent** for the application permissions.

### 3. Configure

```bash
cp .env.example .env
```

Fill in your values:

```
GRAPH_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
GRAPH_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
GRAPH_CLIENT_SECRET=your-secret-here     # only needed for client-credentials
```

### 4. Install dependencies

```bash
pip install -r outlook_integration/requirements.txt
```

## Usage

### Delegated (device-code) — interactive

```python
from outlook_integration import get_delegated_token, list_messages, send_mail

token = get_delegated_token()
# First run prints a URL + code to stderr — open in browser and sign in.
# Subsequent runs use the cached token silently.

# Read latest 10 inbox messages
messages = list_messages(token, top=10)
for m in messages:
    print(m["subject"], m["from"]["emailAddress"]["address"])

# Send an email
send_mail(
    token,
    subject="Hello from Graph",
    body="<p>This is a test.</p>",
    to=["recipient@example.com"],
)
```

### App-only (client credentials) — no user interaction

```python
from outlook_integration import get_app_token, list_messages

token = get_app_token()

# App-only requires user_id since there's no /me context
messages = list_messages(token, user_id="user@contoso.com", top=5)
```

### Calendar

```python
from outlook_integration import get_delegated_token, list_events, create_event

token = get_delegated_token()

# List upcoming events
events = list_events(token, top=10)

# Events in a date range
from outlook_integration import calendar_view
events = calendar_view(token, start="2025-04-01T00:00:00", end="2025-04-30T23:59:59")

# Create an event with a Teams meeting link
new = create_event(
    token,
    subject="Sprint Review",
    start="2025-04-15T14:00:00",
    end="2025-04-15T15:00:00",
    start_tz="Eastern Standard Time",
    end_tz="Eastern Standard Time",
    attendees=["alice@contoso.com", "bob@contoso.com"],
    is_online_meeting=True,
)
print(new["onlineMeeting"]["joinUrl"])
```

### Search email (KQL)

```python
from outlook_integration import get_delegated_token, search_messages

token = get_delegated_token()
results = search_messages(token, "subject:weekly report from:manager@contoso.com")
```

## File structure

```
outlook_integration/
├── __init__.py          # Public API re-exports
├── auth.py              # MSAL auth (device-code + client-credentials)
├── calendar.py          # Calendar CRUD operations
├── config.py            # Environment-based configuration
├── mail.py              # Mail read/search/send operations
├── requirements.txt     # Python dependencies
├── .env.example         # Template for credentials
└── README.md            # This file
```

## Copying to another project

```bash
cp -r outlook_integration /path/to/other/project/
cd /path/to/other/project
pip install -r outlook_integration/requirements.txt
cp outlook_integration/.env.example outlook_integration/.env
# Fill in .env and you're ready
```

The module has zero dependencies on the parent project. It only needs `msal`, `requests`, and optionally `python-dotenv`.
