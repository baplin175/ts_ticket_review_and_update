"""
outlook_integration — Self-contained Microsoft Graph API client for Outlook.

Supports:
  - Device-code flow (delegated / user permissions)
  - Client-credentials flow (app-only permissions)
  - Mail: list, get, search, send, reply, folders
  - Calendar: list, get, calendarView, create, update, delete events

Quick start (delegated):

    from outlook_integration import get_delegated_token, list_messages, send_mail

    token = get_delegated_token()          # one-time browser prompt
    msgs  = list_messages(token, top=10)   # latest 10 inbox messages
    send_mail(token, subject="Hi", body="Hello!", to=["someone@example.com"])
"""

# Auth
from .auth import get_delegated_token, get_app_token  # noqa: F401

# Mail
from .mail import (  # noqa: F401
    list_messages,
    get_message,
    search_messages,
    send_mail,
    reply_to_message,
    list_mail_folders,
)

# Calendar
from .calendar import (  # noqa: F401
    list_events,
    get_event,
    calendar_view,
    create_event,
    update_event,
    delete_event,
    list_calendars,
)
