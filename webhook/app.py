"""
Webhook receiver for TeamSupport ticket events.

Accepts POST payloads from TeamSupport (including Slack-format notifications),
validates the request, extracts the ticket ID, and triggers an incremental
sync for that ticket.

Usage:
    python -m webhook.app                              # port 5002, no auth
    WEBHOOK_SECRET=mysecret python -m webhook.app      # port 5002, bearer-token auth

Testing:
    curl -X POST http://localhost:5002/webhook/teamsupport \\
         -H "Content-Type: application/json" \\
         -H "Authorization: Bearer mysecret" \\
         -d '{"TicketID": "29696"}'
"""

import hashlib
import hmac
import json
import logging
import re
import threading

from flask import Flask, request, jsonify

import config
from run_ingest import _sync

app = Flask(__name__)
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = app.logger


# ── Auth helpers ─────────────────────────────────────────────────────

def _verify_request():
    """Validate the inbound request against WEBHOOK_SECRET.

    Supports two modes:
      1. Bearer token  — Authorization: Bearer <secret>
      2. HMAC-SHA256   — X-TS-Signature: <hex-digest of HMAC(secret, body)>

    Returns None if valid, or a (response, status_code) tuple if invalid.
    """
    secret = config.WEBHOOK_SECRET
    if not secret:
        return None                       # no secret configured → skip (dev mode)

    # Mode 1: Bearer token
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        if hmac.compare_digest(token, secret):
            return None
        return jsonify({"error": "Invalid token"}), 401

    # Mode 2: HMAC-SHA256 signature
    signature = request.headers.get("X-TS-Signature", "")
    if signature:
        expected = hmac.new(
            secret.encode(), request.get_data(), hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(signature, expected):
            return None
        return jsonify({"error": "Invalid signature"}), 401

    return jsonify({"error": "Missing authentication"}), 401


# ── Payload parsing ──────────────────────────────────────────────────

def _extract_ticket_id(payload: dict) -> str | None:
    """Pull the ticket ID from a TS webhook payload.

    Supported payload shapes:
        Generic:    {"TicketID": "12345", ...}
        Nested:     {"data": {"TicketID": "12345"}, ...}
        Typed:      {"ID": "12345", "Type": "Ticket", ...}
        Slack-fmt:  {"text": "Ticket #29696 updated...", ...}

    For Slack-format payloads, the ticket number is extracted from the
    message text via regex (looks for '#NNN' or 'Ticket NNN' patterns).
    """
    # Direct field lookup
    for key in ("TicketID", "ticketID", "ticketId", "ticket_id"):
        val = payload.get(key)
        if val:
            return str(val)

    # Nested under "data"
    data = payload.get("data") or payload.get("Data") or {}
    for key in ("TicketID", "ticketID", "ticketId", "ticket_id"):
        val = data.get(key)
        if val:
            return str(val)

    # Generic typed payload
    if payload.get("Type", "").lower() == "ticket" and payload.get("ID"):
        return str(payload["ID"])

    # Slack-format: ticket number embedded in text body
    text = payload.get("text") or payload.get("fallback") or ""
    if text:
        # Match patterns like "#29696", "Ticket #29696", "Ticket 29696",
        # or URLs containing /Ticket/29696
        m = re.search(r'(?:Ticket\s*#?\s*|#|/Ticket/)(\d{4,})', text, re.IGNORECASE)
        if m:
            return m.group(1)

    # Check Slack attachments array
    for att in payload.get("attachments", []):
        att_text = att.get("text") or att.get("fallback") or att.get("pretext") or ""
        if att_text:
            m = re.search(r'(?:Ticket\s*#?\s*|#|/Ticket/)(\d{4,})', att_text, re.IGNORECASE)
            if m:
                return m.group(1)

    return None


# ── Routes ───────────────────────────────────────────────────────────

@app.route("/webhook/teamsupport", methods=["POST"])
def webhook_teamsupport():
    # 1. Verify
    err = _verify_request()
    if err:
        return err

    # 2. Parse
    payload = request.get_json(silent=True) or {}
    if not payload:
        return jsonify({"error": "Empty or invalid JSON body"}), 400

    # 3. Extract ticket ID
    ticket_id = _extract_ticket_id(payload)
    if not ticket_id:
        log.warning("Could not extract ticket ID from payload: %s",
                     json.dumps(payload, default=str)[:500])
        return jsonify({"error": "No ticket ID found in payload"}), 422

    # 4. Trigger sync in a background thread (don't block the webhook response)
    log.info("Webhook received — syncing ticket_id=%s", ticket_id)
    thread = threading.Thread(
        target=_sync,
        kwargs={"ticket_ids": [ticket_id]},
        daemon=True,
    )
    thread.start()

    return jsonify({"status": "accepted", "ticket_id": ticket_id}), 202


@app.route("/webhook/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.before_request
def _log_all_requests():
    """Log every inbound request for debugging TS payload discovery."""
    if request.path == "/webhook/health":
        return
    body = request.get_data(as_text=True)[:2000]
    log.info(">>> %s %s  Content-Type=%s  Body=%s",
             request.method, request.path,
             request.content_type, body)


# Catch-all: accept POSTs to any path (TS Slack integration may post to /)
@app.route("/", methods=["POST"])
@app.route("/<path:path>", methods=["POST"])
def catch_all(path=""):
    if path == "webhook/teamsupport":
        # Already handled by the explicit route above
        return webhook_teamsupport()

    # Log and try to process anyway
    payload = request.get_json(silent=True) or {}
    if not payload:
        # Slack sometimes sends form-encoded "payload" field
        form_payload = request.form.get("payload")
        if form_payload:
            try:
                payload = json.loads(form_payload)
            except (json.JSONDecodeError, TypeError):
                pass

    ticket_id = _extract_ticket_id(payload) if payload else None
    if ticket_id:
        log.info("Catch-all matched ticket_id=%s from %s %s", ticket_id, request.method, request.path)
        thread = threading.Thread(
            target=_sync,
            kwargs={"ticket_ids": [ticket_id]},
            daemon=True,
        )
        thread.start()
        return jsonify({"status": "accepted", "ticket_id": ticket_id}), 202

    # Just acknowledge so TS doesn't retry — log the payload for inspection
    log.warning("Catch-all: no ticket ID extracted from %s %s — payload: %s",
                request.method, request.path,
                json.dumps(payload, default=str)[:1000])
    return jsonify({"status": "received", "note": "no ticket ID extracted"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=False)
