from datetime import datetime, timezone
from unittest.mock import MagicMock

import db
import pytest
from ingest.extractors import extract_customer_row


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


def test_extract_customer_row_maps_key_acct_boolean():
    now = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
    row = extract_customer_row(
        {
            "ID": "1204066",
            "Name": "Acme Water",
            "IsActive": "True",
            "KeyAcct": "True",
            "DefaultSupportGroup": "Customer Support (CS)",
            "DateCreated": "4/26/2017 8:01 AM",
            "DateModified": "4/8/2025 7:28 PM",
        },
        now,
    )
    assert row["customer_id"] == 1204066
    assert row["customer_name"] == "Acme Water"
    assert row["is_active"] is True
    assert row["key_acct"] is True
    assert row["key_acct_raw"] == "True"


def test_upsert_customer_attribute_uses_on_conflict(mock_conn, monkeypatch):
    conn, cur = mock_conn
    monkeypatch.setattr(db, "get_conn", lambda: conn)
    monkeypatch.setattr(db, "put_conn", lambda _conn: None)

    now = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
    db.upsert_customer_attribute(
        {
            "customer_id": 1204066,
            "customer_name": "Acme Water",
            "is_active": True,
            "key_acct": True,
            "key_acct_raw": "True",
            "default_support_group": "Customer Support (CS)",
            "date_created": now,
            "date_modified": now,
            "source_payload": {"ID": "1204066"},
        },
        now=now,
    )

    sql_executed = cur.execute.call_args[0][0]
    assert "INSERT INTO customer_attributes" in sql_executed
    assert "ON CONFLICT (customer_id) DO UPDATE SET" in sql_executed
