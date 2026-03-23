"""Sync TeamSupport customer metadata used by the dashboard."""

from datetime import datetime, timezone

import db
from ingest.extractors import extract_customer_row
from ts_client import fetch_all_customers


def main():
    db.migrate()
    now = datetime.now(timezone.utc)
    raw_customers = fetch_all_customers()
    upserted = 0
    for raw_customer in raw_customers:
        row = extract_customer_row(raw_customer, now)
        if not row.get("customer_id") or not row.get("customer_name"):
            continue
        db.upsert_customer_attribute(row, now=now)
        upserted += 1
    print(f"[customers] Upserted {upserted} customer metadata row(s).")


if __name__ == "__main__":
    main()
