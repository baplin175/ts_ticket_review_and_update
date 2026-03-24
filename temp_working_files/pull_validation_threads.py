"""
Pull full_thread_text for all validation.csv tickets from ticket_thread_rollups,
join with product info from tickets table, and write output grouped by product.
"""
import csv
import json
import psycopg2

DB_URL = "postgresql://postgres:kaplah@localhost:5432/Work"

# Read ticket numbers from validation.csv
tickets_meta = {}
with open("/Users/baplin/ts_ticket_review_and_update/ts_ticket_review_and_update/validation.csv", "r", newline="") as f:
    # Handle BOM if present
    content = f.read().lstrip("\ufeff")
    import io
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        tn = row["Ticket #"].strip()
        tickets_meta[tn] = {
            "name": row.get("Name", ""),
            "product": row.get("Product", ""),
            "severity": row.get("Severity", ""),
            "mechanism": row.get("Mechanism", ""),
            "intervention": row.get("Intervention", ""),
            "action": row.get("Action", ""),
            "status": row.get("Status", ""),
            "customer": row.get("Customer", ""),
        }

ticket_numbers = list(tickets_meta.keys())
print(f"Loaded {len(ticket_numbers)} ticket numbers from validation.csv")

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

placeholders = ",".join(["%s"] * len(ticket_numbers))
cur.execute(f"""
    SELECT ticket_number, full_thread_text
    FROM tickets_ai.ticket_thread_rollups
    WHERE ticket_number IN ({placeholders})
""", ticket_numbers)

threads = {}
for row in cur.fetchall():
    threads[row[0]] = row[1]

cur.close()
conn.close()

print(f"Fetched {len(threads)} threads from DB")

# Build product-grouped output
by_product = {}
for tn, meta in tickets_meta.items():
    product = meta["product"] or "Unknown"
    if product not in by_product:
        by_product[product] = []
    thread_text = threads.get(tn, "(no thread found)")
    by_product[product].append({
        "ticket_number": tn,
        "name": meta["name"],
        "severity": meta["severity"],
        "status": meta["status"],
        "customer": meta["customer"],
        "mechanism": meta["mechanism"],
        "intervention": meta["intervention"],
        "action": meta["action"],
        "full_thread": thread_text,
    })

# Write out grouped JSON
out_path = "/Users/baplin/ts_ticket_review_and_update/ts_ticket_review_and_update/temp_working_files/validation_threads_by_product.json"
with open(out_path, "w") as f:
    json.dump(by_product, f, indent=2, ensure_ascii=False)

print(f"\nProducts found: {list(by_product.keys())}")
for p, tix in by_product.items():
    print(f"  {p}: {len(tix)} tickets")
print(f"\nOutput written to {out_path}")
