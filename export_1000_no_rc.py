"""Export up to 1000 PM/Impresa tickets without RCA for the CSV pipeline."""

import csv
import sys
import db

SQL = """
SELECT
    t.ticket_id,
    t.ticket_name,
    r.full_thread_text
FROM tickets t
JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
WHERE (
        LOWER(t.product_name) LIKE 'pm%%'
     OR LOWER(t.product_name) LIKE '%%power%%'
     OR LOWER(t.product_name) LIKE '%%impresa%%'
      )
  AND t.date_created >= now() - INTERVAL '18 months'
  AND r.full_thread_text IS NOT NULL
  AND r.full_thread_text <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM ticket_llm_pass_results lp
      WHERE lp.ticket_id = t.ticket_id
        AND lp.pass_name = 'pass1_phenomenon'
        AND lp.status = 'success'
  )
ORDER BY t.date_created DESC
LIMIT 1000;
"""

def main():
    rows = db.fetch_all(SQL)
    if not rows:
        print("No matching tickets found.", file=sys.stderr)
        sys.exit(1)

    out_path = "pipeline_input_pm_impresa_1k.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["ticket_id", "ticket_name", "full_thread_text"])
        for row in rows:
            writer.writerow(row)

    print(f"Wrote {len(rows)} rows to {out_path}")

if __name__ == "__main__":
    main()