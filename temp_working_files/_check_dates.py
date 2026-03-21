import db

rows = db.fetch_all(
    "SELECT snapshot_date FROM daily_open_counts "
    "WHERE snapshot_date >= '2025-11-01' AND snapshot_date <= '2025-12-15' "
    "GROUP BY snapshot_date ORDER BY snapshot_date;"
)
print(f"Dates between Nov 1 - Dec 15, 2025: {len(rows)}")
for r in rows:
    print(r[0])

# Also check total date count by month
rows2 = db.fetch_all(
    "SELECT date_trunc('month', snapshot_date)::date AS month, COUNT(DISTINCT snapshot_date) AS days "
    "FROM daily_open_counts "
    "WHERE snapshot_date >= '2025-01-01' "
    "GROUP BY 1 ORDER BY 1;"
)
print("\nDays per month (2025+):")
for r in rows2:
    print(f"  {r[0]}: {r[1]} days")
