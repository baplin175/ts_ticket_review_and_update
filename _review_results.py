"""Temporary script to review enrichment results."""
import db

# Count results
rows = db.fetch_all("SELECT 'priority' as t, count(*) FROM ticket_priority_scores UNION ALL SELECT 'complexity', count(*) FROM ticket_complexity_scores;")
for r in rows:
    print(f"{r[0]}: {r[1]}")
print()

# Priority distribution
rows2 = db.fetch_all("SELECT priority, count(*) FROM ticket_priority_scores GROUP BY priority ORDER BY priority;")
print("Priority distribution:")
for r in rows2:
    print(f"  P{r[0]}: {r[1]}")
print()

# Complexity distribution
rows3 = db.fetch_all("SELECT overall_complexity, count(*) FROM ticket_complexity_scores GROUP BY overall_complexity ORDER BY overall_complexity;")
print("Complexity distribution:")
for r in rows3:
    print(f"  C{r[0]}: {r[1]}")
print()

# Sample: top priority (most urgent)
rows4 = db.fetch_all("""
SELECT t.ticket_number, t.ticket_name, t.customer, p.priority, 
       substring(p.priority_explanation from 1 for 100)
FROM ticket_priority_scores p 
JOIN tickets t ON t.ticket_id = p.ticket_id 
ORDER BY p.priority ASC LIMIT 10;
""")
print("Top 10 priority (most urgent):")
for r in rows4:
    print(f"  #{r[0]} [{r[2]}] P{r[3]}: {r[1]}")
    print(f"    {r[4]}...")
print()

# Sample: most complex
rows5 = db.fetch_all("""
SELECT t.ticket_number, t.ticket_name, t.customer, 
       c.overall_complexity, c.intrinsic_complexity, c.coordination_load, c.elapsed_drag
FROM ticket_complexity_scores c 
JOIN tickets t ON t.ticket_id = c.ticket_id 
ORDER BY c.overall_complexity DESC LIMIT 10;
""")
print("Top 10 complexity (most complex):")
for r in rows5:
    print(f"  #{r[0]} [{r[2]}] Overall={r[3]} Intrinsic={r[4]} Coord={r[5]} Drag={r[6]}: {r[1]}")
