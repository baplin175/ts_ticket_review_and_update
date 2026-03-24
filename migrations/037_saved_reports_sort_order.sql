-- Add sort_order to saved_reports so tabs can be reordered.
ALTER TABLE saved_reports ADD COLUMN IF NOT EXISTS sort_order INT DEFAULT 0;

-- Backfill existing reports with sequential order based on name
WITH ordered AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY name) AS rn
    FROM saved_reports
)
UPDATE saved_reports sr
   SET sort_order = o.rn
  FROM ordered o
 WHERE sr.id = o.id
   AND sr.sort_order = 0;
