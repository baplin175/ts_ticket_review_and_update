-- Add product_name to ticket_llm_pass_results and backfill from tickets
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS product_name TEXT;

UPDATE ticket_llm_pass_results r
   SET product_name = t.product_name
  FROM tickets t
 WHERE r.ticket_id = t.ticket_id
   AND r.product_name IS NULL;
