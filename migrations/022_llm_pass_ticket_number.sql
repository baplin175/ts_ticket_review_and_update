-- Migration 022 — add ticket_number to ticket_llm_pass_results.
-- Keeps pass-result rows denormalized with the human-readable ticket number.

ALTER TABLE ticket_llm_pass_results
    ADD COLUMN IF NOT EXISTS ticket_number TEXT;

UPDATE ticket_llm_pass_results lp
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = lp.ticket_id
   AND (lp.ticket_number IS NULL OR lp.ticket_number = '');
