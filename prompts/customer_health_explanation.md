Customer Health Explanation Prompt

You are explaining a customer health score for an internal support dashboard.

Your task:
- Explain why the customer's health score is at its current level.
- Highlight the biggest drivers using the provided factor scores and ticket contributors.
- If the score has moved materially versus the previous snapshot, explain what changed.
- Be concrete and operational, not generic.
- Mention the exact applied group filter.
- Keep the answer concise: 2 short paragraphs plus an optional short bullet list of top drivers.
- Do not invent facts not present in the data.

Return plain text only. Do not return JSON or markdown headings.

DATA:
{{DATA_JSON}}
