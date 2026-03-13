You are analyzing a software support or professional-services ticket history to estimate how complex the ticket truly was.

You will be given a single ticket represented as:
- ticket metadata
- a chronological action history
- action types such as Comment, Email, Description
- free-text descriptions, many of which may be blank, repetitive, administrative, or non-technical

Your task is to estimate the ticket’s TRUE COMPLEXITY OF WORK.

The goal is to measure how difficult the underlying work was, not how long the ticket stayed open or how noisy the thread became.

==================================================
CORE RULE
==================================================

Do NOT confuse these with true complexity:
- long elapsed time
- many follow-ups
- customer delays
- repeated scheduling attempts
- waiting on files
- inactivity gaps
- empty emails or comments
- generic check-ins
- “on hold” status
- project age
- thread length alone

A ticket can be open for years and still be low or moderate complexity.
A ticket can be short-lived and still be highly complex.

Your job is to identify the hardest real work implied by the history and score that.

==================================================
WHAT TO EVALUATE
==================================================

Estimate complexity using these dimensions:

1. INTRINSIC_COMPLEXITY
Measure the actual difficulty of the work itself.
Consider:
- non-trivial technical reasoning
- specialist intervention
- custom logic
- scripting, stored procedures, SQL, file transformation, data correction
- ambiguous matching rules
- system behavior analysis
- validation requirements
- test/live promotion complexity
- custom development or technical implementation

2. COORDINATION_LOAD
Measure how much essential coordination was required to complete the work.
Count only coordination that was necessary to delivery, such as:
- multiple parties or teams
- developer/customer/support handoffs
- server access coordination
- test environment setup
- approvals or review checkpoints
- required customer validation
- scheduled execution windows

Do NOT overcount routine follow-up or passive waiting.

3. ELAPSED_DRAG
Measure how much the ticket’s lifecycle was stretched by delay/noise rather than hard work.
Examples:
- customer not ready
- waiting for files
- postponed project timing
- long inactivity gaps
- repeated “checking in”
- waiting for review or signoff
- scheduling churn

4. OVERALL_COMPLEXITY
This is the final score for the ticket.
It should be weighted primarily toward INTRINSIC_COMPLEXITY.
COORDINATION_LOAD can raise it somewhat.
ELAPSED_DRAG should NOT heavily inflate it.

==================================================
HOW TO REASON
==================================================

Read the full history and separate:
- true technical or process complexity
from
- administrative noise and elapsed delay

Focus on the strongest evidence of real work, such as:
- evidence of custom programming
- stored procedures or scripts
- transformation of customer data
- edge cases in file structure
- matching problems across systems
- duplicate or resequencing logic
- ambiguity requiring investigation
- risk to production or billing data
- failed or repeated validation due to technical issues
- non-trivial environment promotion steps

If most of the ticket is administrative but there is one clearly difficult technical core, score based on that core while acknowledging the noise.

Prefer concrete evidence over guesses.
Score conservatively when evidence is weak.
Do not invent unseen work.

==================================================
SCORING SCALE
==================================================

Use a 1–5 integer scale for each score:

1 = Very low
Very little difficulty; mostly simple communication or straightforward execution

2 = Low
Some minor technical or coordination burden, but mostly straightforward

3 = Moderate
Clearly non-trivial; meaningful technical, data, or coordination difficulty

4 = High
Substantial technical/process difficulty, specialist work, notable risk, or complicated validation

5 = Very high
Deep technical ambiguity or major custom work with significant risk, hard constraints, and substantial specialist effort

==================================================
OUTPUT REQUIREMENTS
==================================================

Return ONLY valid JSON.

Use this exact schema:

{
  "ticket_id": "<ticket_id>",
  "ticket_number": "<ticket_number>",
  "ticket_name": "<ticket_name>",
  "intrinsic_complexity": 1,
  "coordination_load": 1,
  "elapsed_drag": 1,
  "overall_complexity": 1,
  "confidence": 0.00,
  "primary_complexity_drivers": [
    "<short driver 1>",
    "<short driver 2>"
  ],
  "complexity_summary": "<brief paragraph explaining the true complexity of the work>",
  "evidence": [
    "<strongest concrete evidence 1>",
    "<strongest concrete evidence 2>",
    "<strongest concrete evidence 3>"
  ],
  "noise_factors": [
    "<factor that made the ticket long/noisy but not intrinsically complex>",
    "<another noise factor>"
  ],
  "duration_vs_complexity_note": "<one sentence distinguishing elapsed time from actual work complexity>"
}

==================================================
FIELD GUIDANCE
==================================================

intrinsic_complexity:
- Score the actual technical/process difficulty of the work

coordination_load:
- Score the degree of necessary coordination required to perform the work

elapsed_drag:
- Score the degree to which waiting, inactivity, postponement, and follow-up stretched the ticket

overall_complexity:
- Final rollup score
- Weight intrinsic_complexity most heavily
- coordination_load can influence it
- elapsed_drag should not dominate it

confidence:
- A number from 0.00 to 1.00 reflecting how clearly the history supports the estimate

primary_complexity_drivers:
- Short phrases only
- Examples: "custom stored procedure", "messy input file rules", "customer validation in test", "risk of duplicate resequencing"

complexity_summary:
- 3 to 6 sentences
- Clearly explain whether the ticket was truly complex or merely long/noisy

evidence:
- Include only the strongest concrete evidence from the text
- Prefer paraphrases grounded in the ticket over vague claims

noise_factors:
- Include things that inflated duration or action count without reflecting real difficulty

duration_vs_complexity_note:
- Explicitly state the distinction between long-running and complex

==================================================
IMPORTANT DECISION RULES
==================================================

1. A long-running project with simple repeated follow-up is not automatically complex.
2. A ticket involving custom data transformation, database logic, or risky resequencing can be complex even if only a few actions mention it.
3. Waiting on customer readiness, files, scheduling, or signoff should primarily raise elapsed_drag, not overall_complexity.
4. Empty actions, trivial replies, and repeated status requests should be treated as noise unless they reveal real delivery constraints.
5. If the ticket appears to be a professional-services or implementation task rather than break/fix support, still score the complexity of the actual work performed.
6. Base your result on the hardest real work evidenced in the history, not on the average blandness of all comments.
7. Do not assume hidden effort unless the thread strongly implies it.

Now analyze this ticket history:

{{TICKET_HISTORY}}