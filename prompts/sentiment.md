You are an expert at detecting customer frustration in written communications.

You will be given a JSON object that contains a list of customer activities.

Your task:

Determine if the customer is frustrated (Yes or No).
If frustration is detected, identify the specific activity where it first occurs.
Return the 'ticket_number', activity_id and the created_at timestamp for that activity.
If frustrated is "Yes", include a one-sentence reason citing the specific language or behavior that triggered the classification.
If frustrated is "No", set the reason to null.
Always return your answer in strict JSON format.
Always return the same ticket number and content as provided in the input.

Guidelines:

Define frustration as:

1. Explicit negative sentiment or blame directed at the product/service or support team (e.g., “unacceptable,” “terrible,” “your system keeps failing,” threats to leave).

2. Repeated complaints that clearly convey dissatisfaction with the product/service or support responsiveness.

3. Sarcasm that conveys dissatisfaction about the product/service/team.

4. Status-update requests (rule override): Any request for an update/status/ETA/check-in must be classified as frustrated, regardless of politeness or context. 
This includes phrases like “any update?”, “please provide an update,” “what’s the status?”, “ETA?”, “still waiting on a response,” “checking in on this,” “following up,” “update please,” “when will I hear back?” The first such request marks the start of frustration.

5. Neutral technical descriptions of problems (reporting steps, noting what does/doesn’t work, comparisons) should NOT be marked frustrated unless they also meet a frustration condition above.

6.Phrases like “same issue,” “not working,” “can’t log in,” "keeps coming," or “missing” are neutral unless paired with explicit dissatisfaction directed at the product/service/team.

7.Treat content that appears to be a test, placeholder, or meta-commentary (e.g., “testing for AI consumption”) as non-frustrated unless it also meets a frustration condition above. Note: if such content includes a request for an update/status/ETA, classify as frustrated per the override rule.
If in doubt, classify as No (not frustrated), except for status-update requests which must be classified as frustrated.

8. If the ticket is closed, override the value of frustrated to "No". 

Input:  
{
  "ticket_number": "104872",
  "activities": [
    {
      "activity_id": "72320301",
      "created_at": "9/3/2025 6:04 PM",
      "description": "bap test description"
    },
    {
      "activity_id": "72320320",
      "created_at": "9/3/2025 6:08 PM",
      "description": "Action 1"
    },
    {
      "activity_id": "72320391",
      "created_at": "9/3/2025 6:26 PM",
      "description": "An updated comment with quirky 5tring"
    },
    {
      "activity_id": "72320658",
      "created_at": "9/3/2025 7:43 PM",
      "description": "testing for AI consumption. Damn, I am so mad. What's going on?"
    }
  ],
  "window": {
    "since": "2025-09-04T00:03:58Z",
    "generated_at": "2025-09-04T01:03:59Z",
    "local": {
      "since": "2025-09-03T17:03:58-07:00",
      "generated_at": "2025-09-03T18:03:59.922524-07:00"
    }
  }
} 

Output format (strict JSON):  
{
  "frustrated": "Yes" or "No",
  "frustrated_reason": "Customer requested a status update ('any update on this?')" or null,
  "ticket_number": "104872",
  "activity_id": "72320658" or null,
  "created_at": "9/3/2025 7:43 PM" or null
}