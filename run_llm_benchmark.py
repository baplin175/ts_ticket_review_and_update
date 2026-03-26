"""
LLM Benchmark — Pass 1 prompt across all Matcha models.

Sends the pass1_phenomenon prompt with a test ticket to every available
Matcha model, times each call, then scores each response using GPT-5.2 (id=87).
Runs multiple iterations and reports per-model averages.

Usage:
    python run_llm_benchmark.py [--iterations N]
"""

import argparse
import csv
import json
import statistics
import time
from pathlib import Path

from matcha_client import call_matcha

# ── Test fixture — Ticket #109530 "1099" (long thread, clear technical issue) ─

TICKET_NAME = "1099"
TICKET_TEXT = (
    "[2026-02-06 07:34 | Amy Wegener] I have 2 1099's that I need to print and can't get them to print "
    "as it is looking to me like its because the history didn't come over in the conversion. Anything I "
    "can do or am I going to have to get my old software company to print them for me? I don't have the "
    "permissions any longer to process anything from them?\n\n"
    "[2026-02-09 06:43 | Susan Thomas] Hi Amy, I wanted to follow up on this ticket. Could you please "
    "let me know the status? Do you still need assistance with this issue?\n\n"
    "[2026-02-09 08:24 | Amy Wegener] Yes I still need help. Gworks is saying I can't do anything "
    "processing wise on that program, its reports only, so need to know figure out how to get the "
    "information to PM to get them printed.\n\n"
    "[2026-02-09 09:35 | Susan Thomas] I reached out to Emelia to get her feedback on this.\n\n"
    "[2026-02-09 09:45 | Susan Thomas] Hi Amy, Since AP history does not transfer from gWorks to PM, "
    "any 1099 forms would have needed to be generated from gWorks while you still had access to that "
    "system. Because those 1099s were not produced before the transition, you will now need to manually "
    "enter the necessary vendor payment information into PM in order to generate your 1099s from PM.\n\n"
    "[2026-02-11 05:02 | Amy Wegener] I guess it can be closed. Don't know how to get this to work, "
    "but guess I will figure something out.\n\n"
    "[2026-02-12 10:50 | Amy Wegener] I cannot get the 1099 for 1 vendor to print. Can someone help?\n\n"
    "[2026-02-13 01:24 | Susan Thomas] Hi Amy, please follow the instructions and let us know how it "
    "goes. Steps to complete: Access the Vendors maintenance option in Accounts Payable (AP/Maintenance/"
    "Vendors). For each vendor requiring a 1099: Edit the vendor record. On the Payment Info tab, ensure "
    "the correct 1099 type and box number are selected. Go to the Invoice Info tab where you can manually "
    "enter the payment information. After entering all vendor payment information, access the 1099 Reports "
    "And Form (AP/Reports/1099 Reports And Form). Select the appropriate report design, enter the reported "
    "year, select the type of 1099, and set minimum amounts if needed. Use the Select Vendors button to "
    "choose which vendors to include. Click Preview or Print to generate the 1099 forms.\n\n"
    "[2026-02-24 08:13 | Amy Wegener] On the invoice tab, how do i manually enter the payment information "
    "needed from GWorks? I don't see where this is supposed to be process like the directions say to input "
    "the information.\n\n"
    "[2026-02-24 09:25 | Susan Thomas] Steps to complete: Access the Vendors maintenance option in "
    "Accounts Payable. Search for and select the vendor requiring a 1099. Click the Edit button. On the "
    "Payment Info tab, verify the correct 1099 type is selected. Click on the Invoice Info tab. In the "
    "Invoice List section, you can add payment information by right-clicking in the grid area and "
    "selecting 'Add' to create a new entry. Enter the payment information from gWorks, including date, "
    "amount, and invoice number. Make sure to select the correct 1099 type and box number for each "
    "payment entry. Save the vendor record.\n\n"
    "[2026-02-27 06:31 | Amy Wegener] I get nothing when I right click in the grid area.\n\n"
    "[2026-02-27 06:59 | Susan Thomas] Issues: Customer needs to print 1099 forms for vendors but is "
    "having difficulty because payment history didn't transfer from their previous system (gWorks) to "
    "PowerManager. Customer cannot access the invoice entry screen — specifically reporting 'I get nothing "
    "when I right click in the grid area' when trying to manually add payment information. Hi Emelia, Amy "
    "attempted to manually enter the invoice information since it did not transfer over from GWorks, but "
    "the fields are greyed out and she's unable to input any data. Can you please review?\n\n"
    "[2026-03-02 02:46 | Emelia De Smet-Bacon] Reviewed this ticket. Found some issues with the "
    "instructions provided on 2/24. Amy should be able to select the 1099 information using the drop down "
    "in the Invoice Info tab under the Vendor's maintenance page. Also advised to have her double check "
    "her dates. Madison went live with PM in 2025, so I don't think Amy would need to enter an old invoice "
    "from gWorks that's dated in 2026.\n\n"
    "[2026-03-02 07:43 | Susan Thomas] Hi Amy, Amy should not right-click to add anything. She just needs "
    "to edit the existing invoice, go to the Invoice Info tab, and select the correct 1099 information "
    "from the drop-down. They will need to click Edit and search for the vendor first.\n\n"
    "[2026-03-02 07:58 | Amy Wegener] I needed the 1099 from an invoice in 2025. Since I was told I could "
    "edit this information since I can't get it from Gworks, my window to go back into Gworks and process "
    "has now passed. How am I supposed to now process the information to get a 1099 printed and submitted?\n\n"
    "[2026-03-03 10:51 | Emelia De Smet-Bacon] It seems Amy was able to enter the invoice but it wouldn't "
    "print. It seems it wouldn't print because the 1099 selections weren't made when creating the invoice. "
    "If Amy can't click the drop down to edit the 1099 information, I'd suggest voiding the unpaid invoice "
    "and starting fresh. Amy will need to create the invoice in Accounts Payable → Processing → Enter "
    "Invoices. Make sure to select the correct 1099 information (Type and 1099 Box #). Then Amy can process "
    "the payment. To print the 1099, go to 1099 reports. Enter the correct Reported Year (2025).\n\n"
    "[2026-03-04 01:50 | Susan Thomas] Hi Amy, it appears the invoice was entered, but it wouldn't print "
    "because the required 1099 selections were not made during creation. If you're unable to edit the 1099 "
    "fields using the dropdown menu, Emelia recommends voiding the unpaid invoice and starting fresh."
)

# ── Models to benchmark ───────────────────────────────────────────────────────
# id → display name  (GPT-5.2 id=87 is reserved for scoring)

MODELS = [
    (43,  "Ultimate 8-in-1 LLM Router"),
    (42,  "GPT 4.1"),
    (29,  "Harris Rapid LLM (Internal)"),
    (1,   "Harris Precise LLM (Internal)"),
    (26,  "Gemini 2.5 Flash"),
    (35,  "Gemini 2.5 Flash-Lite"),
    (47,  "Cohere Command A (Azure)"),
    (39,  "Llama3.2 Vision 90B (Azure)"),
    (50,  "OpenAI o3-mini"),
    (38,  "OpenAI o3"),
    (44,  "DeepSeek R1 528"),
    (74,  "GPT-5 Pro"),
    (21,  "[Legacy] GPT-4o Mini"),
    (67,  "GPT-5"),
    (36,  "Gemini 2.5 Pro"),
    (68,  "GPT-5 Mini"),
    (73,  "Gemini 2.5 Flash Image (Nano Banana)"),
    (77,  "GPT-5.1"),
    (79,  "GPT-5.1-Codex"),
    (80,  "GPT-5.1 Codex Mini"),
    (71,  "Claude Sonnet 4.5"),
    (86,  "GPT-5.1-Codex-Max"),
    (89,  "Grok 4 (Azure)"),
    (90,  "Kimi K2 Thinking"),
    (76,  "Gemini 3.1 Pro"),
    (91,  "Claude Opus 4.6"),
    (92,  "GPT-5.2-Codex"),
    (93,  "Gemini 3 Flash"),
    (94,  "Claude Sonnet 4.6"),
    (83,  "Claude Opus 4.5"),
    (84,  "Claude Haiku 4.5"),
    (98,  "Mistral-Large-3"),
    (87,  "GPT-5.2"),
]

SCORER_ID = 87   # GPT-5.2

SCORER_PROMPT_TEMPLATE = """You are evaluating a model's response to a pass 1 phenomenon extraction task.

The model was asked to analyze a support ticket and return JSON with:
  phenomenon, confidence, component, operation, unexpected_state

--------------------------------
REFERENCE ANSWER (SEMANTIC TARGET)
--------------------------------

The correct answer represents this behavior:

Primary observable issue:
- 1099 forms fail to print for vendor invoices

Key context (may or may not appear explicitly):
- invoice 1099 fields not set or not editable
- inability to input required 1099 data

--------------------------------
SCORING CRITERIA
--------------------------------

Evaluate the response across these dimensions:

1. PHENOMENON ACCURACY (0-4)
- 4 = correctly captures core issue (1099 cannot print)
- 3 = mostly correct but slightly vague
- 2 = partially correct (mentions 1099 but wrong behavior)
- 1 = incorrect issue
- 0 = missing/null

IMPORTANT:
- Accept semantic equivalents
- Do NOT require exact wording

---

2. ROOT CAUSE LEAKAGE (0-2)
- 2 = no cause or explanation included
- 1 = minor leakage
- 0 = explicitly explains why (violates task)

---

3. OPERATION CORRECTNESS (0-2)
- 2 = correct ("print")
- 1 = plausible but not ideal
- 0 = incorrect or invalid

---

4. COMPONENT QUALITY (0-1)
- 1 = correct system area (Accounts Payable / Vendor / 1099)
- 0 = incorrect or missing

---

5. STRUCTURE & FORMAT (0-1)
- 1 = valid JSON, correct fields, no extra text
- 0 = invalid or malformed

---

--------------------------------
FINAL SCORE
--------------------------------

Total score = sum (0-10)

--------------------------------
OUTPUT FORMAT
--------------------------------

Return ONLY JSON:

{{"score": <0-10>, "breakdown": {{"phenomenon": <0-4>, "root_cause": <0-2>, "operation": <0-2>, "component": <0-1>, "format": <0-1>}}, "notes": "<brief explanation of key issues or strengths>"}}

Response to evaluate:
{response}
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def build_pass1_prompt() -> str:
    template = Path("prompts/pass1_phenomenon.txt").read_text()
    return (
        template
        .replace("{{ticket_name}}", TICKET_NAME)
        .replace("{{input_text}}", TICKET_TEXT)
    )


def score_response(response: str) -> dict:
    scorer_prompt = SCORER_PROMPT_TEMPLATE.format(response=response)
    try:
        raw = call_matcha(scorer_prompt, inference_server=SCORER_ID, timeout=60)
        cleaned = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return json.loads(cleaned)
    except Exception as exc:
        return {"score": None, "breakdown": None, "notes": f"Scoring error: {exc}"}


def run_one(model_id: int, model_name: str, prompt: str, iteration: int) -> dict:
    """Run a single model call and return the raw result for one iteration."""
    print(f"    iter {iteration} [{model_id:>3}] {model_name} ...", end=" ", flush=True)
    start = time.time()
    try:
        response = call_matcha(prompt, inference_server=model_id, timeout=60)
        elapsed = time.time() - start
        error = None
    except Exception as exc:
        elapsed = time.time() - start
        response = ""
        error = str(exc)

    if error:
        print(f"ERROR ({elapsed:.1f}s): {error}")
        return {
            "iteration": iteration,
            "elapsed_s": round(elapsed, 2),
            "response": "",
            "score": None,
            "breakdown": None,
            "notes": f"Call failed: {error}",
        }

    print(f"done ({elapsed:.1f}s), scoring ...", end=" ", flush=True)
    scored = score_response(response)
    print(f"score={scored.get('score')}")

    return {
        "iteration": iteration,
        "elapsed_s": round(elapsed, 2),
        "response": response,
        "score": scored.get("score"),
        "breakdown": scored.get("breakdown"),
        "notes": scored.get("notes"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=2,
                        help="Number of times to run each model (default: 2)")
    args = parser.parse_args()
    iterations = args.iterations

    prompt = build_pass1_prompt()

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    json_path = out_dir / "llm_benchmark_results.json"
    csv_path  = out_dir / "llm_benchmark_results.csv"

    # ── Resume: load any previously completed results ─────────────────────────
    all_results: list[dict] = []
    completed_ids: set[int] = set()
    if json_path.exists():
        try:
            saved = json.loads(json_path.read_text())
            all_results = saved.get("results", saved) if isinstance(saved, dict) else saved
            completed_ids = {r["id"] for r in all_results}
            if completed_ids:
                print(f"\nResuming — {len(completed_ids)} model(s) already completed, skipping them.")
        except Exception:
            all_results = []

    remaining = [(mid, mname) for mid, mname in MODELS if mid not in completed_ids]

    print(f"\nBenchmarking {len(remaining)} remaining model(s) x {iterations} iterations against pass1 prompt...\n")
    print(f"Ticket: #{TICKET_NAME}  |  Thread length: {len(TICKET_TEXT)} chars\n")

    for model_id, model_name in remaining:
        print(f"  [{model_id:>3}] {model_name}")
        runs = []
        for i in range(1, iterations + 1):
            run = run_one(model_id, model_name, prompt, i)
            runs.append(run)

        # Compute averages (skip None scores)
        scored_runs = [r for r in runs if r["score"] is not None]
        avg_score = round(statistics.mean(r["score"] for r in scored_runs), 2) if scored_runs else None
        avg_elapsed = round(statistics.mean(r["elapsed_s"] for r in runs), 2)

        model_entry = {
            "id": model_id,
            "name": model_name,
            "avg_score": avg_score,
            "avg_elapsed_s": avg_elapsed,
            "runs": runs,
        }
        all_results.append(model_entry)

        # Save incrementally after each model completes all its iterations
        _save_json(json_path, all_results, prompt)
        _save_csv(csv_path, all_results, iterations)
        print()

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n" + "=" * 95)
    print(f"{'Rank':<5} {'Avg Score':<11} {'Avg Time(s)':<13} {'ID':<5} Model")
    print("=" * 95)

    scored = [r for r in all_results if r["avg_score"] is not None]
    failed = [r for r in all_results if r["avg_score"] is None]

    for rank, r in enumerate(sorted(scored, key=lambda x: (-x["avg_score"], x["avg_elapsed_s"])), 1):
        print(f"{rank:<5} {r['avg_score']:<11} {r['avg_elapsed_s']:<13.1f} {r['id']:<5} {r['name']}")

    if failed:
        print("\n--- Failed / Unscored ---")
        for r in failed:
            print(f"  [{r['id']:>3}] {r['name']:<40} avg_time={r['avg_elapsed_s']:.1f}s")

    # ── CSV output ────────────────────────────────────────────────────────────
    _save_csv(csv_path, all_results, iterations)

    print(f"\nJSON results : {json_path}")
    print(f"CSV results  : {csv_path}")


def _save_json(path: Path, all_results: list[dict], prompt: str) -> None:
    output = {
        "test_input": {
            "ticket_name": TICKET_NAME,
            "ticket_text": TICKET_TEXT,
            "full_prompt_sent_to_llm": prompt,
        },
        "results": all_results,
    }
    with open(path, "w") as f:
        json.dump(output, f, indent=2)


def _save_csv(path: Path, all_results: list[dict], iterations: int) -> None:
    # One row per model with avg + per-iteration columns
    iter_cols = []
    for i in range(1, iterations + 1):
        iter_cols += [f"iter{i}_score", f"iter{i}_elapsed_s"]

    fieldnames = ["id", "name", "avg_score", "avg_elapsed_s"] + iter_cols

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in all_results:
            row: dict = {
                "id": r["id"],
                "name": r["name"],
                "avg_score": r["avg_score"] if r["avg_score"] is not None else "",
                "avg_elapsed_s": r["avg_elapsed_s"],
            }
            for i, run in enumerate(r["runs"], 1):
                row[f"iter{i}_score"] = run["score"] if run["score"] is not None else ""
                row[f"iter{i}_elapsed_s"] = run["elapsed_s"]
            # Pad missing iterations if run was interrupted mid-way
            for i in range(len(r["runs"]) + 1, iterations + 1):
                row[f"iter{i}_score"] = ""
                row[f"iter{i}_elapsed_s"] = ""
            writer.writerow(row)


if __name__ == "__main__":
    main()
