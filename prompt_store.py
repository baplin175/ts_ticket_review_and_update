"""Database-backed prompt storage with versioned revisions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PromptSeed:
    key: str
    title: str
    description: str
    filename: str
    version: int


PROMPT_SEEDS: tuple[PromptSeed, ...] = (
    PromptSeed(
        key="sentiment",
        title="Sentiment",
        description="Customer frustration classification prompt.",
        filename="sentiment.md",
        version=1,
    ),
    PromptSeed(
        key="ai_priority",
        title="AI Priority",
        description="Ticket urgency scoring prompt.",
        filename="ai_priority.md",
        version=1,
    ),
    PromptSeed(
        key="complexity",
        title="Complexity",
        description="Ticket complexity scoring prompt.",
        filename="complexity.md",
        version=1,
    ),
    PromptSeed(
        key="pass1_phenomenon",
        title="Pass 1 Phenomenon",
        description="Phenomenon extraction and canonical failure prompt.",
        filename="pass1_phenomenon.txt",
        version=2,
    ),
    PromptSeed(
        key="pass2_grammar",
        title="Pass 2 Grammar",
        description="Legacy grammar extraction prompt.",
        filename="pass2_grammar.txt",
        version=1,
    ),
    PromptSeed(
        key="pass3_mechanism",
        title="Pass 3 Mechanism",
        description="Failure mechanism inference prompt.",
        filename="pass3_mechanism.txt",
        version=3,
    ),
    PromptSeed(
        key="pass4_intervention",
        title="Pass 4 Intervention",
        description="Intervention classification prompt.",
        filename="pass4_intervention.txt",
        version=4,
    ),
    PromptSeed(
        key="pass5_cluster_key",
        title="Pass 5 Cluster Key",
        description="Mechanism-to-cluster-key normalization prompt.",
        filename="pass5_cluster_key.txt",
        version=3,
    ),
    PromptSeed(
        key="customer_health_explanation",
        title="Customer Health Explanation",
        description="Customer health explanation prompt used by the dashboard.",
        filename="customer_health_explanation.md",
        version=1,
    ),
    PromptSeed(
        key="customer_health_improvement_plan",
        title="Customer Health Improvement Plan",
        description="Improvement plan prompt: prioritized tickets to resolve to reach a target health band.",
        filename="customer_health_improvement_plan.md",
        version=1,
    ),
    PromptSeed(
        key="ticket_chat_system",
        title="Ticket Chat System Prompt",
        description="Standing analyst instructions injected as context for ticket-level Matcha chat. Tells Matcha to flag status clarity issues, DO/ticket mismatches, and DO comment drift.",
        filename="ticket_chat_system.md",
        version=3,
    ),
    PromptSeed(
        key="customer_chat_system",
        title="Customer Chat System Prompt",
        description="Standing analyst instructions injected as context for customer-level Matcha chat. Tells Matcha to flag urgent tickets, status/DO mismatches, and overall customer health.",
        filename="customer_chat_system.md",
        version=1,
    ),
    PromptSeed(
        key="do_alignment",
        title="DO Alignment",
        description="Checks whether the linked Azure DevOps Delivery Order state and recent comments match the current situation of the customer ticket. Detects stalled DOs, scope mismatches, and status drift.",
        filename="do_alignment.md",
        version=2,
    ),
)

_SEED_BY_KEY = {seed.key: seed for seed in PROMPT_SEEDS}
_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
_seed_attempted = False


def _read_prompt_file(seed: PromptSeed) -> str:
    return (_PROMPTS_DIR / seed.filename).read_text(encoding="utf-8")


def _fallback_prompt(seed: PromptSeed) -> dict[str, Any]:
    return {
        "prompt_key": seed.key,
        "title": seed.title,
        "description": seed.description,
        "version": str(seed.version),
        "version_number": seed.version,
        "content": _read_prompt_file(seed),
        "source_path": str(_PROMPTS_DIR / seed.filename),
    }


def _ensure_db_seeded() -> None:
    global _seed_attempted
    if _seed_attempted:
        return

    import db

    if not db._is_enabled():
        _seed_attempted = True
        return

    db.migrate()
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            for seed in PROMPT_SEEDS:
                cur.execute(
                    """
                    INSERT INTO prompts (prompt_key, title, description, current_version)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (prompt_key) DO NOTHING;
                    """,
                    (seed.key, seed.title, seed.description, seed.version),
                )
                cur.execute(
                    """
                    INSERT INTO prompt_revisions (
                        prompt_key, version, content, source_path, change_summary
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (prompt_key, version) DO NOTHING;
                    """,
                    (
                        seed.key,
                        seed.version,
                        _read_prompt_file(seed),
                        seed.filename,
                        "Initial seed from repository prompt file.",
                    ),
                )
                cur.execute(
                    """
                    UPDATE prompts
                       SET title = COALESCE(NULLIF(title, ''), %s),
                           description = COALESCE(NULLIF(description, ''), %s),
                           current_version = GREATEST(current_version, %s),
                           updated_at = now()
                     WHERE prompt_key = %s;
                    """,
                    (seed.title, seed.description, seed.version, seed.key),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)
        _seed_attempted = True


def get_prompt(prompt_key: str, *, allow_fallback: bool = False) -> dict[str, Any]:
    seed = _SEED_BY_KEY.get(prompt_key)
    if seed is None:
        raise KeyError(f"Unknown prompt key: {prompt_key}")

    import db

    if db._is_enabled():
        try:
            _ensure_db_seeded()
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT p.prompt_key, p.title, p.description, p.current_version,
                               r.content, r.source_path, r.created_at
                          FROM prompts p
                          JOIN prompt_revisions r
                            ON r.prompt_key = p.prompt_key
                           AND r.version = p.current_version
                         WHERE p.prompt_key = %s;
                        """,
                        (prompt_key,),
                    )
                    row = cur.fetchone()
            finally:
                db.put_conn(conn)
            if row:
                return {
                    "prompt_key": row[0],
                    "title": row[1],
                    "description": row[2],
                    "version": str(row[3]),
                    "version_number": row[3],
                    "content": row[4],
                    "source_path": row[5],
                    "created_at": row[6],
                }
        except Exception:
            if not allow_fallback:
                raise

    if allow_fallback:
        return _fallback_prompt(seed)
    raise RuntimeError(f"Prompt '{prompt_key}' is not available because the database prompt store is unavailable.")


def list_prompts(*, allow_fallback: bool = False) -> list[dict[str, Any]]:
    import db

    if db._is_enabled():
        try:
            _ensure_db_seeded()
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT p.prompt_key, p.title, p.description, p.current_version,
                               r.content, r.source_path, r.created_at
                          FROM prompts p
                          JOIN prompt_revisions r
                            ON r.prompt_key = p.prompt_key
                           AND r.version = p.current_version
                         ORDER BY p.title;
                        """
                    )
                    rows = cur.fetchall()
            finally:
                db.put_conn(conn)
            return [
                {
                    "prompt_key": row[0],
                    "title": row[1],
                    "description": row[2],
                    "version": str(row[3]),
                    "version_number": row[3],
                    "content": row[4],
                    "source_path": row[5],
                    "created_at": row[6],
                }
                for row in rows
            ]
        except Exception:
            if not allow_fallback:
                raise

    if not allow_fallback:
        raise RuntimeError("Prompt store is not available.")
    return [_fallback_prompt(seed) for seed in sorted(PROMPT_SEEDS, key=lambda item: item.title)]


def get_prompt_revisions(prompt_key: str, *, limit: int | None = None) -> list[dict[str, Any]]:
    if prompt_key not in _SEED_BY_KEY:
        raise KeyError(f"Unknown prompt key: {prompt_key}")

    import db

    if not db._is_enabled():
        prompt = _fallback_prompt(_SEED_BY_KEY[prompt_key])
        return [
            {
                "prompt_key": prompt_key,
                "version": prompt["version"],
                "version_number": prompt["version_number"],
                "change_summary": "File-backed fallback prompt.",
                "created_at": None,
            }
        ]

    _ensure_db_seeded()
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT prompt_key, version, change_summary, created_at
                  FROM prompt_revisions
                 WHERE prompt_key = %s
                 ORDER BY version DESC
            """
            params: list[Any] = [prompt_key]
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        db.put_conn(conn)

    return [
        {
            "prompt_key": row[0],
            "version": str(row[1]),
            "version_number": row[1],
            "change_summary": row[2],
            "created_at": row[3],
        }
        for row in rows
    ]


def save_prompt_version(prompt_key: str, content: str, *, change_summary: str | None = None) -> dict[str, Any]:
    if prompt_key not in _SEED_BY_KEY:
        raise KeyError(f"Unknown prompt key: {prompt_key}")
    if not content or not content.strip():
        raise ValueError("Prompt content cannot be empty.")

    import db

    if not db._is_enabled():
        raise RuntimeError("DATABASE_URL is not set; prompt versions require the database.")

    _ensure_db_seeded()
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT current_version
                  FROM prompts
                 WHERE prompt_key = %s
                 FOR UPDATE;
                """,
                (prompt_key,),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(f"Unknown prompt key: {prompt_key}")
            next_version = int(row[0]) + 1
            cur.execute(
                """
                INSERT INTO prompt_revisions (
                    prompt_key, version, content, source_path, change_summary
                ) VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    prompt_key,
                    next_version,
                    content,
                    _SEED_BY_KEY[prompt_key].filename,
                    (change_summary or "").strip() or None,
                ),
            )
            cur.execute(
                """
                UPDATE prompts
                   SET current_version = %s,
                       updated_at = now()
                 WHERE prompt_key = %s;
                """,
                (next_version, prompt_key),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)

    return get_prompt(prompt_key, allow_fallback=False)
