"""
Database access layer — connection pool, migration runner, and reusable
idempotent upsert helpers for the TeamSupport ingestion pipeline.

All functions are no-ops when DATABASE_URL is empty (JSON-only mode).
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config import DATABASE_URL, DATABASE_SCHEMA

# Register the UUID adapter so psycopg2 handles uuid.UUID natively
psycopg2.extras.register_uuid()

# ── Module-level connection pool (lazy) ──────────────────────────────

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")


def _is_enabled() -> bool:
    """Return True when a DATABASE_URL is configured."""
    return bool(DATABASE_URL)


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return (and lazily create) the module-level connection pool."""
    global _pool
    if _pool is None:
        if not _is_enabled():
            raise RuntimeError("DATABASE_URL is not set; cannot create connection pool.")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DATABASE_URL,
        )
    return _pool


def get_conn():
    """Borrow a connection from the pool and set search_path to the configured schema."""
    conn = get_pool().getconn()
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s;", (DATABASE_SCHEMA,))
    return conn


def put_conn(conn) -> None:
    """Return a connection to the pool."""
    get_pool().putconn(conn)


def close_pool() -> None:
    """Shut down the connection pool (for clean exit)."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


# ── Migration runner ─────────────────────────────────────────────────

def _ensure_migration_table(conn) -> None:
    """Create the target schema and migration tracking table if they do not exist."""
    from psycopg2 import sql as psql
    with conn.cursor() as cur:
        cur.execute(psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            psql.Identifier(DATABASE_SCHEMA)))
        cur.execute("SET search_path TO %s;", (DATABASE_SCHEMA,))
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                filename    TEXT PRIMARY KEY,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
    conn.commit()


def _applied_migrations(conn) -> set:
    """Return the set of migration filenames already applied."""
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM _migrations ORDER BY filename;")
        return {row[0] for row in cur.fetchall()}


def migrate() -> List[str]:
    """Apply any unapplied SQL migration files from migrations/ in order.

    Returns the list of newly-applied filenames.  Safe to call repeatedly.
    """
    if not _is_enabled():
        print("[db] DATABASE_URL not set — skipping migrations.", flush=True)
        return []

    migration_files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith(".sql")
    )
    if not migration_files:
        print("[db] No migration files found.", flush=True)
        return []

    conn = get_conn()
    try:
        _ensure_migration_table(conn)
        already = _applied_migrations(conn)
        applied: List[str] = []

        for fname in migration_files:
            if fname in already:
                continue
            path = os.path.join(MIGRATIONS_DIR, fname)
            sql = Path(path).read_text(encoding="utf-8")
            print(f"[db] Applying migration: {fname} …", flush=True)
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO _migrations (filename) VALUES (%s);",
                    (fname,),
                )
            conn.commit()
            applied.append(fname)
            print(f"[db] Applied: {fname}", flush=True)

        if not applied:
            print("[db] All migrations already applied.", flush=True)
        return applied
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


# ── Generic helpers ──────────────────────────────────────────────────

def execute(sql: str, params: Sequence = ()) -> None:
    """Execute a single statement (no result set)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def fetch_all(sql: str, params: Sequence = ()) -> List[Tuple]:
    """Execute a query and return all rows as tuples."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        put_conn(conn)


def fetch_one(sql: str, params: Sequence = ()) -> Optional[Tuple]:
    """Execute a query and return the first row (or None)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        put_conn(conn)


# ── Idempotent upsert helpers ────────────────────────────────────────

def upsert_ticket(ticket: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    """Insert or update a ticket row.  Idempotent on ticket_id.

    *ticket* must contain at least ``ticket_id``.  All other keys are
    optional and map to column names.  Unknown keys are silently ignored.
    ``source_payload`` should be the raw TS API dict (stored as JSONB).
    """
    now = now or datetime.now(timezone.utc)
    tid = ticket["ticket_id"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tickets (
                    ticket_id, ticket_number, ticket_name,
                    status, severity, product_name, assignee, customer,
                    date_created, date_modified, closed_at,
                    days_opened, days_since_modified,
                    source_updated_at, source_payload,
                    first_ingested_at, last_ingested_at, last_seen_at
                ) VALUES (
                    %(ticket_id)s, %(ticket_number)s, %(ticket_name)s,
                    %(status)s, %(severity)s, %(product_name)s,
                    %(assignee)s, %(customer)s,
                    %(date_created)s, %(date_modified)s, %(closed_at)s,
                    %(days_opened)s, %(days_since_modified)s,
                    %(source_updated_at)s, %(source_payload)s,
                    %(now)s, %(now)s, %(now)s
                )
                ON CONFLICT (ticket_id) DO UPDATE SET
                    ticket_number       = COALESCE(EXCLUDED.ticket_number, tickets.ticket_number),
                    ticket_name         = EXCLUDED.ticket_name,
                    status              = EXCLUDED.status,
                    severity            = EXCLUDED.severity,
                    product_name        = EXCLUDED.product_name,
                    assignee            = EXCLUDED.assignee,
                    customer            = EXCLUDED.customer,
                    date_created        = COALESCE(EXCLUDED.date_created, tickets.date_created),
                    date_modified       = EXCLUDED.date_modified,
                    closed_at           = EXCLUDED.closed_at,
                    days_opened         = EXCLUDED.days_opened,
                    days_since_modified = EXCLUDED.days_since_modified,
                    source_updated_at   = EXCLUDED.source_updated_at,
                    source_payload      = COALESCE(EXCLUDED.source_payload, tickets.source_payload),
                    last_ingested_at    = %(now)s,
                    last_seen_at        = %(now)s;
            """, {
                "ticket_id": tid,
                "ticket_number": ticket.get("ticket_number"),
                "ticket_name": ticket.get("ticket_name"),
                "status": ticket.get("status"),
                "severity": ticket.get("severity"),
                "product_name": ticket.get("product_name"),
                "assignee": ticket.get("assignee"),
                "customer": ticket.get("customer"),
                "date_created": ticket.get("date_created"),
                "date_modified": ticket.get("date_modified"),
                "closed_at": ticket.get("closed_at"),
                "days_opened": ticket.get("days_opened"),
                "days_since_modified": ticket.get("days_since_modified"),
                "source_updated_at": ticket.get("source_updated_at"),
                "source_payload": psycopg2.extras.Json(ticket.get("source_payload"))
                    if ticket.get("source_payload") is not None else None,
                "now": now,
            })
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_action(action: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    """Insert or update a ticket_actions row.  Idempotent on action_id.

    *action* must contain at least ``action_id`` and ``ticket_id``.
    """
    now = now or datetime.now(timezone.utc)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_actions (
                    action_id, ticket_id, created_at, action_type,
                    creator_id, creator_name, party, is_visible,
                    description, cleaned_description,
                    action_class, is_empty, is_customer_visible,
                    source_payload,
                    first_ingested_at, last_ingested_at, last_seen_at
                ) VALUES (
                    %(action_id)s, %(ticket_id)s, %(created_at)s, %(action_type)s,
                    %(creator_id)s, %(creator_name)s, %(party)s, %(is_visible)s,
                    %(description)s, %(cleaned_description)s,
                    %(action_class)s, %(is_empty)s, %(is_customer_visible)s,
                    %(source_payload)s,
                    %(now)s, %(now)s, %(now)s
                )
                ON CONFLICT (action_id) DO UPDATE SET
                    ticket_id           = EXCLUDED.ticket_id,
                    created_at          = COALESCE(EXCLUDED.created_at, ticket_actions.created_at),
                    action_type         = EXCLUDED.action_type,
                    creator_id          = EXCLUDED.creator_id,
                    creator_name        = EXCLUDED.creator_name,
                    party               = EXCLUDED.party,
                    is_visible          = EXCLUDED.is_visible,
                    description         = EXCLUDED.description,
                    cleaned_description = EXCLUDED.cleaned_description,
                    action_class        = COALESCE(EXCLUDED.action_class, ticket_actions.action_class),
                    is_empty            = EXCLUDED.is_empty,
                    is_customer_visible = EXCLUDED.is_customer_visible,
                    source_payload      = COALESCE(EXCLUDED.source_payload, ticket_actions.source_payload),
                    last_ingested_at    = %(now)s,
                    last_seen_at        = %(now)s;
            """, {
                "action_id": action["action_id"],
                "ticket_id": action["ticket_id"],
                "created_at": action.get("created_at"),
                "action_type": action.get("action_type"),
                "creator_id": action.get("creator_id"),
                "creator_name": action.get("creator_name"),
                "party": action.get("party"),
                "is_visible": action.get("is_visible"),
                "description": action.get("description"),
                "cleaned_description": action.get("cleaned_description"),
                "action_class": action.get("action_class"),
                "is_empty": action.get("is_empty", False),
                "is_customer_visible": action.get("is_customer_visible"),
                "source_payload": psycopg2.extras.Json(action.get("source_payload"))
                    if action.get("source_payload") is not None else None,
                "now": now,
            })
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_ticket_with_actions(
    ticket: Dict[str, Any],
    actions: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> None:
    """Upsert a ticket and all its actions in a single DB transaction.

    This prevents partial data when the process crashes between upserting the
    ticket and its actions.
    """
    now = now or datetime.now(timezone.utc)
    tid = ticket["ticket_id"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # -- ticket upsert --
            cur.execute("""
                INSERT INTO tickets (
                    ticket_id, ticket_number, ticket_name,
                    status, severity, product_name, assignee, customer,
                    date_created, date_modified, closed_at,
                    days_opened, days_since_modified,
                    source_updated_at, source_payload,
                    first_ingested_at, last_ingested_at, last_seen_at
                ) VALUES (
                    %(ticket_id)s, %(ticket_number)s, %(ticket_name)s,
                    %(status)s, %(severity)s, %(product_name)s,
                    %(assignee)s, %(customer)s,
                    %(date_created)s, %(date_modified)s, %(closed_at)s,
                    %(days_opened)s, %(days_since_modified)s,
                    %(source_updated_at)s, %(source_payload)s,
                    %(now)s, %(now)s, %(now)s
                )
                ON CONFLICT (ticket_id) DO UPDATE SET
                    ticket_number       = COALESCE(EXCLUDED.ticket_number, tickets.ticket_number),
                    ticket_name         = EXCLUDED.ticket_name,
                    status              = EXCLUDED.status,
                    severity            = EXCLUDED.severity,
                    product_name        = EXCLUDED.product_name,
                    assignee            = EXCLUDED.assignee,
                    customer            = EXCLUDED.customer,
                    date_created        = COALESCE(EXCLUDED.date_created, tickets.date_created),
                    date_modified       = EXCLUDED.date_modified,
                    closed_at           = EXCLUDED.closed_at,
                    days_opened         = EXCLUDED.days_opened,
                    days_since_modified = EXCLUDED.days_since_modified,
                    source_updated_at   = EXCLUDED.source_updated_at,
                    source_payload      = COALESCE(EXCLUDED.source_payload, tickets.source_payload),
                    last_ingested_at    = %(now)s,
                    last_seen_at        = %(now)s;
            """, {
                "ticket_id": tid,
                "ticket_number": ticket.get("ticket_number"),
                "ticket_name": ticket.get("ticket_name"),
                "status": ticket.get("status"),
                "severity": ticket.get("severity"),
                "product_name": ticket.get("product_name"),
                "assignee": ticket.get("assignee"),
                "customer": ticket.get("customer"),
                "date_created": ticket.get("date_created"),
                "date_modified": ticket.get("date_modified"),
                "closed_at": ticket.get("closed_at"),
                "days_opened": ticket.get("days_opened"),
                "days_since_modified": ticket.get("days_since_modified"),
                "source_updated_at": ticket.get("source_updated_at"),
                "source_payload": psycopg2.extras.Json(ticket.get("source_payload"))
                    if ticket.get("source_payload") is not None else None,
                "now": now,
            })

            # -- action upserts (same transaction) --
            for action in actions:
                cur.execute("""
                    INSERT INTO ticket_actions (
                        action_id, ticket_id, created_at, action_type,
                        creator_id, creator_name, party, is_visible,
                        description, cleaned_description,
                        action_class, is_empty, is_customer_visible,
                        source_payload,
                        first_ingested_at, last_ingested_at, last_seen_at
                    ) VALUES (
                        %(action_id)s, %(ticket_id)s, %(created_at)s, %(action_type)s,
                        %(creator_id)s, %(creator_name)s, %(party)s, %(is_visible)s,
                        %(description)s, %(cleaned_description)s,
                        %(action_class)s, %(is_empty)s, %(is_customer_visible)s,
                        %(source_payload)s,
                        %(now)s, %(now)s, %(now)s
                    )
                    ON CONFLICT (action_id) DO UPDATE SET
                        ticket_id           = EXCLUDED.ticket_id,
                        created_at          = COALESCE(EXCLUDED.created_at, ticket_actions.created_at),
                        action_type         = EXCLUDED.action_type,
                        creator_id          = EXCLUDED.creator_id,
                        creator_name        = EXCLUDED.creator_name,
                        party               = EXCLUDED.party,
                        is_visible          = EXCLUDED.is_visible,
                        description         = EXCLUDED.description,
                        cleaned_description = EXCLUDED.cleaned_description,
                        action_class        = COALESCE(EXCLUDED.action_class, ticket_actions.action_class),
                        is_empty            = EXCLUDED.is_empty,
                        is_customer_visible = EXCLUDED.is_customer_visible,
                        source_payload      = COALESCE(EXCLUDED.source_payload, ticket_actions.source_payload),
                        last_ingested_at    = %(now)s,
                        last_seen_at        = %(now)s;
                """, {
                    "action_id": action["action_id"],
                    "ticket_id": action["ticket_id"],
                    "created_at": action.get("created_at"),
                    "action_type": action.get("action_type"),
                    "creator_id": action.get("creator_id"),
                    "creator_name": action.get("creator_name"),
                    "party": action.get("party"),
                    "is_visible": action.get("is_visible"),
                    "description": action.get("description"),
                    "cleaned_description": action.get("cleaned_description"),
                    "action_class": action.get("action_class"),
                    "is_empty": action.get("is_empty", False),
                    "is_customer_visible": action.get("is_customer_visible"),
                    "source_payload": psycopg2.extras.Json(action.get("source_payload"))
                        if action.get("source_payload") is not None else None,
                    "now": now,
                })

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_sync_state(
    source_name: str,
    *,
    status: str,
    error: Optional[str] = None,
    cursor: Optional[str] = None,
    is_success: bool = False,
) -> None:
    """Upsert the sync-state row for a named source."""
    now = datetime.now(timezone.utc)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sync_state (
                    source_name, last_attempted_sync_at,
                    last_successful_sync_at, last_status, last_error,
                    last_cursor, updated_at
                ) VALUES (
                    %(source_name)s, %(now)s,
                    %(success_at)s, %(status)s, %(error)s,
                    %(cursor)s, %(now)s
                )
                ON CONFLICT (source_name) DO UPDATE SET
                    last_attempted_sync_at  = %(now)s,
                    last_successful_sync_at = CASE
                        WHEN %(is_success)s THEN %(now)s
                        ELSE sync_state.last_successful_sync_at
                    END,
                    last_status  = %(status)s,
                    last_error   = %(error)s,
                    last_cursor  = COALESCE(%(cursor)s, sync_state.last_cursor),
                    updated_at   = %(now)s;
            """, {
                "source_name": source_name,
                "now": now,
                "success_at": now if is_success else None,
                "status": status,
                "error": error,
                "cursor": cursor,
                "is_success": is_success,
            })
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def create_ingest_run(source_name: str, config_snapshot: Optional[Dict] = None) -> uuid.UUID:
    """Insert a new ingest_runs row and return its UUID."""
    run_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingest_runs (
                    ingest_run_id, source_name, started_at, status, config_snapshot
                ) VALUES (%s, %s, %s, 'running', %s);
            """, (
                run_id,
                source_name,
                now,
                psycopg2.extras.Json(config_snapshot) if config_snapshot else None,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
    return run_id


def complete_ingest_run(
    run_id: uuid.UUID,
    *,
    status: str = "completed",
    tickets_seen: int = 0,
    tickets_upserted: int = 0,
    actions_seen: int = 0,
    actions_upserted: int = 0,
    error_text: Optional[str] = None,
) -> None:
    """Mark an ingest run as completed (or failed)."""
    now = datetime.now(timezone.utc)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ingest_runs SET
                    completed_at     = %s,
                    status           = %s,
                    tickets_seen     = %s,
                    tickets_upserted = %s,
                    actions_seen     = %s,
                    actions_upserted = %s,
                    error_text       = %s
                WHERE ingest_run_id = %s;
            """, (
                now, status,
                tickets_seen, tickets_upserted,
                actions_seen, actions_upserted,
                error_text,
                run_id,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


# ── Enrichment persistence helpers ───────────────────────────────────

def get_latest_enrichment_hash(
    ticket_id: int,
    enrichment_type: str,
) -> Optional[str]:
    """Return the content hash from the most recent enrichment row for a ticket.

    *enrichment_type* is one of: ``"sentiment"``, ``"priority"``, ``"complexity"``.
    Returns None if no prior row exists.
    """
    _TABLE_MAP = {
        "sentiment":  ("ticket_sentiment",        "thread_hash"),
        "priority":   ("ticket_priority_scores",   "thread_hash"),
        "complexity": ("ticket_complexity_scores",  "technical_core_hash"),
    }
    table, hash_col = _TABLE_MAP[enrichment_type]
    row = fetch_one(
        f"SELECT {hash_col} FROM {table} WHERE ticket_id = %s "
        f"ORDER BY scored_at DESC LIMIT 1;",
        (ticket_id,),
    )
    return row[0] if row else None


def get_current_hashes(ticket_id: int) -> Dict[str, Optional[str]]:
    """Return current thread_hash and technical_core_hash from rollups."""
    row = fetch_one(
        "SELECT thread_hash, technical_core_hash FROM ticket_thread_rollups "
        "WHERE ticket_id = %s;",
        (ticket_id,),
    )
    if row:
        return {"thread_hash": row[0], "technical_core_hash": row[1]}
    return {"thread_hash": None, "technical_core_hash": None}


def insert_sentiment(
    ticket_id: int,
    *,
    thread_hash: Optional[str] = None,
    model_name: Optional[str] = None,
    prompt_name: Optional[str] = None,
    prompt_version: Optional[str] = None,
    frustrated: Optional[str] = None,
    activity_id: Optional[str] = None,
    created_at: Optional[str] = None,
    source_file: Optional[str] = None,
    raw_response: Any = None,
) -> None:
    """Append a sentiment scoring row (append-only)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_sentiment (
                    ticket_id, thread_hash, model_name, prompt_name,
                    prompt_version, frustrated, activity_id, created_at,
                    source_file, raw_response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                ticket_id, thread_hash, model_name, prompt_name,
                prompt_version, frustrated, activity_id, created_at,
                source_file,
                psycopg2.extras.Json(raw_response) if raw_response is not None else None,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def insert_priority(
    ticket_id: int,
    *,
    thread_hash: Optional[str] = None,
    model_name: Optional[str] = None,
    prompt_name: Optional[str] = None,
    prompt_version: Optional[str] = None,
    priority: Optional[int] = None,
    priority_explanation: Optional[str] = None,
    raw_response: Any = None,
) -> None:
    """Append a priority scoring row (append-only)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_priority_scores (
                    ticket_id, thread_hash, model_name, prompt_name,
                    prompt_version, priority, priority_explanation,
                    raw_response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                ticket_id, thread_hash, model_name, prompt_name,
                prompt_version, priority, priority_explanation,
                psycopg2.extras.Json(raw_response) if raw_response is not None else None,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def insert_complexity(
    ticket_id: int,
    *,
    technical_core_hash: Optional[str] = None,
    model_name: Optional[str] = None,
    prompt_name: Optional[str] = None,
    prompt_version: Optional[str] = None,
    intrinsic_complexity: Optional[int] = None,
    coordination_load: Optional[int] = None,
    elapsed_drag: Optional[int] = None,
    overall_complexity: Optional[int] = None,
    confidence: Optional[float] = None,
    primary_complexity_drivers: Any = None,
    complexity_summary: Optional[str] = None,
    evidence: Any = None,
    noise_factors: Any = None,
    duration_vs_complexity_note: Optional[str] = None,
    raw_response: Any = None,
) -> None:
    """Append a complexity scoring row (append-only)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_complexity_scores (
                    ticket_id, technical_core_hash, model_name, prompt_name,
                    prompt_version, intrinsic_complexity, coordination_load,
                    elapsed_drag, overall_complexity, confidence,
                    primary_complexity_drivers, complexity_summary,
                    evidence, noise_factors, duration_vs_complexity_note,
                    raw_response
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """, (
                ticket_id, technical_core_hash, model_name, prompt_name,
                prompt_version, intrinsic_complexity, coordination_load,
                elapsed_drag, overall_complexity, confidence,
                psycopg2.extras.Json(primary_complexity_drivers)
                    if primary_complexity_drivers is not None else None,
                complexity_summary,
                psycopg2.extras.Json(evidence) if evidence is not None else None,
                psycopg2.extras.Json(noise_factors) if noise_factors is not None else None,
                duration_vs_complexity_note,
                psycopg2.extras.Json(raw_response) if raw_response is not None else None,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def load_ticket_with_actions(ticket_id: int) -> Optional[Dict[str, Any]]:
    """Load a ticket and its actions from DB in the same shape the JSON
    pipeline uses.  Returns None if ticket not found.
    """
    trow = fetch_one(
        "SELECT ticket_id, ticket_number, ticket_name, status, severity, "
        "       product_name, assignee, customer, date_created, date_modified, "
        "       days_opened, days_since_modified "
        "FROM tickets WHERE ticket_id = %s;",
        (ticket_id,),
    )
    if not trow:
        return None

    ticket = {
        "ticket_id": str(trow[0]),
        "ticket_number": trow[1] or "",
        "ticket_name": trow[2] or "",
        "status": trow[3] or "",
        "severity": trow[4] or "",
        "product_name": trow[5] or "",
        "assignee": trow[6] or "",
        "customer": trow[7] or "",
        "date_created": trow[8].isoformat() if trow[8] else "",
        "date_modified": trow[9].isoformat() if trow[9] else "",
        "days_opened": trow[10] if trow[10] is not None else "",
        "days_since_modified": trow[11] if trow[11] is not None else "",
    }

    arows = fetch_all(
        "SELECT action_id, created_at, action_type, creator_id, creator_name, "
        "       party, is_visible, cleaned_description "
        "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
        (ticket_id,),
    )
    activities = []
    for a in arows:
        activities.append({
            "action_id": str(a[0]),
            "created_at": a[1].isoformat() if a[1] else "",
            "action_type": a[2] or "",
            "creator_id": str(a[3]) if a[3] else "",
            "creator_name": a[4] or "",
            "party": a[5] or "",
            "is_visible": a[6],
            "description": a[7] or "",
        })
    ticket["activities"] = activities
    return ticket


def ticket_ids_for_numbers(ticket_numbers: list[str]) -> Dict[str, int]:
    """Return a mapping from ticket_number → ticket_id for the given numbers."""
    if not ticket_numbers:
        return {}
    placeholders = ",".join(["%s"] * len(ticket_numbers))
    rows = fetch_all(
        f"SELECT ticket_number, ticket_id FROM tickets WHERE ticket_number IN ({placeholders});",
        tuple(ticket_numbers),
    )
    return {str(r[0]): r[1] for r in rows}


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        applied = migrate()
        if applied:
            print(f"[db] Done — applied {len(applied)} migration(s): {', '.join(applied)}")
        else:
            print("[db] Nothing to apply.")
    else:
        print("Usage: python db.py migrate")
        print("  Applies any unapplied SQL files from migrations/ in order.")
