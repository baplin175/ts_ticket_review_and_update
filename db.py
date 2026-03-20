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
                    action_id, ticket_id, ticket_number, created_at, action_type,
                    creator_id, creator_name, party, is_visible,
                    description, cleaned_description,
                    action_class, is_empty, is_customer_visible,
                    source_payload,
                    first_ingested_at, last_ingested_at, last_seen_at
                ) VALUES (
                    %(action_id)s, %(ticket_id)s, %(ticket_number)s, %(created_at)s, %(action_type)s,
                    %(creator_id)s, %(creator_name)s, %(party)s, %(is_visible)s,
                    %(description)s, %(cleaned_description)s,
                    %(action_class)s, %(is_empty)s, %(is_customer_visible)s,
                    %(source_payload)s,
                    %(now)s, %(now)s, %(now)s
                )
                ON CONFLICT (action_id) DO UPDATE SET
                    ticket_id           = EXCLUDED.ticket_id,
                    ticket_number       = COALESCE(EXCLUDED.ticket_number, ticket_actions.ticket_number),
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
                "ticket_number": action.get("ticket_number"),
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
                        action_id, ticket_id, ticket_number, created_at, action_type,
                        creator_id, creator_name, party, is_visible,
                        description, cleaned_description,
                        action_class, is_empty, is_customer_visible,
                        source_payload,
                        first_ingested_at, last_ingested_at, last_seen_at
                    ) VALUES (
                        %(action_id)s, %(ticket_id)s, %(ticket_number)s, %(created_at)s, %(action_type)s,
                        %(creator_id)s, %(creator_name)s, %(party)s, %(is_visible)s,
                        %(description)s, %(cleaned_description)s,
                        %(action_class)s, %(is_empty)s, %(is_customer_visible)s,
                        %(source_payload)s,
                        %(now)s, %(now)s, %(now)s
                    )
                    ON CONFLICT (action_id) DO UPDATE SET
                        ticket_id           = EXCLUDED.ticket_id,
                        ticket_number       = COALESCE(EXCLUDED.ticket_number, ticket_actions.ticket_number),
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
                    "ticket_number": action.get("ticket_number"),
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


def get_sync_state(source_name: str) -> Optional[Dict[str, Any]]:
    """Read the sync_state row for *source_name*.

    Returns a dict with keys matching the table columns, or None if no
    row exists yet (first run).  Used to retrieve the watermark
    (``last_successful_sync_at``) for incremental syncs.
    """
    row = fetch_one(
        "SELECT source_name, last_successful_sync_at, last_attempted_sync_at, "
        "       last_status, last_error, last_cursor, updated_at "
        "FROM sync_state WHERE source_name = %s;",
        (source_name,),
    )
    if not row:
        return None
    return {
        "source_name": row[0],
        "last_successful_sync_at": row[1],
        "last_attempted_sync_at": row[2],
        "last_status": row[3],
        "last_error": row[4],
        "last_cursor": row[5],
        "updated_at": row[6],
    }


def upsert_sync_state(
    source_name: str,
    *,
    status: str,
    error: Optional[str] = None,
    cursor: Optional[str] = None,
    is_success: bool = False,
    watermark_at: Optional[datetime] = None,
) -> None:
    """Upsert the sync-state row for a named source.

    When *watermark_at* is provided and *is_success* is True, the watermark
    is set to that timestamp instead of the current time.  This allows the
    caller to advance the watermark only as far as the last ticket that was
    actually processed (important when MAX_TICKETS truncates the result set).
    """
    now = datetime.now(timezone.utc)
    success_ts = watermark_at or now
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
                        WHEN %(is_success)s THEN %(success_ts)s
                        ELSE sync_state.last_successful_sync_at
                    END,
                    last_status  = %(status)s,
                    last_error   = %(error)s,
                    last_cursor  = COALESCE(%(cursor)s, sync_state.last_cursor),
                    updated_at   = %(now)s;
            """, {
                "source_name": source_name,
                "now": now,
                "success_at": success_ts if is_success else None,
                "success_ts": success_ts,
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
    ticket_number: Optional[str] = None,
    thread_hash: Optional[str] = None,
    model_name: Optional[str] = None,
    prompt_name: Optional[str] = None,
    prompt_version: Optional[str] = None,
    frustrated: Optional[str] = None,
    frustrated_reason: Optional[str] = None,
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
                    ticket_id, ticket_number, thread_hash, model_name, prompt_name,
                    prompt_version, frustrated, frustrated_reason, activity_id, created_at,
                    source_file, raw_response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                ticket_id, ticket_number, thread_hash, model_name, prompt_name,
                prompt_version, frustrated, frustrated_reason, activity_id, created_at,
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
    ticket_number: Optional[str] = None,
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
                    ticket_id, ticket_number, thread_hash, model_name, prompt_name,
                    prompt_version, priority, priority_explanation,
                    raw_response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                ticket_id, ticket_number, thread_hash, model_name, prompt_name,
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
    ticket_number: Optional[str] = None,
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
                    ticket_id, ticket_number, technical_core_hash, model_name, prompt_name,
                    prompt_version, intrinsic_complexity, coordination_load,
                    elapsed_drag, overall_complexity, confidence,
                    primary_complexity_drivers, complexity_summary,
                    evidence, noise_factors, duration_vs_complexity_note,
                    raw_response
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """, (
                ticket_id, ticket_number, technical_core_hash, model_name, prompt_name,
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


def ticket_numbers_for_ids(ticket_ids: list[int]) -> Dict[int, str]:
    """Return a mapping from ticket_id → ticket_number for the given IDs."""
    if not ticket_ids:
        return {}
    placeholders = ",".join(["%s"] * len(ticket_ids))
    rows = fetch_all(
        f"SELECT ticket_id, ticket_number FROM tickets WHERE ticket_id IN ({placeholders});",
        tuple(ticket_ids),
    )
    return {r[0]: (str(r[1]) if r[1] else None) for r in rows}


def fetch_ticket_numbers_by_status(status: str, *, exclude_closed: bool = False) -> list[str]:
    """Return all ticket_numbers matching the given status that have rollups.

    Special values:
      - ``"Open"`` — all non-closed tickets (closed_at IS NULL and status
        NOT IN ('Closed', 'Closed with Survey')).
      - Any other string — exact status match.

    When *exclude_closed* is True, *status* is ignored and all tickets
    whose status is NOT 'Closed' or 'Closed with Survey' are returned.
    """
    if exclude_closed:
        rows = fetch_all(
            """SELECT t.ticket_number
                 FROM tickets t
                 JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
                WHERE COALESCE(t.status, '') NOT IN ('Closed', 'Closed with Survey')
                  AND r.thread_hash IS NOT NULL
                ORDER BY t.ticket_number;""",
        )
    elif status == "Open":
        rows = fetch_all(
            """SELECT t.ticket_number
                 FROM tickets t
                 JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
                WHERE t.closed_at IS NULL
                  AND COALESCE(t.status, '') NOT IN ('Closed', 'Closed with Survey')
                  AND r.thread_hash IS NOT NULL
                ORDER BY t.ticket_number;""",
        )
    else:
        rows = fetch_all(
            """SELECT t.ticket_number
                 FROM tickets t
                 JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
                WHERE t.status = %s AND r.thread_hash IS NOT NULL
                ORDER BY t.ticket_number;""",
            (status,),
        )
    return [str(r[0]) for r in rows]


def get_open_ticket_ids() -> list[int]:
    """Return ticket_ids for all tickets the DB considers still open (closed_at IS NULL)."""
    rows = fetch_all(
        "SELECT ticket_id FROM tickets WHERE closed_at IS NULL ORDER BY ticket_id;"
    )
    return [r[0] for r in rows]


# ── Analytics rebuild helpers ────────────────────────────────────────

def delete_for_tickets(table: str, ticket_ids: list[int]) -> int:
    """DELETE all rows in *table* for the given ticket_ids.  Returns count deleted."""
    if not ticket_ids:
        return 0
    placeholders = ",".join(["%s"] * len(ticket_ids))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE ticket_id IN ({placeholders});",
                tuple(ticket_ids),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def bulk_insert(table: str, columns: list[str], rows: list[tuple]) -> int:
    """INSERT multiple rows into *table*.  Returns count inserted."""
    if not rows:
        return 0
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders});"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_snapshot_daily(row: Dict[str, Any]) -> None:
    """Upsert a single ticket_snapshots_daily row."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_snapshots_daily (
                    snapshot_date, ticket_id, ticket_number, ticket_name,
                    status, owner, product_name, customer,
                    open_flag, age_days, days_since_modified,
                    priority, overall_complexity, waiting_state,
                    high_priority_flag, high_complexity_flag,
                    source_updated_at
                ) VALUES (
                    %(snapshot_date)s, %(ticket_id)s, %(ticket_number)s, %(ticket_name)s,
                    %(status)s, %(owner)s, %(product_name)s, %(customer)s,
                    %(open_flag)s, %(age_days)s, %(days_since_modified)s,
                    %(priority)s, %(overall_complexity)s, %(waiting_state)s,
                    %(high_priority_flag)s, %(high_complexity_flag)s,
                    %(source_updated_at)s
                )
                ON CONFLICT (snapshot_date, ticket_id) DO UPDATE SET
                    ticket_number      = EXCLUDED.ticket_number,
                    ticket_name        = EXCLUDED.ticket_name,
                    status             = EXCLUDED.status,
                    owner              = EXCLUDED.owner,
                    product_name       = EXCLUDED.product_name,
                    customer           = EXCLUDED.customer,
                    open_flag          = EXCLUDED.open_flag,
                    age_days           = EXCLUDED.age_days,
                    days_since_modified= EXCLUDED.days_since_modified,
                    priority           = EXCLUDED.priority,
                    overall_complexity = EXCLUDED.overall_complexity,
                    waiting_state      = EXCLUDED.waiting_state,
                    high_priority_flag = EXCLUDED.high_priority_flag,
                    high_complexity_flag= EXCLUDED.high_complexity_flag,
                    source_updated_at  = EXCLUDED.source_updated_at;
            """, row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_customer_health(row: Dict[str, Any]) -> None:
    """Upsert a customer_ticket_health row."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customer_ticket_health (
                    as_of_date, customer,
                    open_ticket_count, high_priority_count, high_complexity_count,
                    avg_complexity, avg_elapsed_drag,
                    reopen_count_90d, frustration_count_90d,
                    top_cluster_ids, top_products,
                    ticket_load_pressure_score
                ) VALUES (
                    %(as_of_date)s, %(customer)s,
                    %(open_ticket_count)s, %(high_priority_count)s, %(high_complexity_count)s,
                    %(avg_complexity)s, %(avg_elapsed_drag)s,
                    %(reopen_count_90d)s, %(frustration_count_90d)s,
                    %(top_cluster_ids)s, %(top_products)s,
                    %(ticket_load_pressure_score)s
                )
                ON CONFLICT (as_of_date, customer) DO UPDATE SET
                    open_ticket_count   = EXCLUDED.open_ticket_count,
                    high_priority_count = EXCLUDED.high_priority_count,
                    high_complexity_count= EXCLUDED.high_complexity_count,
                    avg_complexity      = EXCLUDED.avg_complexity,
                    avg_elapsed_drag    = EXCLUDED.avg_elapsed_drag,
                    reopen_count_90d    = EXCLUDED.reopen_count_90d,
                    frustration_count_90d= EXCLUDED.frustration_count_90d,
                    top_cluster_ids     = EXCLUDED.top_cluster_ids,
                    top_products        = EXCLUDED.top_products,
                    ticket_load_pressure_score = EXCLUDED.ticket_load_pressure_score;
            """, row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_product_health(row: Dict[str, Any]) -> None:
    """Upsert a product_ticket_health row."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO product_ticket_health (
                    as_of_date, product_name,
                    ticket_volume, avg_complexity, avg_coordination_load,
                    avg_elapsed_drag, top_clusters, top_mechanisms,
                    dev_touched_rate, customer_wait_rate
                ) VALUES (
                    %(as_of_date)s, %(product_name)s,
                    %(ticket_volume)s, %(avg_complexity)s, %(avg_coordination_load)s,
                    %(avg_elapsed_drag)s, %(top_clusters)s, %(top_mechanisms)s,
                    %(dev_touched_rate)s, %(customer_wait_rate)s
                )
                ON CONFLICT (as_of_date, product_name) DO UPDATE SET
                    ticket_volume       = EXCLUDED.ticket_volume,
                    avg_complexity      = EXCLUDED.avg_complexity,
                    avg_coordination_load= EXCLUDED.avg_coordination_load,
                    avg_elapsed_drag    = EXCLUDED.avg_elapsed_drag,
                    top_clusters        = EXCLUDED.top_clusters,
                    top_mechanisms      = EXCLUDED.top_mechanisms,
                    dev_touched_rate    = EXCLUDED.dev_touched_rate,
                    customer_wait_rate  = EXCLUDED.customer_wait_rate;
            """, row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def upsert_daily_open_count(row: Dict[str, Any]) -> None:
    """Upsert a single daily_open_counts row."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_open_counts (
                    snapshot_date, product_name, status,
                    participant_id, participant_name, participant_type,
                    open_count
                ) VALUES (
                    %(snapshot_date)s, %(product_name)s, %(status)s,
                    %(participant_id)s, %(participant_name)s, %(participant_type)s,
                    %(open_count)s
                )
                ON CONFLICT (snapshot_date, product_name, status, participant_id)
                DO UPDATE SET
                    participant_name = EXCLUDED.participant_name,
                    participant_type = EXCLUDED.participant_type,
                    open_count       = EXCLUDED.open_count;
            """, row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def daily_open_counts_existing_dates() -> set:
    """Return the set of snapshot_dates already present in daily_open_counts."""
    rows = fetch_all("SELECT DISTINCT snapshot_date FROM daily_open_counts;")
    return {r[0] for r in rows}


# ── LLM multi-pass pipeline helpers ──────────────────────────────────

def insert_pass_result(
    ticket_id: int,
    *,
    pass_name: str,
    prompt_version: str,
    model_name: Optional[str] = None,
    input_text: Optional[str] = None,
    status: str = "pending",
    raw_response_text: Optional[str] = None,
    parsed_json: Any = None,
    phenomenon: Optional[str] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
) -> int:
    """Insert a new LLM pass result row.  Returns the new row id.

    For idempotent success: uses ON CONFLICT on the partial unique index
    (ticket_id, pass_name, prompt_version WHERE status='success') to
    update the existing success row rather than creating a duplicate.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_llm_pass_results (
                    ticket_id, pass_name, prompt_version, model_name,
                    input_text, status, raw_response_text, parsed_json,
                    phenomenon, error_message, started_at, completed_at,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                )
                RETURNING id;
            """, (
                ticket_id, pass_name, prompt_version, model_name,
                input_text, status, raw_response_text,
                psycopg2.extras.Json(parsed_json) if parsed_json is not None else None,
                phenomenon, error_message, started_at, completed_at,
            ))
            row_id = cur.fetchone()[0]
        conn.commit()
        return row_id
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def update_pass_result(
    row_id: int,
    *,
    status: str,
    raw_response_text: Optional[str] = None,
    parsed_json: Any = None,
    phenomenon: Optional[str] = None,
    error_message: Optional[str] = None,
    completed_at: Optional[datetime] = None,
    component: Optional[str] = None,
    operation: Optional[str] = None,
    unexpected_state: Optional[str] = None,
    canonical_failure: Optional[str] = None,
    mechanism: Optional[str] = None,
    mechanism_class: Optional[str] = None,
    intervention_type: Optional[str] = None,
    intervention_action: Optional[str] = None,
) -> None:
    """Update an existing pass result row (e.g. pending → success/failed).

    Pass 1 callers use *phenomenon*; Pass 2 callers use *component*,
    *operation*, *unexpected_state*, *canonical_failure*; Pass 3 callers
    use *mechanism*; Pass 4 callers use *mechanism_class*,
    *intervention_type*, *intervention_action*.
    Unused kwargs default to None and are written as NULL.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ticket_llm_pass_results
                   SET status            = %s,
                       raw_response_text  = %s,
                       parsed_json        = %s,
                       phenomenon         = %s,
                       error_message      = %s,
                       completed_at       = %s,
                       component          = %s,
                       operation          = %s,
                       unexpected_state   = %s,
                       canonical_failure  = %s,
                       mechanism          = %s,
                       mechanism_class    = %s,
                       intervention_type  = %s,
                       intervention_action = %s,
                       updated_at         = now()
                 WHERE id = %s;
            """, (
                status, raw_response_text,
                psycopg2.extras.Json(parsed_json) if parsed_json is not None else None,
                phenomenon, error_message, completed_at,
                component, operation, unexpected_state, canonical_failure,
                mechanism,
                mechanism_class, intervention_type, intervention_action,
                row_id,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def delete_prior_failed_pass(
    ticket_id: int, pass_name: str, prompt_version: str
) -> int:
    """Remove any previous failed/pending rows for the ticket+pass+version.

    This keeps the table clean before inserting a fresh attempt.
    Returns the number of rows deleted.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM ticket_llm_pass_results
                 WHERE ticket_id = %s
                   AND pass_name = %s
                   AND prompt_version = %s
                   AND status IN ('pending', 'failed');
            """, (ticket_id, pass_name, prompt_version))
            deleted = cur.rowcount
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def get_latest_pass_result(
    ticket_id: int, pass_name: str, prompt_version: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Return the latest pass result for a ticket.

    If *prompt_version* is given, filter by that version.
    Prefers 'success' rows; falls back to most recent by updated_at.
    """
    if prompt_version:
        row = fetch_one(
            "SELECT id, status, phenomenon, error_message, completed_at "
            "FROM ticket_llm_pass_results "
            "WHERE ticket_id = %s AND pass_name = %s AND prompt_version = %s "
            "ORDER BY CASE WHEN status = 'success' THEN 0 ELSE 1 END, "
            "         updated_at DESC LIMIT 1;",
            (ticket_id, pass_name, prompt_version),
        )
    else:
        row = fetch_one(
            "SELECT id, status, phenomenon, error_message, completed_at "
            "FROM ticket_llm_pass_results "
            "WHERE ticket_id = %s AND pass_name = %s "
            "ORDER BY CASE WHEN status = 'success' THEN 0 ELSE 1 END, "
            "         updated_at DESC LIMIT 1;",
            (ticket_id, pass_name),
        )
    if not row:
        return None
    return {
        "id": row[0],
        "status": row[1],
        "phenomenon": row[2],
        "error_message": row[3],
        "completed_at": row[4],
    }


def fetch_pending_pass1_tickets(
    prompt_version: str,
    *,
    limit: int = 0,
    ticket_ids: Optional[List[int]] = None,
    failed_only: bool = False,
    force: bool = False,
    since: Optional[str] = None,
) -> List[Tuple]:
    """Return (ticket_id, ticket_name, full_thread_text) rows eligible for Pass 1.

    Selection logic:
      - ticket has non-null full_thread_text in rollups
      - no successful pass1_phenomenon result for current prompt_version
        (unless *force* is True)
      - optionally filtered to specific ticket_ids
      - optionally limited to failed-only reruns
      - optionally limited to tickets created after *since* date

    Returns list of (ticket_id, ticket_name, full_thread_text) tuples.
    """
    conditions = ["r.full_thread_text IS NOT NULL", "t.closed_at IS NOT NULL"]
    params: list = []

    if ticket_ids:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        conditions.append(f"t.ticket_id IN ({placeholders})")
        params.extend(ticket_ids)

    if since:
        conditions.append("t.date_created >= %s")
        params.append(since)

    if not force:
        # Exclude tickets that already have a successful pass1 for this version
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass1_phenomenon'
                   AND lp.prompt_version = %s
                   AND lp.status = 'success'
            )
        """)
        params.append(prompt_version)

    if failed_only:
        # Only pick tickets that have a failed pass1 for this version
        conditions.append("""
            EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass1_phenomenon'
                   AND lp.prompt_version = %s
                   AND lp.status = 'failed'
            )
        """)
        params.append(prompt_version)

    where_clause = " AND ".join(conditions)
    sql = (
        f"SELECT t.ticket_id, COALESCE(t.ticket_name, ''), r.full_thread_text "
        f"FROM tickets t "
        f"JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id "
        f"WHERE {where_clause} "
        f"ORDER BY t.ticket_id"
    )
    if limit > 0:
        sql += f" LIMIT {limit}"
    sql += ";"

    return fetch_all(sql, tuple(params))


def fetch_pending_pass2_tickets(
    pass2_prompt_version: str,
    *,
    pass1_pass_name: str = "pass1_phenomenon",
    pass1_prompt_version: str = "1",
    limit: int = 0,
    ticket_ids: Optional[List[int]] = None,
    failed_only: bool = False,
    force: bool = False,
) -> List[Tuple]:
    """Return (ticket_id, phenomenon) rows eligible for Pass 2.

    Selection logic:
      - ticket has a successful Pass 1 result with non-null phenomenon
      - no successful pass2_grammar result for current prompt_version
        (unless *force* is True)
      - optionally filtered to specific ticket_ids
      - optionally limited to failed-only reruns

    Returns list of (ticket_id, phenomenon) tuples.
    """
    conditions = [
        "p1.pass_name = %s",
        "p1.prompt_version = %s",
        "p1.status = 'success'",
        "p1.phenomenon IS NOT NULL",
        "p1.phenomenon != ''",
    ]
    params: list = [pass1_pass_name, pass1_prompt_version]

    if ticket_ids:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        conditions.append(f"t.ticket_id IN ({placeholders})")
        params.extend(ticket_ids)

    if not force:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass2_grammar'
                   AND lp.prompt_version = %s
                   AND lp.status = 'success'
            )
        """)
        params.append(pass2_prompt_version)

    if failed_only:
        conditions.append("""
            EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass2_grammar'
                   AND lp.prompt_version = %s
                   AND lp.status = 'failed'
            )
        """)
        params.append(pass2_prompt_version)

    where_clause = " AND ".join(conditions)
    sql = (
        f"SELECT t.ticket_id, p1.phenomenon "
        f"FROM tickets t "
        f"JOIN ticket_llm_pass_results p1 ON p1.ticket_id = t.ticket_id "
        f"WHERE {where_clause} "
        f"ORDER BY t.ticket_id"
    )
    if limit > 0:
        sql += f" LIMIT {limit}"
    sql += ";"

    return fetch_all(sql, tuple(params))


def fetch_pending_pass3_tickets(
    pass3_prompt_version: str,
    *,
    pass2_pass_name: str = "pass1_phenomenon",
    pass2_prompt_version: str = "2",
    limit: int = 0,
    ticket_ids: Optional[List[int]] = None,
    failed_only: bool = False,
    force: bool = False,
) -> List[Tuple]:
    """Return (ticket_id, canonical_failure, full_thread_text) rows eligible for Pass 3.

    Selection logic:
      - ticket has a successful Pass 2 result with non-null canonical_failure
      - no successful pass3_mechanism result for current prompt_version
        (unless *force* is True)
      - optionally filtered to specific ticket_ids
      - optionally limited to failed-only reruns

    Returns list of (ticket_id, canonical_failure, full_thread_text) tuples.
    """
    conditions = [
        "p2.pass_name = %s",
        "p2.prompt_version = %s",
        "p2.status = 'success'",
        "p2.canonical_failure IS NOT NULL",
        "p2.canonical_failure != ''",
        "t.closed_at IS NOT NULL",
    ]
    params: list = [pass2_pass_name, pass2_prompt_version]

    if ticket_ids:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        conditions.append(f"t.ticket_id IN ({placeholders})")
        params.extend(ticket_ids)

    if not force:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass3_mechanism'
                   AND lp.prompt_version = %s
                   AND lp.status = 'success'
            )
        """)
        params.append(pass3_prompt_version)

    if failed_only:
        conditions.append("""
            EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass3_mechanism'
                   AND lp.prompt_version = %s
                   AND lp.status = 'failed'
            )
        """)
        params.append(pass3_prompt_version)

    where_clause = " AND ".join(conditions)
    sql = (
        f"SELECT t.ticket_id, p2.canonical_failure, "
        f"COALESCE(r.full_thread_text, '') "
        f"FROM tickets t "
        f"JOIN ticket_llm_pass_results p2 ON p2.ticket_id = t.ticket_id "
        f"LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id "
        f"WHERE {where_clause} "
        f"ORDER BY t.ticket_id"
    )
    if limit > 0:
        sql += f" LIMIT {limit}"
    sql += ";"

    return fetch_all(sql, tuple(params))


def fetch_pending_pass4_tickets(
    pass4_prompt_version: str,
    *,
    pass3_pass_name: str = "pass3_mechanism",
    pass3_prompt_version: str = "1",
    limit: int = 0,
    ticket_ids: Optional[List[int]] = None,
    failed_only: bool = False,
    force: bool = False,
) -> List[Tuple]:
    """Return (ticket_id, mechanism) rows eligible for Pass 4.

    Selection logic:
      - ticket has a successful Pass 3 result with non-null mechanism
      - no successful pass4_intervention result for current prompt_version
        (unless *force* is True)
      - optionally filtered to specific ticket_ids
      - optionally limited to failed-only reruns

    Returns list of (ticket_id, mechanism) tuples.
    """
    conditions = [
        "p3.pass_name = %s",
        "p3.prompt_version = %s",
        "p3.status = 'success'",
        "p3.mechanism IS NOT NULL",
        "p3.mechanism != ''",
        "t.closed_at IS NOT NULL",
    ]
    params: list = [pass3_pass_name, pass3_prompt_version]

    if ticket_ids:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        conditions.append(f"t.ticket_id IN ({placeholders})")
        params.extend(ticket_ids)

    if not force:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass4_intervention'
                   AND lp.prompt_version = %s
                   AND lp.status = 'success'
            )
        """)
        params.append(pass4_prompt_version)

    if failed_only:
        conditions.append("""
            EXISTS (
                SELECT 1 FROM ticket_llm_pass_results lp
                 WHERE lp.ticket_id = t.ticket_id
                   AND lp.pass_name = 'pass4_intervention'
                   AND lp.prompt_version = %s
                   AND lp.status = 'failed'
            )
        """)
        params.append(pass4_prompt_version)

    where_clause = " AND ".join(conditions)
    sql = (
        f"SELECT t.ticket_id, p3.mechanism "
        f"FROM tickets t "
        f"JOIN ticket_llm_pass_results p3 ON p3.ticket_id = t.ticket_id "
        f"WHERE {where_clause} "
        f"ORDER BY t.ticket_id"
    )
    if limit > 0:
        sql += f" LIMIT {limit}"
    sql += ";"

    return fetch_all(sql, tuple(params))


def invalidate_stale_pass4(
    ticket_ids: List[int],
    pass3_pass_name: str = "pass3_mechanism",
    pass3_prompt_version: str = "1",
) -> int:
    """Mark existing P4 results as 'skipped' for tickets that lack a valid P3 result.

    When upstream P3 results change (e.g. a ticket is now correctly skipped at P3),
    stale P4 results from prior runs remain in the DB.  This function finds tickets
    in *ticket_ids* that have NO successful P3 row for the given version and sets
    any existing pass4_intervention rows to status='skipped'.

    Returns the number of rows updated.
    """
    if not ticket_ids:
        return 0
    placeholders = ",".join(["%s"] * len(ticket_ids))
    sql = f"""
        UPDATE ticket_llm_pass_results
           SET status = 'skipped',
               error_message = 'upstream P3 mechanism missing for required version'
         WHERE pass_name = 'pass4_intervention'
           AND ticket_id IN ({placeholders})
           AND NOT EXISTS (
               SELECT 1 FROM ticket_llm_pass_results p3
                WHERE p3.ticket_id = ticket_llm_pass_results.ticket_id
                  AND p3.pass_name = %s
                  AND p3.prompt_version = %s
                  AND p3.status = 'success'
                  AND p3.mechanism IS NOT NULL
                  AND p3.mechanism != ''
           );
    """
    params = list(ticket_ids) + [pass3_pass_name, pass3_prompt_version]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            updated = cur.rowcount
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


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
