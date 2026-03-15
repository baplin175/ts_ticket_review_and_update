"""
Tests for post-sync rollup rebuilds.

Verifies that after a successful sync (both API-based and CSV-based),
the rollup + analytics pipeline is automatically invoked for all
upserted ticket IDs.
"""

from unittest.mock import patch, call, MagicMock

import pytest


# ── Helpers ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    """Ensure db module thinks a DATABASE_URL is configured."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


# ── CSV import: run_import returns upserted_ids ─────────────────────

def test_csv_run_import_returns_upserted_ids(tmp_path):
    """run_import() should include sorted upserted ticket IDs in stats."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "Ticket ID,Ticket Number,Ticket Name,Ticket Product Name,"
        "Primary Customer,Severity,Group Name,Ticket Source,Ticket Type,"
        "Is Closed,Date Ticket Created,Days Opened,"
        "Days Since Ticket was Last Modified,"
        "Action Creator Name,Action Description,Action Type,"
        "Date Action Created,Action Hours Spent,Action Source\n"
        "100,T100,Test,Product,Customer,Low,Group,Web,Bug,false,"
        "2026-01-01,10,1,Alice,Hello world,Comment,2026-01-02,,Web\n"
        "200,T200,Test2,Product,Customer,Low,Group,Web,Bug,false,"
        "2026-01-01,5,1,Bob,Hi there,Comment,2026-01-03,,Web\n"
    )

    with patch("db._is_enabled", return_value=True), \
         patch("db.migrate"), \
         patch("db.create_ingest_run", return_value="run-1"), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket"), \
         patch("db.upsert_action"), \
         patch("run_csv_import._load_known_inh_names", return_value=set()), \
         patch("ts_client.fetch_all_users", return_value={}):

        from run_csv_import import run_import
        stats = run_import(str(csv_file), dry_run=False)

    assert "upserted_ids" in stats
    assert sorted(stats["upserted_ids"]) == [100, 200]


def test_csv_dry_run_returns_upserted_ids(tmp_path):
    """Dry-run still tracks tickets seen but doesn't write to DB."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "Ticket ID,Ticket Number,Ticket Name,Ticket Product Name,"
        "Primary Customer,Severity,Group Name,Ticket Source,Ticket Type,"
        "Is Closed,Date Ticket Created,Days Opened,"
        "Days Since Ticket was Last Modified,"
        "Action Creator Name,Action Description,Action Type,"
        "Date Action Created,Action Hours Spent,Action Source\n"
        "100,T100,Test,Product,Customer,Low,Group,Web,Bug,false,"
        "2026-01-01,10,1,Alice,Hello world,Comment,2026-01-02,,Web\n"
    )

    with patch("db._is_enabled", return_value=True), \
         patch("db.migrate"), \
         patch("run_csv_import._load_known_inh_names", return_value=set()), \
         patch("ts_client.fetch_all_users", return_value={}):

        from run_csv_import import run_import
        stats = run_import(str(csv_file), dry_run=True)

    assert "upserted_ids" in stats
    assert sorted(stats["upserted_ids"]) == [100]


# ── CSV import: main() triggers rollup rebuild ──────────────────────

def test_csv_main_triggers_rollup_rebuild_after_import(tmp_path, monkeypatch):
    """After a successful CSV import, main() should call rollup functions."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "Ticket ID,Ticket Number,Ticket Name,Ticket Product Name,"
        "Primary Customer,Severity,Group Name,Ticket Source,Ticket Type,"
        "Is Closed,Date Ticket Created,Days Opened,"
        "Days Since Ticket was Last Modified,"
        "Action Creator Name,Action Description,Action Type,"
        "Date Action Created,Action Hours Spent,Action Source\n"
        "100,T100,Test,Product,Customer,Low,Group,Web,Bug,false,"
        "2026-01-01,10,1,Alice,Hello world,Comment,2026-01-02,,Web\n"
    )

    mock_classify = MagicMock()
    mock_rollups = MagicMock()
    mock_metrics = MagicMock()
    mock_analytics = MagicMock()

    monkeypatch.setattr("sys.argv", ["run_csv_import.py", "--csv", str(csv_file)])

    with patch("db._is_enabled", return_value=True), \
         patch("db.migrate"), \
         patch("db.create_ingest_run", return_value="run-1"), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket"), \
         patch("db.upsert_action"), \
         patch("run_csv_import._load_known_inh_names", return_value=set()), \
         patch("ts_client.fetch_all_users", return_value={}), \
         patch("run_rollups.classify_actions", mock_classify), \
         patch("run_rollups.rebuild_rollups", mock_rollups), \
         patch("run_rollups.rebuild_metrics", mock_metrics), \
         patch("run_rollups.run_analytics_for_tickets", mock_analytics):

        from run_csv_import import main
        main()

    mock_classify.assert_called_once_with([100])
    mock_rollups.assert_called_once_with([100])
    mock_metrics.assert_called_once_with([100])
    mock_analytics.assert_called_once_with([100])


def test_csv_main_skips_rollup_on_dry_run(tmp_path, monkeypatch):
    """Dry-run should NOT trigger rollup rebuild."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "Ticket ID,Ticket Number,Ticket Name,Ticket Product Name,"
        "Primary Customer,Severity,Group Name,Ticket Source,Ticket Type,"
        "Is Closed,Date Ticket Created,Days Opened,"
        "Days Since Ticket was Last Modified,"
        "Action Creator Name,Action Description,Action Type,"
        "Date Action Created,Action Hours Spent,Action Source\n"
        "100,T100,Test,Product,Customer,Low,Group,Web,Bug,false,"
        "2026-01-01,10,1,Alice,Hello world,Comment,2026-01-02,,Web\n"
    )

    mock_classify = MagicMock()

    monkeypatch.setattr("sys.argv", ["run_csv_import.py", "--csv", str(csv_file), "--dry-run"])

    with patch("db._is_enabled", return_value=True), \
         patch("db.migrate"), \
         patch("run_csv_import._load_known_inh_names", return_value=set()), \
         patch("ts_client.fetch_all_users", return_value={}), \
         patch("run_rollups.classify_actions", mock_classify):

        from run_csv_import import main
        main()

    mock_classify.assert_not_called()
