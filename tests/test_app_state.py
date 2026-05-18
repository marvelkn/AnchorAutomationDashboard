"""
Tests for utils.app_state — DB-backed triage, forecast log, and watchlist.

Exercises SQLite mode only (Neon mode requires a live DATABASE_URL). PATH_DB is
redirected to a per-test temp file so no real database/staging.db is touched.

Run:
    pytest tests/test_app_state.py -v
"""

import os
import sys
from datetime import date

import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import app_state


@pytest.fixture
def state_db(tmp_path, monkeypatch):
    """Redirect app_state at a fresh temp SQLite DB with the state tables created."""
    monkeypatch.delenv("DATABASE_URL", raising=False)  # force SQLite mode
    db_file = tmp_path / "test_staging.db"
    monkeypatch.setattr(app_state, "PATH_DB", str(db_file))
    app_state.ensure_state_tables()
    return str(db_file)


# ── triage ────────────────────────────────────────────────────────────────────


class TestTriage:
    def test_empty_when_fresh(self, state_db):
        assert app_state.get_triage_states().empty
        assert app_state.active_triage_map() == {}

    def test_acknowledge_persists(self, state_db):
        app_state.set_triage("INDOMARET", app_state.TRIAGE_ACKNOWLEDGED, note="reviewed")
        m = app_state.active_triage_map()
        assert m == {"INDOMARET": "acknowledged"}

    def test_snooze_active_then_expires(self, state_db):
        app_state.set_triage(
            "HOKBEN", app_state.TRIAGE_SNOOZED, snooze_until=date(2026, 6, 1)
        )
        # Still active the day before expiry.
        assert "HOKBEN" in app_state.active_triage_map(today=date(2026, 5, 20))
        # Resurfaces once the snooze date has passed.
        assert "HOKBEN" not in app_state.active_triage_map(today=date(2026, 6, 2))

    def test_set_triage_upserts(self, state_db):
        app_state.set_triage("ALFAMART", app_state.TRIAGE_ACKNOWLEDGED)
        app_state.set_triage(
            "ALFAMART", app_state.TRIAGE_SNOOZED, snooze_until=date(2026, 12, 1)
        )
        df = app_state.get_triage_states()
        assert len(df) == 1
        assert df.iloc[0]["status"] == "snoozed"

    def test_clear_triage(self, state_db):
        app_state.set_triage("GRAMEDIA", app_state.TRIAGE_ACKNOWLEDGED)
        app_state.clear_triage("GRAMEDIA")
        assert app_state.active_triage_map() == {}

    def test_snooze_requires_date(self, state_db):
        with pytest.raises(ValueError):
            app_state.set_triage("X", app_state.TRIAGE_SNOOZED)

    def test_rejects_unknown_status(self, state_db):
        with pytest.raises(ValueError):
            app_state.set_triage("X", "deleted")


# ── forecast log ──────────────────────────────────────────────────────────────


class TestForecastLog:
    def test_log_and_read_back(self, state_db):
        n = app_state.log_forecast(
            "INDOMARET",
            forecast_months=[202607, 202608],
            point=[1000.0, 1100.0],
            lower=[900.0, 950.0],
            upper=[1100.0, 1250.0],
            method="Holt-Winters (Trend)",
            run_date=date(2026, 5, 18),
        )
        assert n == 2
        log = app_state.get_forecast_log("INDOMARET")
        assert len(log) == 2
        assert set(log["forecast_month"]) == {202607, 202608}

    def test_relog_same_run_is_idempotent(self, state_db):
        for _ in range(2):
            app_state.log_forecast(
                "HOKBEN", [202607], [500.0], [400.0], [600.0], "m",
                run_date=date(2026, 5, 18),
            )
        assert len(app_state.get_forecast_log("HOKBEN")) == 1

    def test_score_accuracy(self, state_db):
        app_state.log_forecast(
            "INDOMARET", [202607], [1000.0], [800.0], [1200.0], "m",
            run_date=date(2026, 5, 1),
        )
        actuals = pd.DataFrame({
            "MERCHANT_GROUP": ["INDOMARET"],
            "TRX_MONTH": [202607],
            "TOTAL_SV": [1100.0],
        })
        scored = app_state.score_forecast_accuracy(actuals)
        assert len(scored) == 1
        row = scored.iloc[0]
        assert row["within_band"]                       # 1100 in [800, 1200]
        assert abs(row["abs_pct_error"] - (100 / 1100 * 100)) < 1e-6

    def test_score_accuracy_empty_when_no_match(self, state_db):
        app_state.log_forecast(
            "INDOMARET", [202607], [1000.0], [800.0], [1200.0], "m",
        )
        actuals = pd.DataFrame({
            "MERCHANT_GROUP": ["ALFAMART"], "TRX_MONTH": [202607], "TOTAL_SV": [1.0],
        })
        assert app_state.score_forecast_accuracy(actuals).empty


# ── watchlist ─────────────────────────────────────────────────────────────────


class TestWatchlist:
    def test_add_and_get(self, state_db):
        app_state.add_to_watchlist("INDOMARET")
        app_state.add_to_watchlist("HOKBEN")
        assert set(app_state.get_watchlist()) == {"INDOMARET", "HOKBEN"}

    def test_add_is_idempotent(self, state_db):
        app_state.add_to_watchlist("INDOMARET")
        app_state.add_to_watchlist("INDOMARET")
        assert app_state.get_watchlist() == ["INDOMARET"]

    def test_remove(self, state_db):
        app_state.add_to_watchlist("INDOMARET")
        app_state.remove_from_watchlist("INDOMARET")
        assert app_state.get_watchlist() == []
