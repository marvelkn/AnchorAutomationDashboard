"""
Tests for utils.app_state — DB-backed triage and watchlist.

The app is Neon-only in production. These tests exercise the same SQL against
a per-test SQLAlchemy engine pointed at a temporary SQLite file — SQLite
supports the ANSI `ON CONFLICT` syntax used in app_state since v3.24, so the
DDL and upsert SQL are portable. This keeps the test suite hermetic (no live
Neon required) while still exercising the production code paths.

Run:
    pytest tests/test_app_state.py -v
"""

import os
import sys
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import app_state


@pytest.fixture
def engine(tmp_path):
    """A fresh SQLAlchemy engine on a per-test SQLite file with state tables ready."""
    db_file = tmp_path / "test_state.db"
    eng = create_engine(f"sqlite:///{db_file}", future=True)
    app_state.ensure_state_tables(eng)
    return eng


# ── triage ────────────────────────────────────────────────────────────────────


class TestTriage:
    def test_empty_when_fresh(self, engine):
        assert app_state.get_triage_states(engine).empty
        assert app_state.active_triage_map(engine) == {}

    def test_acknowledge_persists(self, engine):
        app_state.set_triage(
            "INDOMARET", app_state.TRIAGE_ACKNOWLEDGED, note="reviewed", engine=engine
        )
        m = app_state.active_triage_map(engine)
        assert m == {"INDOMARET": "acknowledged"}

    def test_snooze_active_then_expires(self, engine):
        app_state.set_triage(
            "HOKBEN", app_state.TRIAGE_SNOOZED,
            snooze_until=date(2026, 6, 1), engine=engine,
        )
        # Still active the day before expiry.
        assert "HOKBEN" in app_state.active_triage_map(engine, today=date(2026, 5, 20))
        # Resurfaces once the snooze date has passed.
        assert "HOKBEN" not in app_state.active_triage_map(engine, today=date(2026, 6, 2))

    def test_set_triage_upserts(self, engine):
        app_state.set_triage("ALFAMART", app_state.TRIAGE_ACKNOWLEDGED, engine=engine)
        app_state.set_triage(
            "ALFAMART", app_state.TRIAGE_SNOOZED,
            snooze_until=date(2026, 12, 1), engine=engine,
        )
        df = app_state.get_triage_states(engine)
        assert len(df) == 1
        assert df.iloc[0]["status"] == "snoozed"

    def test_clear_triage(self, engine):
        app_state.set_triage("GRAMEDIA", app_state.TRIAGE_ACKNOWLEDGED, engine=engine)
        app_state.clear_triage("GRAMEDIA", engine=engine)
        assert app_state.active_triage_map(engine) == {}

    def test_snooze_requires_date(self, engine):
        with pytest.raises(ValueError):
            app_state.set_triage("X", app_state.TRIAGE_SNOOZED, engine=engine)

    def test_rejects_unknown_status(self, engine):
        with pytest.raises(ValueError):
            app_state.set_triage("X", "deleted", engine=engine)


# ── watchlist ─────────────────────────────────────────────────────────────────


class TestWatchlist:
    def test_add_and_get(self, engine):
        app_state.add_to_watchlist("INDOMARET", engine=engine)
        app_state.add_to_watchlist("HOKBEN", engine=engine)
        assert set(app_state.get_watchlist(engine)) == {"INDOMARET", "HOKBEN"}

    def test_add_is_idempotent(self, engine):
        app_state.add_to_watchlist("INDOMARET", engine=engine)
        app_state.add_to_watchlist("INDOMARET", engine=engine)
        assert app_state.get_watchlist(engine) == ["INDOMARET"]

    def test_remove(self, engine):
        app_state.add_to_watchlist("INDOMARET", engine=engine)
        app_state.remove_from_watchlist("INDOMARET", engine=engine)
        assert app_state.get_watchlist(engine) == []


# ── engine guard ──────────────────────────────────────────────────────────────


class TestEngineRequired:
    def test_calling_without_engine_raises(self):
        with pytest.raises(ValueError, match="engine is required"):
            app_state.ensure_state_tables(None)
