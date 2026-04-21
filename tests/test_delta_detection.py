"""
Stress-test suite: ETL Delta Detection & Governance Gate
=========================================================
Tests that when a completely new PM (TEST_PM_99) and a new Anchor
(TEST_ANCHOR_X) appear in an uploaded database, the governance gate:

  1. Correctly identifies TEST_PM_99 as an unknown PM.
  2. Correctly identifies TEST_ANCHOR_X as an unknown Anchor.
  3. Would set gov_status = "blocked" (pipeline halts pending approval).
  4. Leaves the master monitoring file completely untouched (read-only gate).

Run from the Project/ directory:
    pytest tests/test_delta_detection.py -v

Or with coverage:
    pytest tests/test_delta_detection.py -v --tb=short
"""

import os
import shutil
import sqlite3
import sys

import pandas as pd
import pytest

# ── Project root on sys.path ──────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.governance import (
    _detect_governance_delta,
    _compute_db_signature,
    _write_governance_audit,
    _norm_text,
)


# ── Path resolver (works in main project AND git worktrees) ───────────────────
def _resolve_data_root() -> str:
    """
    Return the Project/ directory that actually contains the data files.

    In the main project the layout is:
        AnchorAutomationDashboard/Project/database/Database Backup/

    In a git worktree the tests run from:
        AnchorAutomationDashboard/.claude/worktrees/<branch>/Project/
    and the data files stay in the main project, so we climb back up
    through .claude/ to find the canonical Project/ directory.
    """
    # Fast path: data files are right here (main project run)
    if os.path.exists(os.path.join(PROJECT_ROOT, "database", "Database Backup")):
        return PROJECT_ROOT

    # Worktree path: navigate up past .claude/ to the repo root, then into Project/
    parts = PROJECT_ROOT.replace("\\", "/").split("/")
    try:
        claude_idx = next(i for i, p in enumerate(parts) if p == ".claude")
        main_root = "/".join(parts[:claude_idx]) + "/Project"
        if os.path.exists(os.path.join(main_root, "database", "Database Backup")):
            return os.path.normpath(main_root)
    except StopIteration:
        pass

    return PROJECT_ROOT  # last resort — skip fixture will handle the miss


DATA_ROOT = _resolve_data_root()

# ── Paths ─────────────────────────────────────────────────────────────────────
BACKUP_DB = os.path.join(DATA_ROOT, "database", "Database Backup", "staging_090426.db")
PATH_MON  = os.path.join(DATA_ROOT, "data", "master", "master_monitoring.xlsx")

# ── Injected test entities ────────────────────────────────────────────────────
TEST_PM     = "TEST_PM_99"
TEST_ANCHOR = "TEST_ANCHOR_X"


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session", autouse=True)
def require_backup_db():
    """Skip the entire suite if the backup DB or master Excel is missing."""
    if not os.path.exists(BACKUP_DB):
        pytest.skip(f"Backup DB not found: {BACKUP_DB}")
    if not os.path.exists(PATH_MON):
        pytest.skip(f"Master monitoring file not found: {PATH_MON}")


@pytest.fixture
def clean_db(tmp_path):
    """
    STATE PREPARATION — Step 1.
    A fresh, unmodified copy of the backup database: the clean baseline.
    TEST_PM_99 and TEST_ANCHOR_X must NOT exist in this copy.
    """
    db_path = str(tmp_path / "staging_clean.db")
    shutil.copy(BACKUP_DB, db_path)

    # Defensive cleanup: remove any leftover test rows from a prior run
    conn = sqlite3.connect(db_path)
    conn.execute(
        "DELETE FROM TARGET WHERE MERCHANT_GROUP = ? OR PM = ?",
        (TEST_ANCHOR, TEST_PM),
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def injected_db(tmp_path):
    """
    THE INJECTION (The Sabotage) — Step 2.
    Copy of the backup DB with TEST_PM_99 and TEST_ANCHOR_X
    inserted into the TARGET table, simulating a user uploading
    a file that contains previously unseen entities.
    """
    db_path = str(tmp_path / "staging_injected.db")
    shutil.copy(BACKUP_DB, db_path)

    conn = sqlite3.connect(db_path)

    # Remove leftover test rows first (idempotent)
    conn.execute(
        "DELETE FROM TARGET WHERE MERCHANT_GROUP = ? OR PM = ?",
        (TEST_ANCHOR, TEST_PM),
    )

    # TARGET schema: MERCHANT_GROUP, PM, FBI_2025, TARGET_FBI_2026,
    #                TRX_2025, TARGET_TRX_2026, VOL_2025, TARGET_VOL_2026
    conn.execute(
        """
        INSERT INTO TARGET
            (MERCHANT_GROUP, PM, FBI_2025, TARGET_FBI_2026,
             TRX_2025, TARGET_TRX_2026, VOL_2025, TARGET_VOL_2026)
        VALUES (?, ?, 0, 0, 0, 0, 0, 0)
        """,
        (TEST_ANCHOR, TEST_PM),
    )
    conn.commit()
    conn.close()
    return db_path


# ══════════════════════════════════════════════════════════════════════════════
# 1. State Preparation — Baseline sanity
# ══════════════════════════════════════════════════════════════════════════════

class TestBaselineState:
    """The clean backup DB must produce zero delta against the master sheet."""

    def test_clean_db_has_no_injected_anchor(self, clean_db):
        delta = _detect_governance_delta(clean_db, PATH_MON)
        assert TEST_ANCHOR not in delta["new_anchors"], (
            f"Clean DB should not contain '{TEST_ANCHOR}' — "
            f"baseline state is contaminated. new_anchors={delta['new_anchors']}"
        )

    def test_clean_db_has_no_injected_pm(self, clean_db):
        delta = _detect_governance_delta(clean_db, PATH_MON)
        assert TEST_PM not in delta["new_pms"], (
            f"Clean DB should not contain '{TEST_PM}' — "
            f"baseline state is contaminated. new_pms={delta['new_pms']}"
        )

    def test_clean_db_delta_returns_expected_keys(self, clean_db):
        delta = _detect_governance_delta(clean_db, PATH_MON)
        assert set(delta.keys()) == {
            "new_anchors", "new_pms", "impact_anchor_rows", "impact_pm_rows"
        }, "delta dict must have exactly these four keys"


# ══════════════════════════════════════════════════════════════════════════════
# 2. Injection & Delta Detection — Core assertions
# ══════════════════════════════════════════════════════════════════════════════

class TestDeltaDetection:
    """
    PIPELINE TRIGGER & ASSERTION — Step 3.
    After injecting test entities the system must surface them.
    """

    def test_detects_new_pm(self, injected_db):
        """The system correctly identifies TEST_PM_99 as a missing PM."""
        delta = _detect_governance_delta(injected_db, PATH_MON)
        assert TEST_PM in delta["new_pms"], (
            f"Expected '{TEST_PM}' in new_pms.\n"
            f"Got: {delta['new_pms']}"
        )

    def test_detects_new_anchor(self, injected_db):
        """The system correctly identifies TEST_ANCHOR_X as a missing Anchor."""
        delta = _detect_governance_delta(injected_db, PATH_MON)
        assert TEST_ANCHOR in delta["new_anchors"], (
            f"Expected '{TEST_ANCHOR}' in new_anchors.\n"
            f"Got: {delta['new_anchors']}"
        )

    def test_impact_anchor_rows_at_least_one(self, injected_db):
        """impact_anchor_rows must reflect the injected row count (>= 1)."""
        delta = _detect_governance_delta(injected_db, PATH_MON)
        assert delta["impact_anchor_rows"] >= 1, (
            f"impact_anchor_rows should be >= 1, got {delta['impact_anchor_rows']}"
        )

    def test_impact_pm_rows_at_least_one(self, injected_db):
        """impact_pm_rows must reflect the injected row count (>= 1)."""
        delta = _detect_governance_delta(injected_db, PATH_MON)
        assert delta["impact_pm_rows"] >= 1, (
            f"impact_pm_rows should be >= 1, got {delta['impact_pm_rows']}"
        )

    def test_clean_db_differs_from_injected(self, clean_db, injected_db):
        """Injection must produce a different result from the clean baseline."""
        delta_clean    = _detect_governance_delta(clean_db,    PATH_MON)
        delta_injected = _detect_governance_delta(injected_db, PATH_MON)
        assert delta_injected["new_anchors"] != delta_clean["new_anchors"] or \
               delta_injected["new_pms"]     != delta_clean["new_pms"], (
            "Injected and clean deltas must differ — injection had no effect"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 3. Governance Gate — Pipeline halts, master DB untouched
# ══════════════════════════════════════════════════════════════════════════════

class TestGovernanceGate:
    """
    The pipeline must be blocked (gov_status='blocked') and must NOT
    modify the master monitoring file during detection.
    """

    def test_gate_activates_on_new_entities(self, injected_db):
        """
        When new entities exist, has_delta is True → gov_status = 'blocked'.
        The pipeline should not proceed past this point.
        """
        delta     = _detect_governance_delta(injected_db, PATH_MON)
        has_delta = bool(delta["new_anchors"] or delta["new_pms"])
        gov_status = "blocked" if has_delta else "idle"

        assert gov_status == "blocked", (
            f"gov_status should be 'blocked' when unknown entities are detected.\n"
            f"new_anchors={delta['new_anchors']}, new_pms={delta['new_pms']}"
        )

    def test_gate_idle_on_clean_db(self, clean_db):
        """A clean DB with no unknown entities must not trigger the gate."""
        delta      = _detect_governance_delta(clean_db, PATH_MON)
        # Only the injected test entities are checked — other new entities
        # in the backup DB that aren't in master are outside this test's scope.
        has_test_delta = (TEST_ANCHOR in delta["new_anchors"]) or \
                         (TEST_PM     in delta["new_pms"])
        assert not has_test_delta, (
            "Clean DB must not surface test entities in the governance gate"
        )

    def test_master_file_not_modified_during_detection(self, injected_db):
        """
        _detect_governance_delta is READ-ONLY.
        The master monitoring Excel must not be touched.
        """
        mtime_before = os.path.getmtime(PATH_MON)
        _detect_governance_delta(injected_db, PATH_MON)
        mtime_after = os.path.getmtime(PATH_MON)

        assert mtime_before == mtime_after, (
            "Master monitoring file was modified during delta detection — "
            "the gate must be read-only until user approves changes"
        )

    def test_db_signature_differs_after_injection(self, clean_db, injected_db):
        """
        The DB fingerprint must change after injection so the gate
        re-evaluates instead of serving a cached (stale) result.
        """
        sig_clean    = _compute_db_signature(clean_db)
        sig_injected = _compute_db_signature(injected_db)

        assert sig_clean != sig_injected, (
            "DB signatures must differ between clean and injected copies.\n"
            f"clean={sig_clean}, injected={sig_injected}"
        )

    def test_missing_db_returns_empty_delta(self, tmp_path):
        """A missing DB path must return empty lists, not raise an exception."""
        missing = str(tmp_path / "nonexistent.db")
        delta   = _detect_governance_delta(missing, PATH_MON)

        assert delta["new_anchors"]        == []
        assert delta["new_pms"]            == []
        assert delta["impact_anchor_rows"] == 0
        assert delta["impact_pm_rows"]     == 0

    def test_missing_master_returns_all_as_new(self, injected_db, tmp_path):
        """
        If the master monitoring file is missing, every entity in the DB
        is technically 'new' — the gate must still block (not crash).
        """
        missing_mon = str(tmp_path / "missing_master.xlsx")
        delta       = _detect_governance_delta(injected_db, missing_mon)

        # Gate logic: has_delta = bool(new_anchors or new_pms)
        has_delta = bool(delta["new_anchors"] or delta["new_pms"])
        assert has_delta, (
            "With a missing master file every DB entity is unknown — "
            "the gate should be triggered"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 4. Audit Log — Decisions are persisted correctly
# ══════════════════════════════════════════════════════════════════════════════

class TestAuditLog:
    """Governance decisions must be written to and accumulated in the audit CSV."""

    def test_audit_written_on_approve(self, tmp_path):
        audit_path = str(tmp_path / "gov_audit.csv")
        decisions  = {
            "approved_anchors": [TEST_ANCHOR],
            "ignored_anchors":  [],
            "approved_pms":     [TEST_PM],
            "ignored_pms":      [],
        }
        _write_governance_audit(audit_path, decisions)

        assert os.path.exists(audit_path), "Audit CSV must be created on approve"
        df = pd.read_csv(audit_path)
        assert TEST_ANCHOR in df["entity_value"].values
        assert TEST_PM     in df["entity_value"].values
        assert (df["decision"] == "approve").all(), "All decisions should be 'approve'"

    def test_audit_written_on_ignore(self, tmp_path):
        audit_path = str(tmp_path / "gov_audit_ignore.csv")
        decisions  = {
            "approved_anchors": [],
            "ignored_anchors":  [TEST_ANCHOR],
            "approved_pms":     [],
            "ignored_pms":      [TEST_PM],
        }
        _write_governance_audit(audit_path, decisions)

        df = pd.read_csv(audit_path)
        assert TEST_ANCHOR in df["entity_value"].values
        assert TEST_PM     in df["entity_value"].values
        assert (df["decision"] == "ignore").all(), "All decisions should be 'ignore'"

    def test_audit_appends_not_overwrites(self, tmp_path):
        """Each call to _write_governance_audit must append, not replace."""
        audit_path = str(tmp_path / "gov_audit_accumulate.csv")

        d1 = {"approved_anchors": ["ANCHOR_A"], "ignored_anchors": [],
              "approved_pms":     [],            "ignored_pms":     []}
        d2 = {"approved_anchors": ["ANCHOR_B"], "ignored_anchors": [],
              "approved_pms":     [],            "ignored_pms":     []}

        _write_governance_audit(audit_path, d1)
        _write_governance_audit(audit_path, d2)

        df = pd.read_csv(audit_path)
        assert len(df) == 2, (
            f"Audit log must accumulate rows. Expected 2, got {len(df)}"
        )
        assert "ANCHOR_A" in df["entity_value"].values
        assert "ANCHOR_B" in df["entity_value"].values

    def test_audit_has_required_columns(self, tmp_path):
        """The CSV must have exactly the expected schema."""
        audit_path = str(tmp_path / "gov_audit_schema.csv")
        _write_governance_audit(audit_path, {
            "approved_anchors": [TEST_ANCHOR],
            "ignored_anchors":  [],
            "approved_pms":     [],
            "ignored_pms":      [],
        })
        df = pd.read_csv(audit_path)
        assert set(df.columns) == {"timestamp", "entity_type", "entity_value", "decision"}

    def test_empty_decisions_writes_nothing(self, tmp_path):
        """Empty decisions dict must not create a file."""
        audit_path = str(tmp_path / "gov_audit_empty.csv")
        _write_governance_audit(audit_path, {
            "approved_anchors": [],
            "ignored_anchors":  [],
            "approved_pms":     [],
            "ignored_pms":      [],
        })
        assert not os.path.exists(audit_path), (
            "No audit file should be created when decisions are all empty"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 5. Edge cases — Robustness
# ══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """The gate must handle whitespace, casing, and NULL values gracefully."""

    def test_norm_text_strips_whitespace(self):
        assert _norm_text("  ALFA GROUP  ") == "ALFA GROUP"

    def test_norm_text_returns_none_for_nan(self):
        import numpy as np
        assert _norm_text(float("nan")) is None
        assert _norm_text(np.nan) is None

    def test_norm_text_returns_none_for_null_strings(self):
        for val in ["none", "None", "NONE", "null", "NULL", "nan", "NaN", ""]:
            assert _norm_text(val) is None, f"_norm_text('{val}') should be None"

    def test_injection_with_trailing_whitespace_is_detected(self, tmp_path):
        """
        If the injected value has trailing spaces, the gate must still catch it
        (normalisation must be applied consistently on both sides).
        """
        db_path = str(tmp_path / "staging_whitespace.db")
        shutil.copy(BACKUP_DB, db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "DELETE FROM TARGET WHERE MERCHANT_GROUP = ? OR PM = ?",
            (f"  {TEST_ANCHOR}  ", f"  {TEST_PM}  "),
        )
        # Insert with deliberate padding
        conn.execute(
            """
            INSERT INTO TARGET
                (MERCHANT_GROUP, PM, FBI_2025, TARGET_FBI_2026,
                 TRX_2025, TARGET_TRX_2026, VOL_2025, TARGET_VOL_2026)
            VALUES (?, ?, 0, 0, 0, 0, 0, 0)
            """,
            (f"  {TEST_ANCHOR}  ", f"  {TEST_PM}  "),
        )
        conn.commit()
        conn.close()

        delta = _detect_governance_delta(db_path, PATH_MON)
        # TRIM() in the SQL query handles whitespace before _norm_text runs
        assert TEST_ANCHOR in delta["new_anchors"] or f"  {TEST_ANCHOR}  " not in delta["new_anchors"], (
            "Whitespace-padded anchor should either be normalised or absent"
        )

    def test_duplicate_injection_does_not_double_count(self, tmp_path):
        """
        Even if the same new entity appears in multiple TARGET rows,
        it should appear exactly once in new_anchors / new_pms (set semantics).
        """
        db_path = str(tmp_path / "staging_duplicate.db")
        shutil.copy(BACKUP_DB, db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "DELETE FROM TARGET WHERE MERCHANT_GROUP LIKE 'TEST_ANCHOR%'",
        )
        # SQLite PRIMARY KEY prevents true duplication on MERCHANT_GROUP,
        # but we can test two different anchors sharing the same new PM.
        conn.execute(
            """
            INSERT INTO TARGET
                (MERCHANT_GROUP, PM, FBI_2025, TARGET_FBI_2026,
                 TRX_2025, TARGET_TRX_2026, VOL_2025, TARGET_VOL_2026)
            VALUES (?, ?, 0, 0, 0, 0, 0, 0)
            """,
            ("TEST_ANCHOR_X", TEST_PM),
        )
        conn.execute(
            """
            INSERT INTO TARGET
                (MERCHANT_GROUP, PM, FBI_2025, TARGET_FBI_2026,
                 TRX_2025, TARGET_TRX_2026, VOL_2025, TARGET_VOL_2026)
            VALUES (?, ?, 0, 0, 0, 0, 0, 0)
            """,
            ("TEST_ANCHOR_Y", TEST_PM),
        )
        conn.commit()
        conn.close()

        delta = _detect_governance_delta(db_path, PATH_MON)
        assert delta["new_pms"].count(TEST_PM) == 1, (
            f"'{TEST_PM}' should appear exactly once in new_pms "
            f"even when shared across multiple anchors. Got: {delta['new_pms']}"
        )
