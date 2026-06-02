"""
Tests for utils.cloud_db — input-validation and the SQL-identifier guard.

The upsert itself uses Postgres-only SQL (CREATE TABLE ... LIKE INCLUDING,
ON CONFLICT), so it is not hermetically runnable against SQLite. These tests
cover the validation paths that run *before* any database call, including the
identifier allowlist added to defend against malicious upload column names.

Run:
    pytest tests/test_cloud_db.py -v
"""

import os
import sys

import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.cloud_db import _safe_ident, upsert_dataframe


class TestSafeIdent:
    @pytest.mark.parametrize("name", ["id", "merchant_group", "_tmp_target", "A1", "x_2_y"])
    def test_accepts_valid_identifiers(self, name):
        assert _safe_ident(name) == name

    @pytest.mark.parametrize("name", [
        'a"; DROP TABLE target; --',
        "has space",
        "has-dash",
        "1leading_digit",
        "semi;colon",
        "",
        None,
    ])
    def test_rejects_unsafe_identifiers(self, name):
        with pytest.raises(ValueError):
            _safe_ident(name)


class TestUpsertValidation:
    # engine is never reached: every case below raises before any DB call.
    def test_empty_dataframe_raises(self):
        with pytest.raises(ValueError, match="empty"):
            upsert_dataframe(None, pd.DataFrame(), "target", ["id"])

    def test_unsafe_column_name_raises(self):
        df = pd.DataFrame({"id": [1], 'bad"col': [2]})
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            upsert_dataframe(None, df, "target", ["id"])

    def test_unsafe_table_name_raises(self):
        df = pd.DataFrame({"id": [1], "v": [2]})
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            upsert_dataframe(None, df, "bad-table", ["id"])

    def test_missing_conflict_columns_raises(self):
        df = pd.DataFrame({"id": [1], "v": [2]})
        with pytest.raises(ValueError):
            upsert_dataframe(None, df, "target", [])

    def test_conflict_column_not_in_frame_raises(self):
        df = pd.DataFrame({"id": [1], "v": [2]})
        with pytest.raises(ValueError, match="Missing conflict column"):
            upsert_dataframe(None, df, "target", ["nope"])
