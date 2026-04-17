"""
Data Service — cloud-aware data loading layer.

All Pandas/SQL logic that used to live inline in pages/4_Dashboard.py lives here.
Callbacks import these functions; they stay free of any Dash/Streamlit imports.
"""
import os
import sqlite3
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_DB = os.path.join(_BASE, "database", "staging.db")


def _get_connection():
    """Return (engine_or_conn, is_neon) for whichever DB backend is active."""
    neon_url = os.getenv("DATABASE_URL")
    if neon_url:
        from utils.cloud_db import build_engine
        return build_engine(), True
    return sqlite3.connect(PATH_DB), False


def _table_exists(conn_or_engine, name: str) -> bool:
    """Return True if the given table exists in the active database."""
    if hasattr(conn_or_engine, "connect"):          # SQLAlchemy engine (Neon)
        from sqlalchemy import text
        q = text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :t)"
        )
        with conn_or_engine.connect() as c:
            return bool(c.execute(q, {"t": name.lower()}).scalar())
    else:                                            # sqlite3 connection
        result = pd.read_sql_query(
            f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}'",
            conn_or_engine,
        )
        return result.iloc[0, 0] == 1


def _norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase all column names (Postgres returns lowercase by default)."""
    df.columns = [c.upper() for c in df.columns]
    return df


def _read(conn_or_engine, table: str, is_neon: bool) -> pd.DataFrame:
    """Read a table if it exists, else return empty DataFrame."""
    tbl = table.lower() if is_neon else table.upper()
    if not _table_exists(conn_or_engine, table):
        return pd.DataFrame()
    df = pd.read_sql_query(f"SELECT * FROM {tbl}", conn_or_engine)
    return _norm_columns(df)


# ── Public loaders ─────────────────────────────────────────────────────────────

def load_all() -> dict:
    """
    Load every analytics table in one call.
    Returns a dict with keys:
        card_share, card_history, card_monthly,
        monitoring, monitoring_weekly, target, metadata
    """
    conn, is_neon = _get_connection()
    try:
        data = {
            "card_share":        _read(conn, "PROCESSED_CARD_SHARE",      is_neon),
            "card_history":      _read(conn, "PROCESSED_CARD_HISTORY",     is_neon),
            "card_monthly":      _read(conn, "PROCESSED_CARD_MONTHLY",     is_neon),
            "monitoring":        _read(conn, "PROCESSED_MONITORING",       is_neon),
            "monitoring_weekly": _read(conn, "PROCESSED_MONITORING_WEEKLY", is_neon),
            "target":            _read(conn, "TARGET",                     is_neon),
            "metadata":          _read(conn, "APP_METADATA",               is_neon),
        }
    finally:
        if not is_neon:
            conn.close()
    return data


def load_card_share() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_CARD_SHARE", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_card_history() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_CARD_HISTORY", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_card_monthly() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_CARD_MONTHLY", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_monitoring() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_MONITORING", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_monitoring_weekly() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_MONITORING_WEEKLY", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_target() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "TARGET", is_neon)
    finally:
        if not is_neon:
            conn.close()


def load_metadata() -> dict:
    """Return APP_METADATA table as a plain Python dict {KEY: VALUE}."""
    conn, is_neon = _get_connection()
    try:
        df = _read(conn, "APP_METADATA", is_neon)
    finally:
        if not is_neon:
            conn.close()
    if df.empty or "KEY" not in df.columns:
        return {}
    return dict(zip(df["KEY"], df["VALUE"]))


def load_mid() -> pd.DataFrame:
    conn, is_neon = _get_connection()
    try:
        return _read(conn, "PROCESSED_MID", is_neon)
    finally:
        if not is_neon:
            conn.close()


def db_exists() -> bool:
    """True if a usable data source (SQLite or Neon) is available."""
    if os.getenv("DATABASE_URL"):
        return True
    return os.path.exists(PATH_DB)


def db_status() -> dict:
    """
    Return a status dict consumed by the sidebar DB badge.
    Keys: last_update (str), size_kb (int|None), signal (bool)
    """
    meta = load_metadata()
    last_update = meta.get("LAST_DATA_UPDATE", "Unknown")
    signal = meta.get("NEW_DATA_SIGNAL") == "1"
    size_kb = None
    if os.path.exists(PATH_DB):
        size_kb = round(os.path.getsize(PATH_DB) / 1024)
    return {"last_update": last_update, "signal": signal, "size_kb": size_kb}
