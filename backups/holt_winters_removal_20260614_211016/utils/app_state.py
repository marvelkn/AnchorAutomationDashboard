"""
DB-backed application state for the BTN Anchor dashboard.

The processed/* tables are rebuilt wholesale by every pipeline run, so they
cannot hold user-generated state. This module owns two small side tables
that persist *between* sessions and pipeline runs:

  app_triage    - per-merchant triage decisions for the Action Center
                  (a merchant the user acknowledged or snoozed)
  app_watchlist - merchants the user pinned for focused tracking

Both are persisted in **Neon (PostgreSQL)**. The local SQLite fallback
was removed because the app is now strictly Neon-only — see plan
`act-as-a-senior-glistening-lovelace.md`.

Every public function requires a SQLAlchemy `engine`. The DDL and upsert SQL
use ANSI `ON CONFLICT` which works equally well against in-memory SQLite if
tests want to exercise this module without spinning up Neon.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

log = logging.getLogger(__name__)

TRIAGE_TABLE = "app_triage"
WATCHLIST_TABLE = "app_watchlist"

# Valid triage statuses. 'open' is the implicit default (no row).
TRIAGE_ACKNOWLEDGED = "acknowledged"
TRIAGE_SNOOZED = "snoozed"
_VALID_STATUSES = frozenset({TRIAGE_ACKNOWLEDGED, TRIAGE_SNOOZED})

_DDL = {
    TRIAGE_TABLE: (
        f"CREATE TABLE IF NOT EXISTS {TRIAGE_TABLE} ("
        " merchant_group TEXT PRIMARY KEY,"
        " status TEXT NOT NULL,"
        " snooze_until TEXT,"
        " note TEXT,"
        " updated_at TEXT NOT NULL)"
    ),
    WATCHLIST_TABLE: (
        f"CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE} ("
        " merchant_group TEXT PRIMARY KEY,"
        " added_at TEXT NOT NULL)"
    ),
}


def _require_engine(engine):
    if engine is None:
        raise ValueError(
            "engine is required — this module is Neon-only. "
            "Build one with utils.cloud_db.build_engine()."
        )


def _exec(engine, sql: str, params: dict | None = None) -> None:
    _require_engine(engine)
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def _query(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    _require_engine(engine)
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def ensure_state_tables(engine) -> None:
    """Create the two state tables if they do not yet exist.

    Safe to call on every page load — CREATE TABLE IF NOT EXISTS is a no-op
    once the tables are present.
    """
    _require_engine(engine)
    with engine.begin() as conn:
        for ddl in _DDL.values():
            conn.execute(text(ddl))


# ── triage ────────────────────────────────────────────────────────────────────


def get_triage_states(engine) -> pd.DataFrame:
    """Return all triage rows as a DataFrame.

    Columns: merchant_group, status, snooze_until, note, updated_at.
    Returns an empty (correctly-typed) frame when the table has no rows.
    """
    cols = ["merchant_group", "status", "snooze_until", "note", "updated_at"]
    sql = (
        "SELECT merchant_group, status, snooze_until, note, updated_at "
        f"FROM {TRIAGE_TABLE}"
    )
    try:
        df = _query(engine, sql)
    except Exception:
        return pd.DataFrame(columns=cols)
    df.columns = [c.lower() for c in df.columns]
    return df


def active_triage_map(engine, today: date = None) -> dict:
    """Map merchant_group -> status for triage decisions that are *still active*.

    An 'acknowledged' decision is permanent until cleared. A 'snoozed' decision
    expires once snooze_until passes, at which point the merchant should
    resurface in the Action Center. Expired snoozes are excluded here.
    """
    today = today or date.today()
    df = get_triage_states(engine)
    if df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        status = str(r["status"])
        if status == TRIAGE_SNOOZED:
            until = str(r.get("snooze_until") or "")
            if until and until < today.isoformat():
                continue  # snooze has expired — merchant resurfaces
        out[str(r["merchant_group"])] = status
    return out


def set_triage(
    merchant_group: str,
    status: str,
    *,
    snooze_until: date = None,
    note: str = "",
    engine=None,
) -> None:
    """Upsert a triage decision for one merchant.

    status must be 'acknowledged' or 'snoozed'. snooze_until is required for
    'snoozed' and ignored otherwise.
    """
    if status not in _VALID_STATUSES:
        raise ValueError(f"status must be one of {sorted(_VALID_STATUSES)}")
    if status == TRIAGE_SNOOZED and snooze_until is None:
        raise ValueError("snooze_until is required when status is 'snoozed'")

    until_str = snooze_until.isoformat() if snooze_until else None
    now_str = datetime.now().isoformat(timespec="seconds")

    sql = (
        f"INSERT INTO {TRIAGE_TABLE} "
        "(merchant_group, status, snooze_until, note, updated_at) "
        "VALUES (:m, :s, :u, :n, :t) "
        "ON CONFLICT (merchant_group) DO UPDATE SET "
        "status = EXCLUDED.status, snooze_until = EXCLUDED.snooze_until, "
        "note = EXCLUDED.note, updated_at = EXCLUDED.updated_at"
    )
    _exec(engine, sql, {
        "m": merchant_group, "s": status, "u": until_str,
        "n": note, "t": now_str,
    })


def clear_triage(merchant_group: str, engine=None) -> None:
    """Remove a triage decision — the merchant returns to the open queue."""
    _exec(
        engine,
        f"DELETE FROM {TRIAGE_TABLE} WHERE merchant_group = :m",
        {"m": merchant_group},
    )


# ── watchlist ─────────────────────────────────────────────────────────────────


def get_watchlist(engine) -> list:
    """Return the pinned merchant names, oldest pin first."""
    sql = f"SELECT merchant_group FROM {WATCHLIST_TABLE} ORDER BY added_at"
    try:
        df = _query(engine, sql)
    except Exception:
        return []
    return df.iloc[:, 0].astype(str).tolist() if not df.empty else []


def add_to_watchlist(merchant_group: str, engine=None) -> None:
    """Pin a merchant. Re-pinning an already-pinned merchant is a no-op."""
    now_str = datetime.now().isoformat(timespec="seconds")
    _exec(
        engine,
        f"INSERT INTO {WATCHLIST_TABLE} (merchant_group, added_at) "
        "VALUES (:m, :t) ON CONFLICT (merchant_group) DO NOTHING",
        {"m": merchant_group, "t": now_str},
    )


def remove_from_watchlist(merchant_group: str, engine=None) -> None:
    """Unpin a merchant."""
    _exec(
        engine,
        f"DELETE FROM {WATCHLIST_TABLE} WHERE merchant_group = :m",
        {"m": merchant_group},
    )


# ── ML run audit log ──────────────────────────────────────────────────────────

_CREATE_ML_RUN_LOG = """
CREATE TABLE IF NOT EXISTS ml_run_log (
    id                SERIAL PRIMARY KEY,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_version      TEXT,
    n_merchants       INTEGER,
    chosen_k          INTEGER,
    k_elbow           INTEGER,
    k_silhouette      INTEGER,
    silhouette_score  DOUBLE PRECISION,
    davies_bouldin    DOUBLE PRECISION,
    business_anchored BOOLEAN,
    justification     TEXT
)
"""


def ensure_ml_run_log(engine) -> None:
    """Create ml_run_log table if it does not exist. Idempotent."""
    _require_engine(engine)
    with engine.begin() as conn:
        conn.execute(text(_CREATE_ML_RUN_LOG))


def write_ml_run(engine, *, data_version: str, n_merchants: int, k_diag: dict) -> None:
    """Append one row to ml_run_log recording the K-selection decision. Non-fatal."""
    try:
        ensure_ml_run_log(engine)
        ks   = k_diag.get("k_values", [])
        sils = k_diag.get("silhouette", [])
        dbis = k_diag.get("davies_bouldin", [])
        ck   = k_diag.get("chosen_k", 0)
        sil_for_chosen = sils[ks.index(ck)] if ck in ks and sils else None
        dbi_for_chosen = dbis[ks.index(ck)] if ck in ks and dbis else None
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO ml_run_log
                        (data_version, n_merchants, chosen_k, k_elbow, k_silhouette,
                         silhouette_score, davies_bouldin, business_anchored, justification)
                    VALUES
                        (:dv, :nm, :ck, :ke, :ks, :sil, :dbi, :ba, :just)
                """),
                {
                    "dv":   data_version,
                    "nm":   n_merchants,
                    "ck":   ck,
                    "ke":   k_diag.get("k_elbow"),
                    "ks":   k_diag.get("k_silhouette"),
                    "sil":  sil_for_chosen,
                    "dbi":  dbi_for_chosen,
                    "ba":   k_diag.get("business_anchored", True),
                    "just": k_diag.get("justification", ""),
                },
            )
    except Exception:
        log.warning("write_ml_run failed; audit record not written", exc_info=True)
