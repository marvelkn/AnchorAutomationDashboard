import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import logging
import os
import pickle
from datetime import datetime, date, timedelta
import sys

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)
from utils.theme import (
    apply_theme, page_header, section_label, section_header, styled_divider,
    kpi_card, kpi_row, tab_desc, tab_label_with_badge, filter_pill,
    portfolio_filter_bar,
    status_card, apply_plotly_theme, get_palette, stale_data_banner,
    left_accent_card, status_chip_html, status_box, hex_to_rgba,
    NAVY, GOLD, GOLD_DIM, BG, SURFACE, BORDER, TEXT_PRI, TEXT_SEC,
    GREEN, RED, AMBER, BLUE_ACC,
    CLUSTER_COLORS, PAYMENT_COLORS,
    SUCCESS, WARNING, DANGER, INFO, PM_PALETTE,
)
from utils.cloud_db import build_engine
from utils.formatting import (
    fmt_count, fmt_currency_idr, fmt_growth, fmt_pct, fmt_zscore,
    growth_cell_style, zscore_cell_style,
)
from utils.growth_analytics import (
    BASELINE_FLOORS, compose_urgency_score, compute_growth_signals,
    extract_recent_weeks,
)
from utils.ml_engine import (
    run_ml as _run_ml_pure, hw_forecast as _hw_forecast, N_CLUSTERS, Z_THRESH,
    prepare_cluster_features as _prepare_cluster_features,
    select_optimal_k as _select_optimal_k,
)
from utils import app_state
from sqlalchemy import text

log = logging.getLogger(__name__)

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon=os.path.join(_BASE, "static", "btn_logo.png"),
    layout="wide",
)
apply_theme()

# ── Strict Neon gate ──────────────────────────────────────────────────────────
# The local SQLite fallback was removed (see plan act-as-a-senior-glistening-lovelace.md).
# The dashboard is meaningful only when Neon is reachable; the snapshot tier still
# kicks in if a live Neon read fails mid-session.
if not bool(os.getenv("DATABASE_URL")):
    st.error(
        "**Cloud database not configured.** Set the `DATABASE_URL` environment "
        "variable to your Neon connection string and restart the app."
    )
    st.stop()

from utils.rate_limiter import enforce_rate_limit
enforce_rate_limit("dashboard_page", max_calls=60, window_seconds=60, label="dashboard loads")


@st.dialog("No Data Found", width="large")
def _show_no_data_dialog():
    st.markdown(
        """
        <div style="text-align:center;padding:16px 0 8px;">
            <div style="font-size:var(--fs-kpi);margin-bottom:12px;"></div>
            <div style="font-size:var(--fs-lg);font-weight:var(--fw-bold);margin-bottom:8px;">
                The dashboard has no data to display.
            </div>
            <div style="font-size:var(--fs-base);color:var(--btn-text-sec);line-height:1.6;max-width:min(480px,100%);margin:0 auto 24px;">
                The database is empty or was recently reset. Run the automated pipeline
                to ingest Card Share &amp; Monitoring data before opening the dashboard.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Go to Automated Pipeline", use_container_width=True, type="primary"):
            st.switch_page("pages/00_Automated_Pipeline.py")
    with c2:
        if st.button("Go to Global Settings", use_container_width=True):
            st.switch_page("pages/0_Master_Configuration.py")

def _p():
    """Get current palette dict for theme-aware chart colours."""
    return get_palette()

def _chart_base():
    """Return common Plotly layout kwargs for the active palette."""
    p = _p()
    return dict(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=p['TEXT_PRI'], family='Roboto, sans-serif'),
    )

def _xaxis():
    p = _p()
    return dict(showgrid=False, color=p['TEXT_SEC'])

def _yaxis():
    p = _p()
    return dict(showgrid=True, gridcolor=p['BORDER'], color=p['TEXT_SEC'])

_MONTH_ABBR = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
               'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

def _month_label(yyyymm):
    """Convert an integer YYYYMM (e.g. 202405) to a short axis label ("MAY '24")."""
    v = int(yyyymm)
    y, m = v // 100, v % 100
    if not 1 <= m <= 12:
        return str(v)
    return f"{_MONTH_ABBR[m - 1]} '{y % 100:02d}"

def _next_months(last_yyyymm, n):
    """Return the n calendar months (YYYYMM ints) following last_yyyymm."""
    out = []
    y, m = int(last_yyyymm) // 100, int(last_yyyymm) % 100
    for _ in range(n):
        m += 1
        if m > 12:
            m, y = 1, y + 1
        out.append(y * 100 + m)
    return out

# ── PATHS ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_CARD   = os.path.join(BASE_DIR, "data", "master", "master_card_share.xlsx")
PATH_MON    = os.path.join(BASE_DIR, "data", "master", "master_monitoring.xlsx")
PATH_RAW_MON = os.path.join(BASE_DIR, "data", "raw", "real", "Monitoring Weekly Anchor 2026.xlsx")


def table_exists(engine, name, schema="public"):
    """True iff `name` exists in the given schema on Neon."""
    from sqlalchemy import text
    q = text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :s AND table_name = :t)")
    with engine.connect() as conn:
        return bool(conn.execute(q, {"s": schema, "t": name.lower()}).scalar())


def _existing_tables(engine, names, schema="public"):
    """Resolve existence of many tables in ONE query (vs one connection each).

    Returns {original_name: bool}. Used by the cold-load path so it does a single
    information_schema round-trip instead of one checkout + ping per table.
    """
    q = text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :s AND table_name = ANY(:names)"
    )
    want = [n.lower() for n in names]
    with engine.connect() as conn:
        present = {r[0] for r in conn.execute(q, {"s": schema, "names": want})}
    return {n: (n.lower() in present) for n in names}


@st.cache_resource
def _get_engine():
    """One pooled SQLAlchemy engine per app session (avoids per-rerun pool churn)."""
    return build_engine()


# ── DB TRANSFER CONTROL — version probe + local snapshot fallback ─────────────
# The processed tables change only when the pipeline runs. The heavy loaders are
# keyed on a cheap LAST_DATA_UPDATE probe so a full table pull happens only when
# the data genuinely changed — not on a fixed timer. A pickled snapshot of the
# last good load keeps the dashboard online (read-only) when Neon is unreachable.
_SNAPSHOT_DIR  = os.path.join(BASE_DIR, "data", "snapshot")
_SNAPSHOT_FILE = os.path.join(_SNAPSHOT_DIR, "dashboard_snapshot.pkl")


@st.cache_data(ttl=60, show_spinner=False)
def _get_data_version() -> str:
    """Cheap probe of app_metadata.LAST_DATA_UPDATE — used as the cache key for
    the heavy loaders so a full table pull happens only when data changed."""
    try:
        df = pd.read_sql_query(
            "SELECT value FROM app_metadata WHERE key = 'LAST_DATA_UPDATE'",
            _get_engine(),
        )
        return str(df["value"].iloc[0]) if not df.empty else "none"
    except Exception:
        return "unknown"


def _pickle_write(path: str, obj) -> None:
    """Best-effort pickle dump. A snapshot is a safety net — a write failure must
    never block the live load, so it is logged at debug level only."""
    try:
        os.makedirs(_SNAPSHOT_DIR, exist_ok=True)
        with open(path, "wb") as fh:
            pickle.dump(obj, fh)
    except Exception:
        log.debug("snapshot write failed: %s", path, exc_info=True)


def _pickle_read(path: str):
    """Best-effort pickle load; returns None on any failure (missing/corrupt)."""
    try:
        with open(path, "rb") as fh:
            return pickle.load(fh)
    except Exception:
        log.debug("snapshot read failed: %s", path, exc_info=True)
        return None


_MONTHLY_SNAPSHOT_FILE = os.path.join(_SNAPSHOT_DIR, "monthly_snapshot.pkl")
_WM_ANOMALY_SNAPSHOT_FILE = os.path.join(_SNAPSHOT_DIR, "wm_anomaly_snapshot.pkl")


def _write_snapshot(result: tuple) -> None:
    """Persist the last successful dashboard load, stamped with the load time."""
    _pickle_write(_SNAPSHOT_FILE, {
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "result": result,
    })


def _read_snapshot():
    """Return (result_tuple, as_of_str) from the local snapshot, or (None, None)."""
    payload = _pickle_read(_SNAPSHOT_FILE)
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"], payload.get("as_of")
    return None, None


def _write_monthly_snapshot(df) -> None:
    """Best-effort: persist the last good PROCESSED_CARD_MONTHLY pull to disk."""
    _pickle_write(_MONTHLY_SNAPSHOT_FILE, df)


def _read_monthly_snapshot():
    """Return the monthly DataFrame from the local snapshot, or None."""
    return _pickle_read(_MONTHLY_SNAPSHOT_FILE)


def _write_wm_anomaly_snapshot(df) -> None:
    """Best-effort: persist the last good WEEKLY_MONITOR (year=2026) pull."""
    _pickle_write(_WM_ANOMALY_SNAPSHOT_FILE, df)


def _read_wm_anomaly_snapshot():
    """Return the cached WEEKLY_MONITOR DataFrame, or None."""
    return _pickle_read(_WM_ANOMALY_SNAPSHOT_FILE)


@st.cache_data(ttl=86400)
def _load_monthly_raw(data_version: str):
    """Cached load of the full PROCESSED_CARD_MONTHLY table.

    Keyed on data_version so it refetches only when the pipeline writes new
    data; the 24h ttl is just a safety net. On a Neon failure it falls back to
    a local snapshot, or an empty frame, so the monthly view degrades gracefully.
    """
    try:
        df = pd.read_sql_query("SELECT * FROM processed_card_monthly", _get_engine())
        df.columns = [c.upper() for c in df.columns]
        _write_monthly_snapshot(df)
        return df
    except Exception:
        snap = _read_monthly_snapshot()
        return snap if snap is not None else pd.DataFrame()

# ── MACHINE LEARNING ENGINE ──────────────────────────────────────────────────────
# run_ml + hw_forecast now live in utils/ml_engine.py (pure, unit-tested). The
# cached wrapper below preserves the per-session @st.cache_data behaviour and the
# existing call sites; N_CLUSTERS / Z_THRESH / _hw_forecast are imported above.
@st.cache_data
def run_ml(df_c, df_m, df_t=None):
    return _run_ml_pure(df_c, df_m, df_t)


@st.cache_data(show_spinner=False)
def _k_diagnostics(df_c, df_m, df_t=None):
    """Elbow + Silhouette K-selection sweep for the Merchant Tiers diagnostics panel.

    Recomputed here rather than read from run_ml's df.attrs because @st.cache_data
    does not preserve DataFrame.attrs across its boundary. Operates on the exact same
    standardized feature space K-Means clusters on (via prepare_cluster_features).
    """
    _df, X_s = _prepare_cluster_features(df_c, df_m, df_t)
    if X_s is None or len(X_s) < 3:
        return None
    return _select_optimal_k(X_s, k_min=2, k_max=5, business_k=N_CLUSTERS)


# ── DB LOAD ──────────────────────────────────────────────────────────────────
# Neon is required (gated at the top of the file); the engine is always live.
engine = _get_engine()

# Plan F1/F2/F5 — ensure the user-state side tables (triage, forecast log,
# watchlist) exist. Wrapped defensively: a state-table failure must never
# block the dashboard from rendering its core analytics.
try:
    app_state.ensure_state_tables(engine=engine)
except Exception:
    log.warning("ensure_state_tables failed; user-state features (triage/forecast log/watchlist) may be unavailable", exc_info=True)


@st.cache_data(ttl=86400, show_spinner="Loading dashboard data...")
def _load_dashboard_data(data_version: str):
    """Cached load of the 5 core dashboard tables + table-existence flags.

    Keyed on data_version: a full pull happens only when the pipeline writes
    new data, not on a fixed timer — the primary control on Neon transfer.
    On a Neon failure (e.g. transfer quota exceeded) the last good load is
    served from a local pickle snapshot so the dashboard stays online read-only.

    Returns the 11 data values plus a load_meta dict describing the source.
    """
    try:
        eng = _get_engine()
        # One existence query for all six tables instead of one connection each.
        _flags = _existing_tables(eng, [
            "PROCESSED_CARD_SHARE", "PROCESSED_CARD_HISTORY", "PROCESSED_MONITORING",
            "PROCESSED_MONITORING_WEEKLY", "TARGET", "PROCESSED_CARD_MONTHLY",
        ])
        has_card        = _flags["PROCESSED_CARD_SHARE"]
        has_card_hist   = _flags["PROCESSED_CARD_HISTORY"]
        has_mon         = _flags["PROCESSED_MONITORING"]
        has_mon_weekly  = _flags["PROCESSED_MONITORING_WEEKLY"]
        has_tgt         = _flags["TARGET"]
        has_monthly_tbl = _flags["PROCESSED_CARD_MONTHLY"]

        # One shared connection for all reads instead of a checkout per query.
        with eng.connect() as conn:
            df_card        = pd.read_sql_query("SELECT * FROM processed_card_share", conn) if has_card else pd.DataFrame()
            df_card_hist   = pd.read_sql_query("SELECT * FROM processed_card_history", conn) if has_card_hist else pd.DataFrame()
            df_mon         = pd.read_sql_query("SELECT * FROM processed_monitoring", conn) if has_mon else pd.DataFrame()
            df_mon_weekly  = pd.read_sql_query("SELECT * FROM processed_monitoring_weekly", conn) if has_mon_weekly else pd.DataFrame()
            df_target      = pd.read_sql_query("SELECT * FROM target", conn) if has_tgt else pd.DataFrame()

        # Column normalization for Postgres (ensure uppercase for dashboard consistency)
        for df in [df_card, df_card_hist, df_mon, df_mon_weekly, df_target]:
            if len(df.columns) > 0:
                df.columns = [c.upper() for c in df.columns]

        result = (df_card, df_card_hist, df_mon, df_mon_weekly, df_target,
                  has_card, has_card_hist, has_mon, has_mon_weekly, has_tgt, has_monthly_tbl)
        _write_snapshot(result)
        return result + ({"source": "neon", "as_of": None},)
    except Exception as neon_err:
        # Neon unreachable — serve the read-only pickle snapshot (last good load).
        snapshot, as_of = _read_snapshot()
        if snapshot is None:
            raise neon_err
        return snapshot + ({"source": "snapshot", "as_of": as_of},)


(df_card, df_card_hist, df_mon, df_mon_weekly, df_target,
 has_card, has_card_hist, has_mon, has_mon_weekly, has_tgt, has_monthly_tbl,
 _load_meta) = _load_dashboard_data(_get_data_version())

# Show popup if Neon is connected but tables are empty (e.g. after a database reset)
if df_card.empty and df_mon.empty:
    _show_no_data_dialog()
    st.stop()

# Source-aware banners — differentiate between read-only snapshot and live local DB.
_meta_source = _load_meta.get("source")
if _meta_source == "snapshot":
    st.warning(
        "Live database unavailable — showing the last saved snapshot from "
        f"{_load_meta.get('as_of') or 'an earlier session'}. "
        "Figures are read-only. Run a fresh ingest from the Automated Pipeline page to refresh."
    )

# ── BATCH METADATA & SIGNALS ─────────────────────────────────────────────────
# Intentionally uncached: tiny payload, and carries the live NEW_DATA_SIGNAL badge.
_last_update = "Unknown"
_show_new_badge = False
try:
    _df_meta = pd.read_sql_query("SELECT * FROM app_metadata", engine)
    _df_meta.columns = [c.upper() for c in _df_meta.columns]
    _meta_dict = dict(zip(_df_meta['KEY'], _df_meta['VALUE']))
    _last_update = _meta_dict.get('LAST_DATA_UPDATE', 'Unknown')
    _show_new_badge = _meta_dict.get('NEW_DATA_SIGNAL') == '1'
except Exception:
    pass  # metadata is non-critical; dashboard continues with defaults

# ── HEADER ───────────────────────────────────────────────────────────────────
# Freshness state, derived once, used by both the persistent chip and the
# one-time toast. The chip is purely passive (pointer-events: none in CSS);
# the toast acknowledges the *event* of a fresh ingestion and auto-dismisses.
def _freshness_state(last_update_str: str) -> tuple[str, str, str]:
    """Return (variant, relative_label, absolute_label) for the freshness chip."""
    if not last_update_str or last_update_str == "Unknown":
        return ("unknown", "Freshness unknown", "Last pipeline run: unknown")
    ts = None
    try:
        ts = datetime.fromisoformat(last_update_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            ts = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ("unknown", "Freshness unknown", f"Last pipeline run: {last_update_str}")
    now = datetime.now(ts.tzinfo) if ts.tzinfo else datetime.now()
    delta = now - ts
    hours = delta.total_seconds() / 3600
    days = delta.days
    if hours < 1:
        rel = "Updated just now"
    elif hours < 24:
        rel = f"Updated {int(hours)}h ago"
    else:
        rel = f"Updated {days}d ago"
    variant = "fresh" if hours < 24 else ("recent" if days <= 7 else "stale")
    abs_label = f"Last pipeline run: {ts.strftime('%d %b %Y %H:%M')} WIB"
    return (variant, rel, abs_label)

_variant, _rel_label, _abs_label = _freshness_state(_last_update)

st.markdown(
    f"""
    <div class="dashboard-header-row">
      <div>
        <div class="dashboard-page-eyebrow">Merchant Analytics</div>
        <h3 class="dashboard-page-title">Merchant Decision Intelligence</h3>
      </div>
      <span class="fresh-chip fresh-chip--{_variant}"
            role="status" aria-live="polite"
            title="{_abs_label}">
        <span class="fresh-chip__dot" aria-hidden="true"></span>
        {_rel_label}
      </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# One-time toast on the first view after a fresh ingestion. The persistent
# chip is the steady-state indicator; this just acknowledges the *event* and
# consumes NEW_DATA_SIGNAL so the toast doesn't replay on later sessions.
if _show_new_badge and not st.session_state.get("_seen_new_data_signal"):
    st.toast(f"New data ingested · {_last_update}")
    st.session_state["_seen_new_data_signal"] = True
    try:
        with engine.begin() as _conn_m:
            _conn_m.execute(text(
                "UPDATE app_metadata SET value = '0' WHERE key = 'NEW_DATA_SIGNAL'"
            ))
    except Exception:
        pass  # non-critical — the chip remains informative without the flag

# ── Stale Data Banner ─────────────────────────────────────────────────────────
# Uses Neon's app_metadata.LAST_DATA_UPDATE timestamp; suppressed on snapshot
# tier (the snapshot banner above already tells the user analytics are stale).
if _meta_source == "neon":
    stale_data_banner(last_update=_last_update, threshold_hours=24)

# ── Global KPI Strip (full-portfolio totals, always unfiltered) ───────────────
_total_merchants = df_card['MERCHANT_GROUP'].nunique()          if not df_card.empty and 'MERCHANT_GROUP' in df_card.columns else 0
_ytd_sv          = df_card['TOTAL_SV'].sum()                    if not df_card.empty and 'TOTAL_SV'        in df_card.columns else 0
_ytd_trx         = df_card['TOTAL_TRX'].sum()                   if not df_card.empty and 'TOTAL_TRX'       in df_card.columns else 0
_avg_onus        = df_card['RASIO_ONUS'].mean()                 if not df_card.empty and 'RASIO_ONUS'      in df_card.columns else 0

_ml_kpi = run_ml(df_card, df_mon, df_target) if (has_card and has_mon) else pd.DataFrame()
_high_risk_count = int(_ml_kpi['CHURN_RISK'].str.contains('HIGH', na=False).sum()) if not _ml_kpi.empty and 'CHURN_RISK' in _ml_kpi.columns else 0

_sv_fmt  = f"Rp {_ytd_sv/1e9:,.1f} M"  if _ytd_sv >= 1e9 else f"Rp {_ytd_sv/1e6:,.0f} Jt"
_trx_fmt = f"{_ytd_trx/1e6:,.2f} M"    if _ytd_trx >= 1e6 else f"{_ytd_trx:,.0f}"

# Plan U3 — portfolio-wide monthly trend for the hero KPI sparklines + the
# month-over-month delta. PROCESSED_CARD_HISTORY carries the monthly series;
# YTD totals alone can't show direction, so we derive it here.
def _mom_delta(values):
    """Latest-vs-prior percent change for a monthly series, or None."""
    s = [float(v) for v in values if pd.notna(v)]
    if len(s) < 2 or s[-2] == 0:
        return None
    return (s[-1] - s[-2]) / s[-2] * 100.0

_spark_sv, _spark_trx = None, None
_delta_sv, _delta_trx = None, None
if not df_card_hist.empty and 'TRX_MONTH' in df_card_hist.columns:
    _port_monthly = (
        df_card_hist.groupby('TRX_MONTH', as_index=False)
        .agg(sv=('TOTAL_SV', 'sum'), trx=('TOTAL_TRX', 'sum'))
        .sort_values('TRX_MONTH')
    )
    if len(_port_monthly) >= 2:
        _spark_sv  = _port_monthly['sv'].tail(8).tolist()
        _spark_trx = _port_monthly['trx'].tail(8).tolist()
        _delta_sv  = _mom_delta(_port_monthly['sv'].tolist())
        _delta_trx = _mom_delta(_port_monthly['trx'].tolist())

kpi_row([
    # Page-level strip — hero=True so this row reads as the page's headline
    # (largest type, elevated shadow). Per-tab boxes use the standard size,
    # giving the user a clear two-tier visual hierarchy on every screen.
    kpi_card(f"{_total_merchants:,}", "Merchants Tracked", hero=True),
    kpi_card(_sv_fmt,                 "YTD Sales Volume", hero=True,
             delta=_delta_sv, spark=_spark_sv),
    kpi_card(_trx_fmt,                "YTD Transactions", hero=True,
             delta=_delta_trx, spark=_spark_trx),
    kpi_card(f"{_avg_onus*100:.1f}%", "Avg On-Us Ratio", hero=True),
    kpi_card(
        str(_high_risk_count),
        "High Risk Merchants",
        kind="danger" if _high_risk_count > 0 else "success",
        hero=True,
    ),
])

# Portfolio filter widgets used to live here as a "global" strip above the
# tabs, but only Card Share (tab1) and Weekly Monitor (tab2) ever honored
# them — the macro tabs (Overview / Tiers / Health / Anomaly) ignored the
# selection. The widgets now render inside each filter-aware tab via
# portfolio_filter_bar(), and the scoped df_*_filt frames are computed
# inside those tab bodies. Shared state lives in st.session_state under
# the keys pf_group / pf_brand, so a user's selection on Card Share
# persists when they switch to Weekly Monitor.

CLAMP = CLUSTER_COLORS

# ── TABS ──────────────────────────────────────────────────────────────────────
# Tab order per user directive:
# Overview → Card Share → Weekly → Merchant Tiers → Health → Anomaly
#
# Variable bindings stay aligned with each tab's content block:
# tab0 = Overview, tab1 = Card Share, tab2 = Weekly,
# tab3 = Merchant Tiers, tab4 = Health, tab5 = Anomaly
#
# Plan §4.1 — Health label keeps a numeric badge so daily users see at a
# glance how many merchants need attention.
tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Overview",
    "Card Share",
    "Weekly Monitor",
    "Merchant Tiers",
    tab_label_with_badge("Health Alerts", _high_risk_count),
    "Anomaly Detection",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 0 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab0:
    # ── High-risk banner ─────────────────────────────────────────────────────
    if _high_risk_count > 0:
        st.warning(f"**{_high_risk_count} merchant(s) need immediate attention.** See the **Health Alerts** tab for recommended actions.")

    # ──────────────────────────────────────────────────────────────────────────
    # Plan §1.3 — Daily Briefing
    # Adds a high-density briefing at the top of Overview: a horizontal AM-
    # coverage bar chart on the left (every PM, ranked by avg achievement,
    # high-risk count called out as a red badge) and an "Insight of the Day"
    # auto-selected merchant card on the right (highest urgency, what's wrong,
    # and a CTA to the Health Alerts tab). Detail sections below remain for
    # deeper drill-down.
    # ──────────────────────────────────────────────────────────────────────────
    if not _ml_kpi.empty and 'PM' in _ml_kpi.columns:
        styled_divider()
        section_label("Daily Briefing")
        _bf_left, _bf_right = st.columns([3, 2], gap="large")

        with _bf_left:
            # Per-PM aggregates: count, avg achievement, high-risk count.
            _pm_agg = (
                _ml_kpi.assign(
                    _is_high=_ml_kpi['CHURN_RISK'].astype(str).str.contains('HIGH', na=False)
                )
                .groupby('PM', dropna=False)
                .agg(
                    merchants=('MERCHANT_GROUP', 'count'),
                    avg_ach=('ACHIEVEMENT_PCT', 'mean'),
                    high_risk=('_is_high', 'sum'),
                )
                .reset_index()
            )
            _pm_agg = _pm_agg[
                _pm_agg['PM'].notna() & (_pm_agg['PM'].astype(str).str.upper() != 'UNASSIGNED')
            ].copy()
            _pm_agg['avg_ach'] = _pm_agg['avg_ach'].fillna(0).clip(lower=0)
            # Order by achievement descending so the most concerning PMs sit
            # at the bottom of the chart (where the eye lands last).
            _pm_agg = _pm_agg.sort_values('avg_ach', ascending=True)

            if _pm_agg.empty:
                st.caption("No PM coverage data this period.")
            else:
                # Bar color scales by achievement: red below 60, amber 60-100, green ≥100.
                def _ach_color(v):
                    if v >= 100: return SUCCESS
                    if v >= 60:  return WARNING
                    return DANGER
                _pm_colors = [_ach_color(v) for v in _pm_agg['avg_ach']]

                _bar_text = [
                    f"{v:.0f}%" + (f"  ⚠ {int(h)} high-risk" if h > 0 else "")
                    for v, h in zip(_pm_agg['avg_ach'], _pm_agg['high_risk'])
                ]
                _fig_pm = go.Figure(go.Bar(
                    x=_pm_agg['avg_ach'],
                    y=_pm_agg['PM'],
                    orientation='h',
                    marker_color=_pm_colors,
                    text=_bar_text,
                    textposition='outside',
                    cliponaxis=False,
                    customdata=_pm_agg[['merchants', 'high_risk']].values,
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Avg achievement: %{x:.1f}%<br>"
                        "Merchants: %{customdata[0]}<br>"
                        "High-risk: %{customdata[1]}<extra></extra>"
                    ),
                ))
                # Reference line at the 100% target.
                _fig_pm.add_vline(x=100, line_dash='dash', line_color=TEXT_SEC, opacity=0.5)
                _fig_pm.update_layout(
                    height=max(60 + 40 * len(_pm_agg), 220),
                    margin=dict(l=4, r=120, t=10, b=32),
                    xaxis={**_xaxis(), 'title': 'Avg Achievement %', 'range': [0, max(140, float(_pm_agg['avg_ach'].max()) * 1.15)]},
                    yaxis=dict(showgrid=False, automargin=True),
                    **_chart_base(),
                )
                st.plotly_chart(_fig_pm, use_container_width=True, theme=None)
                st.caption("Bars colored by achievement: green ≥ 100%, amber 60-99%, red < 60%. Hover for merchant counts.")

        with _bf_right:
            # Auto-pick the single most actionable merchant for today. Use the
            # composite urgency score from §4.2 so this stays consistent with
            # the Action Inbox ordering on the Health Alerts tab.
            _focus_pool = _ml_kpi.copy()
            if not _focus_pool.empty and 'RISK_SCORE' in _focus_pool.columns:
                _focus_pool = _focus_pool.copy()
                _focus_pool['_URGENCY'] = compose_urgency_score(
                    _focus_pool['RISK_SCORE'].astype(float),
                    achievement_pct=_focus_pool.get('ACHIEVEMENT_PCT', pd.Series(dtype=float)),
                    is_iforest_anomaly=_focus_pool.get('IF_IS_ANOMALY', pd.Series(dtype=bool)),
                ).values
                _focus = _focus_pool.sort_values('_URGENCY', ascending=False).head(1)
            else:
                _focus = pd.DataFrame()

            if _focus.empty:
                st.markdown(
                    f"<div style='padding:18px;border-radius:12px;background:{SUCCESS}14;"
                    f"border:1px solid {SUCCESS}55;'>"
                    f"<div style='font-weight:var(--fw-bold);color:{SUCCESS};'>No urgent alerts</div>"
                    f"<div style='margin-top:6px;color:var(--btn-text-sec);font-size:var(--fs-sm);'>"
                    f"All merchants are within normal operating bands today.</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            else:
                _f = _focus.iloc[0]
                _f_merchant = _f.get('MERCHANT_GROUP', '—')
                _f_pm       = _f.get('PM', 'N/A')
                _f_cluster  = _f.get('CLUSTER', '—')
                _f_ach      = float(_f.get('ACHIEVEMENT_PCT', 0) or 0)
                _f_risk     = float(_f.get('RISK_SCORE', 0) or 0)
                _f_growth   = float(_f.get('SV_GROWTH_RATE', 0) or 0)
                _f_color    = DANGER if _f_risk >= 60 else WARNING

                st.markdown(
                    f"<div style='padding:18px;border-radius:12px;"
                    f"background:linear-gradient(135deg, {_f_color}1F, {_f_color}10);"
                    f"border:1px solid {_f_color}66;'>"
                    f"<div style='display:flex;align-items:center;gap:8px;'>"
                    f"<span style='display:inline-block;width:8px;height:8px;border-radius:50%;background:{_f_color};'></span>"
                    f"<span style='font-size:var(--fs-xs);color:{_f_color};font-weight:var(--fw-bold);"
                    f"text-transform:uppercase;letter-spacing:1px;'>Insight of the Day</span>"
                    f"</div>"
                    f"<div style='margin-top:8px;font-weight:var(--fw-bold);font-size:var(--fs-lg);'>{_f_merchant}</div>"
                    f"<div style='color:var(--btn-text-sec);font-size:var(--fs-sm);margin-top:2px;'>"
                    f"PM · {_f_pm}  ·  Tier · {_f_cluster}"
                    f"</div>"
                    f"<div style='margin-top:14px;display:grid;grid-template-columns:1fr 1fr;gap:10px;'>"
                    f"<div><div style='color:var(--btn-text-sec);font-size:var(--fs-2xs);text-transform:uppercase;letter-spacing:0.5px;'>Risk Score</div>"
                    f"<div style='font-weight:var(--fw-bold);font-size:var(--fs-md);color:{_f_color};'>{_f_risk:.0f} / 100</div></div>"
                    f"<div><div style='color:var(--btn-text-sec);font-size:var(--fs-2xs);text-transform:uppercase;letter-spacing:0.5px;'>Achievement</div>"
                    f"<div style='font-weight:var(--fw-bold);font-size:var(--fs-md);color:{DANGER if _f_ach < 60 else (WARNING if _f_ach < 100 else SUCCESS)};'>{fmt_pct(_f_ach, decimals=0, scale=False)}</div></div>"
                    f"</div>"
                    f"<div style='margin-top:14px;color:var(--btn-text-pri);font-size:var(--fs-sm);line-height:1.5;'>"
                    f"Growth trend: <b>{fmt_growth(_f_growth, decimals=1, scale=True)}</b> MoM. "
                    f"This merchant tops today's urgency ranking. Open the <b>Health Alerts</b> tab for "
                    f"the full Action Inbox and a one-click PM Manager handoff."
                    f"</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # ── PM Coverage Cards (visual, not table) ─────────────────────────────────
    if not df_target.empty and 'PM' in df_target.columns:
        styled_divider()
        section_label("Account Manager Coverage")
        _unassigned = int((df_target['PM'].fillna('UNASSIGNED').str.upper() == 'UNASSIGNED').sum())
        _assigned   = len(df_target) - _unassigned
        _active_pms = int(df_target['PM'].dropna()[
            df_target['PM'].dropna().str.upper() != 'UNASSIGNED'
        ].nunique())
        _avg_per_pm = round(_assigned / max(_active_pms, 1), 1)

        # ── Row 1: Individual PM cards ────────────────────────────────────────
        if not _ml_kpi.empty and 'PM' in _ml_kpi.columns:
            _pm_list = sorted(df_target['PM'].dropna().unique().tolist())
            _pm_list = [pm for pm in _pm_list if pm.upper() != 'UNASSIGNED']
            _am_cards_html = '<div style="display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;">'
            for i, _pm in enumerate(_pm_list[:6]):
                _pm_merch = _ml_kpi[_ml_kpi['PM'] == _pm]
                _pm_count = len(_pm_merch)
                _pm_high  = int(_pm_merch['CHURN_RISK'].str.contains('HIGH', na=False).sum())
                _pm_ach   = float(_pm_merch['ACHIEVEMENT_PCT'].mean()) if 'ACHIEVEMENT_PCT' in _pm_merch.columns else 0
                _chip = (
                    status_chip_html(f"{_pm_high} High Risk", "danger")
                    if _pm_high > 0 else
                    status_chip_html("All on track", "ok")
                )
                _am_cards_html += left_accent_card(
                    icon="", name=_pm,
                    count=_pm_count, sub_label="merchants",
                    bar_label="Achievement", bar_value=_pm_ach,
                    accent=PM_PALETTE[i % len(PM_PALETTE)],
                    chip_html=_chip,
                )
            _am_cards_html += '</div>'
            st.markdown(_am_cards_html, unsafe_allow_html=True)

        # ── Row 2: Aggregate stat strip ───────────────────────────────────────
        _unasgn_color = '#FBBF24' if _unassigned > 0 else '#34D399'
        _unasgn_bg    = '#FBBF2414' if _unassigned > 0 else '#34D39914'
        _unasgn_sub   = f'+{_unassigned} need assignment' if _unassigned > 0 else 'fully assigned'
        st.markdown(
            f"""<div class="agg-strip">
              <div class="agg-strip-item">
                <div class="kpi-label">Active Account Managers</div>
                <div class="kpi-value">{_active_pms}</div>
                <div class="kpi-meta">PMs managing portfolio</div>
              </div>
              <div class="agg-strip-item">
                <div class="kpi-label">Avg Merchant Load</div>
                <div class="kpi-value">{_avg_per_pm}</div>
                <div class="kpi-meta">merchants per AM</div>
              </div>
              <div class="agg-strip-item" style="background:{_unasgn_bg};">
                <div class="kpi-label" style="color:{_unasgn_color};">Unassigned Merchants</div>
                <div class="kpi-value" style="color:{_unasgn_color};">{_unassigned}</div>
                <div class="kpi-meta" style="color:{_unasgn_color};font-weight:var(--fw-semibold);">{_unasgn_sub}</div>
              </div>
            </div>""",
            unsafe_allow_html=True
        )
    else:
        st.info("No assignment data loaded. Run the pipeline to populate PM assignments.")

    styled_divider()

    # ── AI Insights — elevated to main page level (no longer inside an expander) ──
    section_label("AI Insights & Recommendations")
    with st.container():
        if not has_mon_weekly:
            st.warning("AI Insights require processed Monitoring Weekly data in the database.")
        else:
            df_ai_wk = df_mon_weekly[df_mon_weekly['YEAR'] == '2026'].copy()
            W_COLS = sorted([c for c in df_ai_wk.columns if c.startswith('W') and c[1:].isdigit()])
            if df_ai_wk.empty:
                st.info("No 2026 monitoring data found for the current filter.")
            else:
                # Derive current week number (used by Deep Dive risk score calculation)
                latest_wk_num = 0
                for _w in reversed(W_COLS):
                    if df_ai_wk[_w].fillna(0).sum() > 0:
                        latest_wk_num = int(_w[1:])
                        break

                # --- Deep Dive & Projection ---
                section_label("Deep Dive & Projection (Specific Merchant)")
                all_merch_ai = sorted(df_ai_wk['MERCHANT_GROUP'].unique().tolist())
                # Determine the page default so a relevant merchant is profiled on
                # load without user input. Priority:
                #   1. Explicit portfolio filter the user set elsewhere (pf_group).
                #      sel_group only exists inside the Card Share / Weekly tabs;
                #      the Overview tab reads this shared filter from session_state.
                #   2. Current month's top merchant by sales volume (auto-select).
                #   3. Alphabetically first merchant (index 0) as a last resort.
                _pf_group = st.session_state.get("pf_group", "ALL GROUPS")
                _ai_default_idx = 0
                if _pf_group != "ALL GROUPS" and _pf_group in all_merch_ai:
                    _ai_default_idx = all_merch_ai.index(_pf_group)
                elif not df_card_hist.empty and 'TRX_MONTH' in df_card_hist.columns:
                    # Top merchant of the most recent month by total sales volume.
                    # Reuses the same max_month / TOTAL_SV basis as the MoM growth
                    # table below. Guarded: df_card_hist (monthly) and df_ai_wk
                    # (weekly monitoring) are distinct tables whose merchant naming
                    # can differ, so we only apply the result if it is a valid option.
                    _cur_month = df_card_hist['TRX_MONTH'].max()
                    _month_vol = (
                        df_card_hist[df_card_hist['TRX_MONTH'] == _cur_month]
                        .groupby('MERCHANT_GROUP')['TOTAL_SV'].sum()
                    )
                    if not _month_vol.empty:
                        _top_merch = _month_vol.idxmax()
                        if _top_merch in all_merch_ai:
                            _ai_default_idx = all_merch_ai.index(_top_merch)
                sel_merch = st.selectbox("Select Merchant Entity to Profile:", all_merch_ai, index=_ai_default_idx, key="ai_sel_merch")
                if sel_merch:
                    col_txt, col_graph = st.columns([1, 1], gap="large")
                    df_m_wk  = df_ai_wk[df_ai_wk['MERCHANT_GROUP'] == sel_merch]
                    df_m_vol = df_m_wk[df_m_wk['DIMENSI'] == 'VOL']
                    ytd_actual = float(df_m_vol['YTD'].iloc[0]) if not df_m_vol.empty else 0
                    target_row = df_target[df_target['MERCHANT_GROUP'] == sel_merch]
                    fy_target  = float(target_row['TARGET_VOL_2026'].iloc[0]) if not target_row.empty else 0
                    active_weeks_count = int((df_m_vol[W_COLS].iloc[0] > 0).sum()) if not df_m_vol.empty else 0
                    merch_hist = df_card_hist[df_card_hist['MERCHANT_GROUP'] == sel_merch].copy()
                    # ── Holt-Winters Forecast ─────────────────────────────────
                    # Attempt statistical forecast. Falls back to linear if
                    # insufficient data (<6 months) or if model fitting fails.
                    # Use calendar month to determine remaining months in the year —
                    # len(merch_hist) is unreliable because PROCESSED_CARD_HISTORY
                    # contains multi-year data, causing _remaining_months to floor at 0.
                    _remaining_months = max(0, 12 - datetime.now().month)
                    # Always forecast at least 6 months so the chart is visible even
                    # when all 12 months of current-year data are present.
                    _forecast_periods = _remaining_months if _remaining_months > 0 else 6
                    _hw_result = _hw_forecast(
                        merch_hist[['TRX_MONTH', 'TOTAL_SV']] if not merch_hist.empty else None,
                        periods_ahead=_forecast_periods
                    )
                    if _hw_result['success']:
                        proj_eoy          = ytd_actual + (_hw_result['projected_eoy'] if _remaining_months > 0 else 0)
                        _proj_method      = _hw_result['method']
                        _hw_forecast_vals = _hw_result['forecast']
                        _hw_lower         = _hw_result['lower']
                        _hw_upper         = _hw_result['upper']
                        _hw_reason        = None
                    else:
                        proj_eoy          = (ytd_actual / active_weeks_count * 52) if active_weeks_count > 0 else 0
                        _proj_method      = 'Estimated Run Rate'
                        _hw_forecast_vals = None
                        _hw_lower         = None
                        _hw_upper         = None
                        _hw_reason        = _hw_result['reason']
                    seasonality_str = "No historical seasonality data found."
                    season_df  = pd.DataFrame()
                    if not merch_hist.empty:
                        merch_hist['MonthIdx'] = (merch_hist['TRX_MONTH'] % 100).astype(int)
                        mo_avg = merch_hist.groupby('MonthIdx')['TOTAL_SV'].mean().reset_index()
                        all_mo_avg = mo_avg['TOTAL_SV'].mean()
                        if all_mo_avg > 0:
                            mo_avg['Multiplier'] = mo_avg['TOTAL_SV'] / all_mo_avg
                            season_df = mo_avg.copy()
                            peak_mo   = season_df.loc[season_df['Multiplier'].idxmax()]
                            peak_name = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][int(peak_mo['MonthIdx'])-1]
                            seasonality_str = f"Historically shows a **{peak_mo['Multiplier']:.1f}x surge in {peak_name}** vs baseline."
                    with col_txt:
                        section_label(f"AI Insight Summary: {sel_merch}")
                        rate_pct   = (proj_eoy / fy_target * 100) if fy_target > 0 else 0
                        if fy_target == 0:
                            status_str = f"No FY Target is registered for {sel_merch}."
                        elif proj_eoy >= fy_target:
                            over_by    = proj_eoy - fy_target
                            status_str = f"Based on a strictly linear trajectory, they are trending to **comfortably exceed** their Target by **{over_by/1e9:,.1f} Billion Rp**, wrapping the year with an estimated `+{rate_pct-100:.0f}%` surplus!"
                        elif proj_eoy >= fy_target * 0.8:
                            fall_short = fy_target - proj_eoy
                            status_str = f"They are trending slightly underneath their Target, projected to **miss it by roughly {fall_short/1e9:,.1f} Billion Rp** (`{rate_pct:.0f}%` achievement). If they experience a seasonal bump, they can still catch up."
                        else:
                            fall_short = fy_target - proj_eoy
                            status_str = f"They are drastically underperforming mathematically. At their current velocity of {proj_eoy/52/1e6:,.1f} Jt per week, they will completely **fail their FY Target by {fall_short/1e9:,.1f} Billion Rp** (projected `{rate_pct:.0f}%` final achievement). **Intervention is required.**"
                        _pp6 = _p()
                        exec_color = "#34D399" if rate_pct >= 100 else ("#FBBF24" if rate_pct >= 80 else "#F87171")
                        exec_dot   = (f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
                                      f'background:{exec_color};margin-right:6px;vertical-align:middle;"></span>')
                        exec_label = "ON TRACK" if rate_pct >= 100 else ("AT RISK" if rate_pct >= 80 else "CRITICAL — INTERVENTION REQUIRED")
                        st.markdown(
                            f"""<div style="border-left:5px solid {exec_color};background:{exec_color}18;
                                border-radius:0 12px 12px 0;padding:16px 20px;margin-bottom:14px;">
                                <div style="font-size:var(--fs-2xs);font-weight:var(--fw-bold);text-transform:uppercase;
                                            letter-spacing:.08em;color:{exec_color};">{exec_dot}STATUS: {exec_label}</div>
                                <div style="font-size:var(--fs-sm);margin-top:8px;color:{_pp6['TEXT_PRI']};line-height:1.65;">
                                    <b>{sel_merch}</b> has accumulated <code>Rp {ytd_actual/1e9:,.2f}B</code> YTD across
                                    <b>{active_weeks_count}</b> active weeks.<br>{status_str}
                                </div>
                            </div>""", unsafe_allow_html=True
                        )
                        if seasonality_str != "No historical seasonality data found.":
                            st.markdown(
                                f"""<div style="background:{_pp6['SURFACE2']};border:1px solid {_pp6['BORDER']};
                                    border-radius:10px;padding:12px 16px;font-size:var(--fs-sm);
                                    color:{_pp6['TEXT_PRI']};margin-bottom:14px;">
                                    <b>Seasonality Intelligence:</b> {seasonality_str}
                                </div>""", unsafe_allow_html=True
                            )
                        _proj_kind = "success" if rate_pct >= 100 else ("accent" if rate_pct >= 80 else "danger")
                        st.markdown(
                            kpi_card(
                                f"Rp {proj_eoy/1e9:,.2f} B",
                                f"Projected Year-End Run Rate ({_proj_method}) — {rate_pct:.1f}% of Target",
                                kind=_proj_kind,
                            ),
                            unsafe_allow_html=True,
                        )
                        with st.expander("What's Driving This Merchant's Risk?", expanded=True):
                            fi_scores = {}
                            if active_weeks_count > 0 and latest_wk_num > 0:
                                inactivity_ratio = 1.0 - (active_weeks_count / latest_wk_num)
                                fi_scores["Inactivity Rate"]     = round(min(inactivity_ratio * 100, 100), 1)
                                fi_scores["Target Gap"]          = round(max(0, 100 - rate_pct) / 2, 1)
                                fi_scores["Low Weekly Velocity"] = round(max(0, 100 - min((ytd_actual / active_weeks_count) / 1e7, 100)), 1)
                            if not merch_hist.empty and 'TOTAL_SV' in merch_hist.columns and len(merch_hist) >= 6:
                                recent = merch_hist.sort_values('TRX_MONTH').tail(3)['TOTAL_SV'].mean()
                                older  = merch_hist.sort_values('TRX_MONTH').head(3)['TOTAL_SV'].mean()
                                if older > 0:
                                    fi_scores["Declining Volume Trend"] = round(min(max(0, (1 - recent / older) * 100), 100), 1)
                            for k, v in {"Inactivity Rate": 15.0, "Target Gap": 30.0, "Low Weekly Velocity": 20.0, "Declining Volume Trend": 25.0}.items():
                                fi_scores.setdefault(k, v)
                            fi_df = pd.DataFrame(list(fi_scores.items()), columns=["Factor", "Impact Score"]).sort_values("Impact Score", ascending=True)
                            bar_colors = ["#F87171" if s >= 50 else ("#FBBF24" if s >= 25 else "#34D399") for s in fi_df["Impact Score"]]
                            fig_fi = go.Figure(go.Bar(
                                x=fi_df["Impact Score"], y=fi_df["Factor"], orientation="h",
                                marker_color=bar_colors, marker_line_width=0,
                                text=[f"{v:.1f}" for v in fi_df["Impact Score"]], textposition="outside",
                                hovertemplate="<b>%{y}</b><br>Impact: <b>%{x:.1f}</b><extra></extra>",
                            ))
                            fig_fi.update_layout(
                                title="Risk Factor Contribution (Domain Heuristic)", height=240, margin=dict(l=0, r=50, t=36, b=32),
                                xaxis=dict(title="Risk Impact Score (0–100)", range=[0, max(fi_df["Impact Score"].max() * 1.25, 10)], showgrid=False, tickfont=dict(color=_pp6["TEXT_SEC"])),
                                yaxis=dict(showgrid=False, tickfont=dict(color=_pp6["TEXT_PRI"])),
                                **_chart_base(),
                            )
                            st.plotly_chart(fig_fi, use_container_width=True, theme=None)
                            st.caption("Each bar shows how much a specific business factor is contributing to this merchant's overall risk level. Longer bar = greater urgency to address that factor.")

                        # ── Isolation Forest Feature Contribution (Model-Based) ──
                        # Only shown when this merchant is flagged by Isolation Forest.
                        # Method: Leave-One-Feature-Out (LOFO) — each feature is
                        # individually neutralized to the portfolio mean (scaled 0),
                        # the model is re-scored (no re-fit), and the delta is measured.
                        # Positive delta = feature drives the anomaly.
                        _lofo_col_map = {
                            'IF_CONTRIB_AVG_SV':      'Avg Settlement Volume',
                            'IF_CONTRIB_AVG_FBI':      'Avg Fee-Based Income',
                            'IF_CONTRIB_RASIO_ONUS':   'On-Us Ratio',
                            'IF_CONTRIB_SV_GROWTH':    'Volume Growth Rate',
                            'IF_CONTRIB_ACHIEVEMENT':  'Target Achievement %',
                            'IF_CONTRIB_WEEKS_ACTIVE': 'Activity Weeks'
                        }
                        _merch_ml = _ml_kpi[_ml_kpi['MERCHANT_GROUP'] == sel_merch] if not _ml_kpi.empty else pd.DataFrame()
                        _is_if_anomaly = (
                            not _merch_ml.empty and
                            'IF_IS_ANOMALY' in _merch_ml.columns and
                            bool(_merch_ml['IF_IS_ANOMALY'].iloc[0])
                        )
                        if _is_if_anomaly and all(c in _merch_ml.columns for c in _lofo_col_map):
                            with st.expander("Isolation Forest Feature Contribution (Model-Based)", expanded=True):
                                _lofo_vals = {
                                    label: float(_merch_ml[col].iloc[0])
                                    for col, label in _lofo_col_map.items()
                                }
                                _lofo_df = pd.DataFrame(
                                    list(_lofo_vals.items()),
                                    columns=['Feature', 'Contribution']
                                ).sort_values('Contribution', ascending=True)
                                _lofo_colors = [
                                    '#F87171' if v > 0 else '#34D399'
                                    for v in _lofo_df['Contribution']
                                ]
                                fig_lofo = go.Figure(go.Bar(
                                    x=_lofo_df['Contribution'],
                                    y=_lofo_df['Feature'],
                                    orientation='h',
                                    marker_color=_lofo_colors,
                                    marker_line_width=0,
                                    text=[f"{v:+.4f}" for v in _lofo_df['Contribution']],
                                    textposition='outside',
                                    hovertemplate='<b>%{y}</b><br>LOFO Delta: <b>%{x:+.4f}</b><extra></extra>',
                                ))
                                fig_lofo.update_layout(
                                    title='IF Feature Contribution (LOFO Method)',
                                    height=260,
                                    margin=dict(l=0, r=80, t=36, b=32),
                                    xaxis=dict(title='Anomaly Score Delta', showgrid=False, tickfont=dict(color=_pp6['TEXT_SEC'])),
                                    yaxis=dict(showgrid=False, tickfont=dict(color=_pp6['TEXT_PRI'])),
                                    **_chart_base(),
                                )
                                st.plotly_chart(fig_lofo, use_container_width=True, theme=None)
                                st.caption(
                                    "Red = feature drives the anomaly (neutralizing it lowers the score). "
                                    "Green = feature reduces anomaly risk. "
                                    "LOFO: each feature is set to the portfolio mean and the Isolation Forest score delta is measured — no model re-fitting."
                                )
                    with col_graph:
                        if not season_df.empty:
                            mo_names    = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                            full_season = pd.DataFrame({'MonthIdx': range(1, 13)})
                            full_season = pd.merge(full_season, season_df, on='MonthIdx', how='left').fillna({'Multiplier': 1.0, 'SV': 0})
                            full_season['MonthName'] = mo_names
                            fig_sea = px.line(full_season, x='MonthName', y='Multiplier', markers=True,
                                              title=f"Historical Seasonality Curve ({sel_merch})",
                                              labels={'Multiplier': 'Volume Multiplier (1.0 = Average)'})
                            fig_sea.add_hline(y=1.0, line_dash="dash", line_color=get_palette()['TEXT_SEC'], annotation_text="Baseline Avg (1.0x)")
                            fig_sea.update_traces(line=dict(width=3, color=get_palette()['GOLD']), marker=dict(size=8))
                            fig_sea.update_layout(height=300, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                            st.plotly_chart(fig_sea, use_container_width=True, theme=None)
                        else:
                            st.info(f"Insufficient historical Realisasi monthly data to chart statistical seasonality for {sel_merch}.")

                    styled_divider()
                    section_label("Volume Outlook — Forecast & Target Tracking")
                    st.caption(f"Forecasting **{sel_merch}** · Model: **{_proj_method}**")
                    _fc_kind = "success" if rate_pct >= 100 else ("accent" if rate_pct >= 80 else "danger")
                    _mcol1, _mcol2, _mcol3, _mcol4 = st.columns(4)
                    with _mcol1:
                        st.markdown(kpi_card(f"Rp {proj_eoy/1e9:,.2f} B", "Projected Year-End", kind=_fc_kind), unsafe_allow_html=True)
                    with _mcol2:
                        st.markdown(kpi_card(f"{rate_pct:.0f}%", "of FY 2026 Target", kind=_fc_kind), unsafe_allow_html=True)
                    with _mcol3:
                        st.markdown(kpi_card(f"Rp {ytd_actual/1e9:,.2f} B", "YTD Actual"), unsafe_allow_html=True)
                    with _mcol4:
                        st.markdown(kpi_card(f"{active_weeks_count}", "Active Weeks"), unsafe_allow_html=True)
                    # ── Volume Outlook — Forecast & Target Tracking ───────
                    if _hw_forecast_vals is not None and len(_hw_forecast_vals) > 0:
                        _pal        = _p()
                        _C_ACTUAL   = _pal['GOLD']
                        _C_FORECAST = '#1B59F8'   # BTN primary blue
                        _band_rgba  = hex_to_rgba(_C_FORECAST, 0.13)

                        # Gap-free history straight from the model (already
                        # calendar-contiguous). Values plotted in Rp Billion.
                        _full_m = list(_hw_result['hist_months'])
                        _full_v = [float(v) / 1e9 for v in _hw_result['hist_values']]
                        hist_m, hist_v = _full_m[-14:], _full_v[-14:]   # last 14 mo for readability

                        fc_v  = [float(v) / 1e9 for v in np.asarray(_hw_forecast_vals)]
                        lo_v  = [float(v) / 1e9 for v in np.asarray(_hw_lower)] if _hw_lower is not None else fc_v
                        up_v  = [float(v) / 1e9 for v in np.asarray(_hw_upper)] if _hw_upper is not None else fc_v
                        fc_m  = _next_months(hist_m[-1], len(fc_v))

                        hist_lbl = [_month_label(m) for m in hist_m]
                        fc_lbl   = [_month_label(m) for m in fc_m]

                        # Anchor forecast + band to the last actual so the line
                        # reads as one continuous path with no visual gap.
                        anc_x  = [hist_lbl[-1]] + fc_lbl
                        anc_fc = [hist_v[-1]]   + fc_v
                        anc_up = [hist_v[-1]]   + up_v
                        anc_lo = [hist_v[-1]]   + lo_v

                        fig = make_subplots(
                            rows=2, cols=1, vertical_spacing=0.19,
                            row_heights=[0.60, 0.40],
                            subplot_titles=(
                                f"Monthly Volume Trajectory — {_proj_method}",
                                "Cumulative Progress vs FY 2026 Target",
                            ),
                        )
                        for _ann in fig.layout.annotations:
                            _ann.font = dict(size=13, color=_pal['TEXT_PRI'])
                            _ann.x, _ann.xanchor = 0, 'left'

                        # ── Panel A — monthly trajectory ──
                        fig.add_trace(go.Scatter(
                            x=anc_x, y=anc_up, mode='lines', line=dict(width=0),
                            hoverinfo='skip', showlegend=False,
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=anc_x, y=anc_lo, mode='lines', line=dict(width=0),
                            fill='tonexty', fillcolor=_band_rgba,
                            name='80% confidence', hoverinfo='skip',
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=hist_lbl, y=hist_v, mode='lines+markers', name='Actual',
                            line=dict(color=_C_ACTUAL, width=3),
                            marker=dict(size=6, color=_C_ACTUAL),
                            hovertemplate='<b>%{x}</b><br>Actual: Rp %{y:,.2f} B<extra></extra>',
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=anc_x, y=anc_fc, mode='lines+markers', name='Forecast',
                            line=dict(color=_C_FORECAST, width=3, dash='dash'),
                            marker=dict(size=6, color=_C_FORECAST),
                            hovertemplate='<b>%{x}</b><br>Forecast: Rp %{y:,.2f} B<extra></extra>',
                        ), row=1, col=1)

                        # ── Panel B — cumulative vs FY target ──
                        _cur_year = datetime.now().year
                        _cy = [(m, v) for m, v in zip(_full_m, _full_v) if m // 100 == _cur_year]
                        cum_lbl, cum_v, _run = [], [], 0.0
                        for m, v in _cy:
                            _run += v
                            cum_lbl.append(_month_label(m)); cum_v.append(_run)

                        fcum_lbl, fcum_v = [], []
                        if cum_lbl:
                            fcum_lbl.append(cum_lbl[-1]); fcum_v.append(cum_v[-1])
                        _run_fc = cum_v[-1] if cum_v else 0.0
                        for m, v in zip(fc_m, fc_v):
                            if m // 100 != _cur_year:
                                continue
                            _run_fc += v
                            fcum_lbl.append(_month_label(m)); fcum_v.append(_run_fc)

                        proj_cum_end = fcum_v[-1] if fcum_v else (cum_v[-1] if cum_v else 0.0)
                        fy_target_b  = fy_target / 1e9
                        cum_rate     = (proj_cum_end / fy_target_b * 100) if fy_target_b > 0 else 0
                        end_color    = (_pal['GREEN'] if cum_rate >= 100
                                        else _pal['AMBER'] if cum_rate >= 80
                                        else _pal['RED'])

                        fig.add_trace(go.Scatter(
                            x=cum_lbl, y=cum_v, mode='lines',
                            line=dict(color=_C_ACTUAL, width=2.5),
                            fill='tozeroy', fillcolor=hex_to_rgba(_C_ACTUAL, 0.15),
                            hovertemplate='<b>%{x}</b><br>Cumulative actual: Rp %{y:,.2f} B<extra></extra>',
                            showlegend=False,
                        ), row=2, col=1)
                        fig.add_trace(go.Scatter(
                            x=fcum_lbl, y=fcum_v, mode='lines',
                            line=dict(color=_C_FORECAST, width=2.5, dash='dash'),
                            fill='tozeroy', fillcolor=hex_to_rgba(_C_FORECAST, 0.10),
                            hovertemplate='<b>%{x}</b><br>Cumulative projected: Rp %{y:,.2f} B<extra></extra>',
                            showlegend=False,
                        ), row=2, col=1)
                        if fy_target_b > 0:
                            fig.add_hline(
                                y=fy_target_b, line_dash='dash', line_width=1.5,
                                line_color=_pal['TEXT_SEC'], row=2, col=1,
                                annotation_text=f"FY TARGET — Rp {fy_target_b:,.1f} B",
                                annotation_position='top left',
                                annotation_font=dict(size=10, color=_pal['TEXT_SEC']),
                            )
                        if fcum_lbl:
                            fig.add_trace(go.Scatter(
                                x=[fcum_lbl[-1]], y=[fcum_v[-1]], mode='markers',
                                marker=dict(size=12, color=end_color,
                                            line=dict(width=2, color=_pal['SURFACE'])),
                                hovertemplate=(f'<b>Projected year-end</b><br>'
                                               f'Rp %{{y:,.2f}} B<br>{cum_rate:.0f}% of target'
                                               f'<extra></extra>'),
                                showlegend=False,
                            ), row=2, col=1)
                            fig.add_annotation(
                                x=fcum_lbl[-1], y=fcum_v[-1], row=2, col=1,
                                text=f"<b>{cum_rate:.0f}% of target</b>",
                                showarrow=False, yshift=16,
                                font=dict(size=11, color=end_color),
                            )

                        fig.update_layout(
                            height=560, **_chart_base(),
                            margin=dict(l=10, r=24, t=58, b=24),
                            hovermode='x unified',
                            legend=dict(orientation='h', y=1.10, x=1, xanchor='right',
                                        font=dict(size=11)),
                        )
                        fig.update_xaxes(
                            patch=dict(**_xaxis(), categoryorder='array',
                                       categoryarray=hist_lbl + fc_lbl),
                            row=1, col=1)
                        fig.update_xaxes(
                            patch=dict(**_xaxis(), categoryorder='array',
                                       categoryarray=cum_lbl + fcum_lbl[1:]),
                            row=2, col=1)
                        fig.update_yaxes(
                            patch=dict(**_yaxis(), title_text='Volume (Rp Billion)'),
                            row=1, col=1)
                        fig.update_yaxes(
                            patch=dict(**_yaxis(), title_text='Cumulative (Rp Billion)'),
                            row=2, col=1)
                        st.plotly_chart(fig, use_container_width=True, theme=None)
                        st.caption(
                            f"Model: {_proj_method}. Forecast basis: monthly card-settlement "
                            f"history. Shaded band = 80% confidence range (widens with horizon). "
                            f"Panel B tracks cumulative {_cur_year} volume against the FY target."
                        )
                    else:
                        st.info(
                            f"Statistical forecast unavailable for {sel_merch} "
                            f"({_hw_reason or 'insufficient historical data'}). "
                            f"The year-end projection above uses a linear run-rate estimate."
                        )



    # ── PM Assignment Table — relocated to the bottom of the Overview tab ─────────
    # Preserved for operational/administrative reference, but moved out of the
    # primary analytical flow to keep the page minimalist (still one click away).
    styled_divider()
    if not df_target.empty and 'PM' in df_target.columns:
        with st.expander("View Full PM Assignment Table", expanded=False):
            _tgt_display = df_target[['MERCHANT_GROUP', 'PM']].copy()
            st.dataframe(
                _tgt_display,
                column_config={
                    "MERCHANT_GROUP": st.column_config.TextColumn("Merchant Group", width="large"),
                    "PM":             st.column_config.TextColumn("Account Manager", width="medium"),
                },
                hide_index=True,
                use_container_width=True,
                height=380,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARD SHARE
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    sel_group, sel_brand = portfolio_filter_bar(df_card, scope_key="t1")
    _filt_group = sel_group != "ALL GROUPS"
    _filt_brand = sel_brand not in ("TOTAL GROUP", "TOTAL PORTFOLIO")
    df_card_filt      = df_card[df_card['MERCHANT_GROUP'] == sel_group].copy() if _filt_group else df_card.copy()
    df_card_hist_filt = df_card_hist[df_card_hist['MERCHANT_GROUP'] == sel_group].copy() if _filt_group else df_card_hist.copy()
    if _filt_group and _filt_brand:
        df_card_filt      = df_card_filt[df_card_filt['MERCHANT_ANCHOR'] == sel_brand]
        df_card_hist_filt = df_card_hist_filt[df_card_hist_filt['MERCHANT_ANCHOR'] == sel_brand]

    tab_desc("Monthly payment type breakdown — TRANSACTION / SALES VOLUME / FEE BASED INCOME. Data is sourced directly from the database and respects the filters above.")

    # KPIs from DB (filtered by Merchant Group / Brand selection)
    if not df_card_filt.empty:
        avg_onus = df_card_filt['RASIO_ONUS'].mean() if 'RASIO_ONUS' in df_card_filt.columns else 0
        st.markdown(f"""<div class="stats-grid">
            <div class="stat-card amber">
                <div class="stat-label">YTD Sales Volume</div>
                <div class="stat-value">Rp {df_card_filt['TOTAL_SV'].sum()/1e9:,.1f}M</div>
                <div class="stat-meta">total sales</div>
            </div>
            <div class="stat-card green">
                <div class="stat-label">YTD Fee-Based Income</div>
                <div class="stat-value">Rp {df_card_filt['TOTAL_FBI'].sum()/1e6:,.0f}Jt</div>
                <div class="stat-meta">fee income</div>
            </div>
            <div class="stat-card blue">
                <div class="stat-label">YTD Transactions</div>
                <div class="stat-value">{df_card_filt['TOTAL_TRX'].sum()/1e6:,.2f}M</div>
                <div class="stat-meta">total transactions</div>
            </div>
            <div class="stat-card purple">
                <div class="stat-label">Avg On-Us Ratio</div>
                <div class="stat-value">{avg_onus*100:.1f}%</div>
                <div class="stat-meta">on-us share</div>
            </div>
        </div>""", unsafe_allow_html=True)

    # Reconstruct Monthly Matrix from PROCESSED_CARD_MONTHLY
    df_monthly_raw = (
        _load_monthly_raw(_get_data_version()).copy()
        if has_monthly_tbl else pd.DataFrame()
    )
    # Guard: in offline/snapshot mode the monthly frame may be empty or missing
    # its key columns — skip the section gracefully instead of raising KeyError.
    _monthly_ok = (
        has_monthly_tbl and not df_monthly_raw.empty
        and {'TRX_MONTH', 'YEAR', 'MERCHANT_GROUP', 'MERCHANT_ANCHOR'}.issubset(df_monthly_raw.columns)
    )
    if has_monthly_tbl and not _monthly_ok:
        st.info("Monthly payment-type breakdown is unavailable while the live database is offline.")
    if _monthly_ok:

        # Apply Sidebar Filters to detailed monthly data
        if sel_group != "ALL GROUPS":
            df_monthly_raw = df_monthly_raw[df_monthly_raw['MERCHANT_GROUP'] == sel_group]
            if sel_brand not in ["TOTAL GROUP", "TOTAL PORTFOLIO"]:
                df_monthly_raw = df_monthly_raw[df_monthly_raw['MERCHANT_ANCHOR'] == sel_brand]
        
        # Aggregate across merchants/brands if multiple selected (Exclude grouping columns from sum)
        agg_cols = [c for c in df_monthly_raw.columns if any(p in c for p in ['TRX_','SV_','FBI_','TOTAL_'])]
        agg_cols = [c for c in agg_cols if c not in ['TRX_MONTH', 'YEAR']]
        df_monthly_agg = df_monthly_raw.groupby(['TRX_MONTH', 'YEAR'])[agg_cols].sum().reset_index()
        
        if df_monthly_agg.empty:
            st.info("No monthly trend data found for the current filter.")
        else:
            MONTH_ABB = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
            def get_mo_lbl(row):
                try:
                    code = str(int(row['TRX_MONTH']))
                    if len(code) < 6: return f"ID:{code}"
                    yr, mo = code[:4], int(code[4:])
                    return f"{MONTH_ABB[mo-1]}-{yr[2:]}"
                except:
                    return "Err"
            
            df_monthly_agg['Bulan'] = df_monthly_agg.apply(get_mo_lbl, axis=1)
            avail_years = sorted(df_monthly_agg['YEAR'].unique().tolist(), reverse=True)
            
            col_yr, col_vm = st.columns([2,3])
            with col_yr:
                sel_yr = st.selectbox("Year", ['All'] + [str(y) for y in avail_years], key="t1_year")
            with col_vm:
                chart_type = st.radio("Chart Style", ["Stacked Bar", "Line Trend", "Both"], horizontal=True, key="t1_chart")

            if sel_yr != 'All':
                df_monthly_agg = df_monthly_agg[df_monthly_agg['YEAR'] == int(sel_yr)]

            # Core Payment Type Mapping
            SECTIONS = {
                'TRANSACTION':      ('', BLUE_ACC, ['TRX_DEBIT_ONUS','TRX_DEBIT_OFFUS','TRX_CREDIT_OFFUS','TRX_QRIS_ONUS','TRX_QRIS_OFFUS'], 'TOTAL_TRX'),
                'SALES VOLUME':     ('', GREEN,    ['SV_DEBIT_ONUS','SV_DEBIT_OFFUS','SV_CREDIT_OFFUS','SV_QRIS_ONUS','SV_QRIS_OFFUS'],  'TOTAL_SV'),
                'FEE BASED INCOME': ('', AMBER,    ['FBI_DEBIT_ONUS','FBI_DEBIT_OFFUS','FBI_CREDIT_OFFUS','FBI_QRIS_ONUS','FBI_QRIS_OFFUS'],'TOTAL_FBI'),
            }
            
            TYPE_LABELS = {
                'DEBIT_ONUS': 'Debit BTN (On-Us)',
                'DEBIT_OFFUS':'Debit Other (Off-Us)',
                'CREDIT_OFFUS':'Credit Card',
                'QRIS_ONUS':  'QRIS BTN (On-Us)',
                'QRIS_OFFUS': 'QRIS Other (Off-Us)'
            }

            def fmt_num_db(v, sec_name):
                if v == 0: return "-"
                if 'SALES' in sec_name or 'FEE' in sec_name:
                    if abs(v) >= 1e9: return f"Rp {v/1e9:,.2f}M"
                    if abs(v) >= 1e6: return f"Rp {v/1e6:,.1f}Jt"
                    return f"Rp {v:,.0f}"
                return f"{v:,.0f}"

            for sec_name, (icon, accent, sub_cols, total_col) in SECTIONS.items():
                section_header(icon, sec_name, accent_color=accent)

                # Check which subcols exist in data
                valid_sub = [c for c in sub_cols if c in df_monthly_agg.columns]
                all_display_cols = ['Bulan'] + valid_sub + [total_col]

                display = df_monthly_agg[all_display_cols].copy()

                # Rename columns for cleaner display
                clean_map = {total_col: 'TOTAL'}
                for c in valid_sub:
                    suffix = c.replace('TRX_','').replace('SV_','').replace('FBI_','')
                    clean_map[c] = TYPE_LABELS.get(suffix, suffix)
                display = display.rename(columns=clean_map)

                # YTD row (needed for table + the inline composition pill)
                ytd_vals = display.drop(columns=['Bulan']).sum()

                # Plan §2 declutter #3 + §3.3 — drop the redundant 360-px donut
                # next to every monthly stacked bar (it duplicated the totals
                # the bar already shows). Replace with a one-line composition
                # caption that quotes the top 2 payment types' share — keeps the
                # mix-information value, frees up half the horizontal space,
                # makes the trend chart the main visual.
                _ytd_total = float(ytd_vals.get('TOTAL', 0) or 0)
                _type_cols_caption = [clean_map[c] for c in valid_sub]
                _mix_parts = []
                if _ytd_total > 0:
                    _shares = sorted(
                        ((name, float(ytd_vals.get(name, 0) or 0) / _ytd_total)
                         for name in _type_cols_caption),
                        key=lambda kv: kv[1], reverse=True,
                    )
                    for name, share in _shares[:3]:
                        if share > 0:
                            _mix_parts.append(f"**{name}** {fmt_pct(share, decimals=1, scale=True)}")
                if _mix_parts:
                    st.caption("Mix (selected period): " + " · ".join(_mix_parts))

                # ── Full-width chart (no twin donut) ──────────────────────────
                if chart_type in ("Stacked Bar", "Both"):
                    type_cols_clean = [clean_map[c] for c in valid_sub]
                    melted = display.melt(id_vars="Bulan", value_vars=type_cols_clean, var_name="Type", value_name="Value")
                    fig_s = px.bar(
                        melted, x="Bulan", y="Value", color="Type",
                        color_discrete_map=PAYMENT_COLORS,
                        barmode="stack",
                        title=f"{sec_name} — Composition",
                    )
                    fig_s.update_layout(
                        height=340, margin=dict(l=0, r=0, t=36, b=96),
                        xaxis=dict(tickangle=-30),
                        # Mobile fix: legend below the bars (was the default
                        # right-side vertical legend, which squeezed the chart
                        # into an unreadable sliver on phones). Horizontal +
                        # bottom = bars use 100% width on every screen.
                        legend=dict(
                            orientation="h", yanchor="top", y=-0.30,
                            x=0, xanchor="left",
                            title_text="", font=dict(size=11),
                        ),
                        **_chart_base(),
                    )
                    st.plotly_chart(fig_s, use_container_width=True, theme=None)
                if chart_type in ("Line Trend", "Both"):
                    fig_l = go.Figure()
                    fig_l.add_trace(go.Scatter(
                        x=display["Bulan"], y=display["TOTAL"],
                        mode="lines+markers+text",
                        line=dict(color=accent, width=2.5),
                        text=[fmt_num_db(v, sec_name) for v in display["TOTAL"]],
                        textposition="top center",
                        marker=dict(size=7, color=accent, line=dict(color=_p()['BG'], width=1.5)),
                    ))
                    fig_l.update_layout(
                        title=f"{sec_name} — Total Trend",
                        height=320, margin=dict(l=0, r=0, t=36, b=64),
                        xaxis=dict(tickangle=-30),
                        **_chart_base(),
                    )
                    st.plotly_chart(fig_l, use_container_width=True, theme=None)

                # ── Monthly breakdown table in expander (drill-down) ────────────
                with st.expander("View Monthly Breakdown Table"):
                    ytd_row = pd.DataFrame([['YTD (Selected)'] + ytd_vals.tolist()], columns=display.columns)
                    disp_full = pd.concat([display, ytd_row], ignore_index=True)

                    disp_fmt = disp_full.copy()
                    val_cols_fmt = [c for c in disp_fmt.columns if c != 'Bulan']
                    for col in val_cols_fmt:
                        disp_fmt[col] = disp_fmt[col].apply(lambda v: fmt_num_db(v, sec_name))

                    _accent_cap = accent
                    def style_table_db(row):
                        is_ytd = row.name == len(disp_fmt) - 1
                        styles = []
                        for col in disp_fmt.columns:
                            if is_ytd:
                                styles.append(f'background-color:{_accent_cap};color:white;font-weight:bold;')
                            elif col == 'TOTAL':
                                styles.append('font-weight:600;')
                            else:
                                styles.append('')
                        return styles

                    st.dataframe(
                        disp_fmt.style.apply(style_table_db, axis=1),
                        use_container_width=True, hide_index=True,
                        height=min(38 * len(disp_fmt) + 40, 500),
                    )

    else:
        conn.close()
        st.warning("PROCESSED_CARD_MONTHLY table is missing. Re-run the Automated Pipeline.")

    styled_divider()


    # Top Merchants overview from DB
    if not df_card_filt.empty:
        section_label("Top Merchants Analytics (YTD)")

        # Create a rich dataframe with calculated metrics
        df_c = df_card_filt.copy()
        df_c['AVG_TRX_VAL'] = np.where(df_c['TOTAL_TRX'] > 0, df_c['TOTAL_SV'] / df_c['TOTAL_TRX'], 0)
        df_c['FBI_YIELD'] = np.where(df_c['TOTAL_SV'] > 0, (df_c['TOTAL_FBI'] / df_c['TOTAL_SV']) * 100, 0)
        
        top_n_c = st.slider("Top N Merchants", 10, 50, 20, key="t1_topn")

        df_top = df_c.sort_values('TOTAL_SV', ascending=False).head(top_n_c)
        
        # Format display dataframe
        disp_top = df_top[['MERCHANT_GROUP', 'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'AVG_TRX_VAL', 'FBI_YIELD', 'RASIO_ONUS']].copy()
        
        # Plan §3.6 — route through utils/formatting so values auto-scale with magnitude
        # (e.g. Rp 28.9 Jt vs Rp 1.93 M instead of hard-coded /1e9 that crushes mid-tier
        # merchants to "Rp 0.05 M"). FBI_YIELD is already in percent form (line 1165
        # multiplied by 100), RASIO_ONUS is a 0–1 fraction. AVG_TRX_VAL stays in raw
        # rupiah for column-wise comparability.
        format_dict = {
            'Sales Volume':     fmt_currency_idr,
            'Fee Based Income': fmt_currency_idr,
            'Transactions':     fmt_count,
            'Avg Trx Size':     lambda x: f"Rp {x:,.0f}" if pd.notna(x) else "—",
            'FBI Yield':        lambda x: fmt_pct(x, decimals=2, scale=False),
            'On-Us Ratio':      lambda x: fmt_pct(x, decimals=1, scale=True),
        }
        
        col_names = {
            'MERCHANT_GROUP': 'Merchant Group',
            'TOTAL_SV': 'Sales Volume',
            'TOTAL_TRX': 'Transactions',
            'TOTAL_FBI': 'Fee Based Income',
            'AVG_TRX_VAL': 'Avg Trx Size',
            'FBI_YIELD': 'FBI Yield',
            'RASIO_ONUS': 'On-Us Ratio'
        }
        
        _disp_top = disp_top.rename(columns=col_names)
        st.dataframe(
            _disp_top.style.format(format_dict),
            use_container_width=True, height=min(38 * len(disp_top) + 40, 500)
        )

        with st.expander("Raw Card Share Data"):
            st.dataframe(df_c.reset_index(drop=True), use_container_width=True)
            st.download_button("Download CSV", df_c.to_csv(index=False, encoding='utf-8-sig'), "card_share_data.csv", "text/csv")

        # ── GROWTH ANALYTICS (Realisasi) ──────────────────────────────────
        # We can now use df_card_hist from DB for growth instead of re-parsing Excel
        if not df_card_hist.empty:
            max_month = df_card_hist['TRX_MONTH'].max()
            try:
                curr_yr = int(str(max_month)[:4])
                curr_mo = int(str(max_month)[4:])
                # True month-over-month: previous calendar month with January rollback.
                if curr_mo == 1:
                    prev_yr, prev_mo = curr_yr - 1, 12
                else:
                    prev_yr, prev_mo = curr_yr, curr_mo - 1
                prev_month = int(f"{prev_yr}{prev_mo:02d}")
                fy_prev_yr = curr_yr - 1

                MONTH_ABB = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                col_curr = f"{MONTH_ABB[curr_mo-1]}-{str(curr_yr)[2:]}"
                col_prev = f"{MONTH_ABB[prev_mo-1]}-{str(prev_yr)[2:]}"
                col_fy_prev = f"FY-{str(fy_prev_yr)[2:]}"

                gh1, gh2 = st.columns([3, 1])
                with gh1:
                    section_label("Top & Bottom Merchant Growth (MoM)")
                    _freshness_txt = f"Comparing **{col_curr}** vs **{col_prev}** (previous month)"
                    if _last_update != 'Unknown':
                        _freshness_txt += f" | Last data update: **{_last_update}**"
                    st.caption(_freshness_txt)
                    st.caption("Growth rates are point-in-time comparisons from the last data ingestion. Run the pipeline to refresh.")
                with gh2:
                    metric_sel = st.selectbox(
                        "Metric",
                        ["SALES VOLUME", "TRANSACTION", "FEE BASED INCOME"],
                        key="t1_metric_growth",
                        label_visibility="collapsed"
                    )
                m_col = 'TOTAL_SV' if 'SALES' in metric_sel else ('TOTAL_TRX' if 'TRANS' in metric_sel else 'TOTAL_FBI')
                
                # Current month
                df_curr = df_card_hist[df_card_hist['TRX_MONTH'] == max_month].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_curr)
                # Previous month
                df_prev = df_card_hist[df_card_hist['TRX_MONTH'] == prev_month].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_prev)
                # FY Previous (prior full calendar year, separate from MoM prev_yr)
                df_fy = df_card_hist[df_card_hist['YEAR'] == fy_prev_yr].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_fy_prev)
                
                df_growth = pd.merge(df_curr, df_prev, on='MERCHANT_GROUP', how='outer')
                df_growth = pd.merge(df_growth, df_fy, on='MERCHANT_GROUP', how='outer').fillna(0)

                # Plan §3.1 — replace the broken raw `(curr - prev) / prev` formula
                # with: baseline-floor classification + symmetric percent change for
                # ranking + separate buckets for new merchants and dropped-off ones.
                # This kills the HOKBEN +328,734% outlier and the four merchants pinned
                # at exactly -100% in the prior visualization.
                _baseline = BASELINE_FLOORS[m_col]
                df_signals = compute_growth_signals(
                    df_growth, curr_col=col_curr, prev_col=col_prev, baseline=_baseline,
                )

                df_established = df_signals[df_signals['Status'] == 'established'].copy()
                df_new_react   = df_signals[df_signals['Status'] == 'new_reactivated'].copy()
                df_dropped     = df_signals[df_signals['Status'] == 'dropped_off'].copy()

                # Rank by symmetric percent change (bounded [-200, +200]) so the chart
                # scale is meaningful. Raw Growth % stays in the table for reference.
                top_10 = df_established.sort_values('Symmetric %', ascending=False).head(10).copy()
                bot_10 = df_established.sort_values('Symmetric %', ascending=True).head(10).copy()

                def _val_fmt(x):
                    if pd.isna(x):
                        return "—"
                    if 'TOTAL_TRX' in m_col:
                        return fmt_count(x)
                    return fmt_currency_idr(x)

                def _row_growth_color(row):
                    pct = row['Symmetric %']
                    color = GREEN if pct > 0 else (RED if pct < 0 else TEXT_SEC)
                    styles = [''] * len(row)
                    # Color only the % columns. Index by column name to be order-safe.
                    for i, col in enumerate(row.index):
                        if col in ('Growth %', 'Symmetric %', 'Delta'):
                            styles[i] = f'color: {color}; font-weight: 600;'
                    return styles

                _table_formatters = {
                    col_curr:    _val_fmt,
                    col_prev:    _val_fmt,
                    col_fy_prev: _val_fmt,
                    'Delta':     _val_fmt,
                    'Growth %':     lambda x: fmt_growth(x, decimals=1, scale=False, cap=10_000),
                    'Symmetric %':  lambda x: fmt_growth(x, decimals=1, scale=False),
                }

                # Coverage caption — surface how many merchants were classified into each
                # bucket so the user understands why the bar chart only shows established
                # merchants. Without this, "Top/Bottom 10" feels incomplete.
                _n_total = len(df_signals)
                _n_est   = len(df_established)
                _n_new   = len(df_new_react)
                _n_drop  = len(df_dropped)
                _n_inact = _n_total - _n_est - _n_new - _n_drop
                st.caption(
                    f"Of **{_n_total}** merchants this month: "
                    f"**{_n_est}** established · **{_n_new}** new/re-activated · "
                    f"**{_n_drop}** dropped off · **{_n_inact}** inactive. "
                    f"Only established merchants appear in the Top/Bottom bars below — "
                    f"baseline for {metric_sel} is {fmt_currency_idr(_baseline) if 'TOTAL_TRX' not in m_col else fmt_count(_baseline)}."
                )

                # Plan §3.1 visual fix — side-by-side panels (each with its own scale),
                # symmetric-pct as the bar value (bounded), and bar text that combines
                # the absolute Delta with the % so users see both the relative move and
                # the actual rupiah/trx change.
                def _bar_text(row, prefix=""):
                    delta_str = _val_fmt(row['Delta'])
                    pct_str   = fmt_growth(row['Symmetric %'], decimals=1, scale=False)
                    return f"{prefix}{delta_str}  ({pct_str})"

                _gcol_l, _gcol_r = st.columns(2, gap="large")
                with _gcol_l:
                    section_label(f"Top 10 by {metric_sel} Growth")
                    if not top_10.empty:
                        # Reverse order so largest growth appears at the top of the bar chart.
                        _top_plot = top_10.iloc[::-1]
                        fig_top10 = go.Figure(go.Bar(
                            x=_top_plot['Symmetric %'],
                            y=_top_plot['MERCHANT_GROUP'],
                            orientation='h',
                            marker_color=GREEN,
                            text=[_bar_text(r, prefix="+") for _, r in _top_plot.iterrows()],
                            textposition='outside',
                            cliponaxis=False,
                        ))
                        fig_top10.update_layout(
                            height=380, margin=dict(l=4, r=140, t=10, b=32),
                            xaxis={**_xaxis(), 'title': 'Symmetric Growth % (bounded ±200)',
                                   'range': [0, 220]},
                            yaxis=dict(showgrid=False, automargin=True),
                            **_chart_base(),
                        )
                        st.plotly_chart(fig_top10, use_container_width=True, theme=None)
                        with st.expander("View raw data"):
                            st.dataframe(
                                top_10[['MERCHANT_GROUP', col_curr, col_prev, 'Delta', 'Growth %', 'Symmetric %']]
                                .style.apply(_row_growth_color, axis=1).format(_table_formatters).hide(axis="index"),
                                use_container_width=True,
                            )
                    else:
                        st.info("No established merchants this month.")

                with _gcol_r:
                    section_label(f"Bottom 10 by {metric_sel} Growth")
                    if not bot_10.empty:
                        # Reverse so largest decline (most negative) appears at the top.
                        _bot_plot = bot_10.iloc[::-1]
                        fig_bot10 = go.Figure(go.Bar(
                            x=_bot_plot['Symmetric %'],
                            y=_bot_plot['MERCHANT_GROUP'],
                            orientation='h',
                            marker_color=RED,
                            text=[_bar_text(r) for _, r in _bot_plot.iterrows()],
                            textposition='outside',
                            cliponaxis=False,
                        ))
                        fig_bot10.update_layout(
                            height=380, margin=dict(l=4, r=140, t=10, b=32),
                            xaxis={**_xaxis(), 'title': 'Symmetric Growth % (bounded ±200)',
                                   'range': [-220, 0]},
                            yaxis=dict(showgrid=False, automargin=True),
                            **_chart_base(),
                        )
                        st.plotly_chart(fig_bot10, use_container_width=True, theme=None)
                        with st.expander("View raw data"):
                            st.dataframe(
                                bot_10[['MERCHANT_GROUP', col_curr, col_prev, 'Delta', 'Growth %', 'Symmetric %']]
                                .style.apply(_row_growth_color, axis=1).format(_table_formatters).hide(axis="index"),
                                use_container_width=True,
                            )
                    else:
                        st.info("No established merchants this month.")

            except Exception as e:
                st.error(f"Growth calculation failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WEEKLY MONITORING (reads from '2026' sheet directly)
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    sel_group, sel_brand = portfolio_filter_bar(df_card, scope_key="t2")
    _filt_group = sel_group != "ALL GROUPS"

    # Weekly Monitor: df_mon_weekly.MERCHANT_GROUP may store brand names
    # (= df_card.MERCHANT_ANCHOR) OR the parent group name directly — handle
    # both naming conventions.
    df_mon_weekly_filt = df_mon_weekly.copy()
    if _filt_group and not df_card.empty and not df_mon_weekly_filt.empty:
        _group_brands = (
            df_card[df_card['MERCHANT_GROUP'] == sel_group]['MERCHANT_ANCHOR']
            .str.strip().str.upper().unique()
        )
        _mon_mg_upper = df_mon_weekly_filt['MERCHANT_GROUP'].str.strip().str.upper()
        df_mon_weekly_filt = df_mon_weekly_filt[
            _mon_mg_upper.isin(_group_brands) | (_mon_mg_upper == sel_group)
        ]

    # Tab rebuilt for at-a-glance clarity. Old layout buried the actual signals
    # (this week's volume, week-over-week change, who moved) under a 6-control
    # filter bar, a meaningless "Selected Year" KPI card, and a 52-column raw
    # matrix at the bottom. New layout leads with the four numbers a director
    # actually wants, then movers, then a compact 12-week heatmap, with the
    # full matrix demoted to an opt-in expander.
    tab_desc(
        "Weekly pulse of the merchant portfolio. Pick a metric — the KPIs, "
        "movers, and heatmap update together. The full 52-week matrix lives "
        "in the expander at the bottom for power users."
    )

    if not has_mon_weekly:
        st.warning("Weekly Monitoring data is missing. Run the Automated Pipeline first.")
    else:
        # ── Step 1: Minimal controls (year + metric only) ───────────────────
        avail_years_mon = (
            sorted(df_mon_weekly_filt['YEAR'].unique().tolist(), reverse=True)
            if not df_mon_weekly_filt.empty and 'YEAR' in df_mon_weekly_filt.columns
            else []
        )
        _DIM_LABELS = {'VOL': 'Volume', 'TRX': 'Transactions', 'FBI': 'Fee Income'}
        _ctl_l, _ctl_r = st.columns([1, 3])
        with _ctl_l:
            sel_yr_mon = st.selectbox(
                "Year",
                [str(y) for y in avail_years_mon] if avail_years_mon else ["No Data"],
                key="t2_year_mon",
            )
        with _ctl_r:
            _df_year = (
                df_mon_weekly_filt[df_mon_weekly_filt['YEAR'] == str(sel_yr_mon)].copy()
                if not df_mon_weekly_filt.empty and 'YEAR' in df_mon_weekly_filt.columns
                else pd.DataFrame()
            )
            _avail_dims = (sorted(_df_year['DIMENSI'].dropna().unique().tolist())
                           if not _df_year.empty and 'DIMENSI' in _df_year.columns else [])
            # Default to VOL — it's the headline business metric.
            _default_idx = _avail_dims.index('VOL') if 'VOL' in _avail_dims else 0
            sel_metric = st.radio(
                "Metric",
                _avail_dims if _avail_dims else ['VOL'],
                index=_default_idx if _avail_dims else 0,
                horizontal=True,
                key="t2_metric_pick",
                format_func=lambda d: _DIM_LABELS.get(d, d),
            )

        # ── Step 2: Build the metric-scoped slice ───────────────────────────
        df_metric = _df_year[_df_year['DIMENSI'] == sel_metric].copy() if not _df_year.empty else pd.DataFrame()

        if df_metric.empty:
            st.info(f"No {sel_metric} data for {sel_yr_mon}.")
        else:
            W_COLS_DB = sorted(
                [c for c in df_metric.columns if c.startswith('W') and len(c) >= 2 and c[1:].isdigit()],
                key=lambda c: int(c[1:]),
            )
            # Coerce W-columns to numeric (the source can be object-typed).
            for _wc in W_COLS_DB:
                df_metric[_wc] = pd.to_numeric(df_metric[_wc], errors='coerce').fillna(0.0)

            # Aggregate per-week totals across all merchants in scope.
            _weekly_totals = df_metric[W_COLS_DB].sum(axis=0)

            # Latest "this week" = the most recent populated week.
            _populated = [w for w in W_COLS_DB if _weekly_totals[w] > 0]
            if not _populated:
                st.info(f"No populated weeks for {sel_metric} in {sel_yr_mon} yet.")
                _latest_w = None
            else:
                _latest_w = _populated[-1]

            # Helper used in multiple places: format a value for the active metric.
            def _fmt_metric_val(v):
                if pd.isna(v):
                    return "—"
                if sel_metric == 'TRX':
                    return fmt_count(v)
                return fmt_currency_idr(v)

            # ── Step 3: Insight-forward KPI strip (4 cards that answer real Qs) ─
            _this_week_total = float(_weekly_totals[_latest_w]) if _latest_w else 0.0

            # WoW: this week vs the average of the prior 4 weeks (not just last week)
            # — robust to single-week noise. Same baseline used by Health Alerts.
            _baseline_4w = 0.0
            _wow_pct = 0.0
            if _latest_w is not None:
                _idx = W_COLS_DB.index(_latest_w)
                _prior_window = W_COLS_DB[max(0, _idx - 4):_idx]
                if _prior_window:
                    _baseline_4w = float(_weekly_totals[_prior_window].mean())
                    if _baseline_4w > 0:
                        _wow_pct = (_this_week_total - _baseline_4w) / _baseline_4w

            # Active merchants this week = how many have non-zero W{latest}.
            _active_this_week = int((df_metric[_latest_w] > 0).sum()) if _latest_w else 0
            _active_total     = int((df_metric[W_COLS_DB].sum(axis=1) > 0).sum())

            # Behind pace = merchants whose W{latest} fell below half their 4-week avg.
            _behind_pace = 0
            if _latest_w and _idx > 0:
                _row_avg = df_metric[_prior_window].mean(axis=1) if _prior_window else df_metric[W_COLS_DB].mean(axis=1)
                _behind_pace = int(((df_metric[_latest_w] < _row_avg * 0.5) & (_row_avg > 0)).sum())

            _wow_color = SUCCESS if _wow_pct > 0.02 else (DANGER if _wow_pct < -0.02 else WARNING)
            _wow_arrow = "▲" if _wow_pct > 0 else ("▼" if _wow_pct < 0 else "•")

            _k1, _k2, _k3, _k4 = st.columns(4)
            with _k1:
                st.markdown(kpi_card(
                    _fmt_metric_val(_this_week_total),
                    f"This week ({_latest_w or '—'})",
                ), unsafe_allow_html=True)
            with _k2:
                _wow_str = f"{_wow_arrow} {fmt_growth(_wow_pct, decimals=1, scale=True)}" if _latest_w else "—"
                st.markdown(
                    f"<div class='kpi-card' style='border-top:3px solid {_wow_color};'>"
                    f"<div class='kpi-val' style='color:{_wow_color};'>{_wow_str}</div>"
                    f"<div class='kpi-lbl'>vs prior 4-week avg</div></div>",
                    unsafe_allow_html=True,
                )
            with _k3:
                st.markdown(kpi_card(
                    f"{_active_this_week} / {_active_total}",
                    "Active this week",
                ), unsafe_allow_html=True)
            with _k4:
                _bp_kind = "danger" if _behind_pace > 0 else "success"
                st.markdown(kpi_card(
                    str(_behind_pace),
                    "Behind pace · investigate",
                    kind=_bp_kind,
                ), unsafe_allow_html=True)

            styled_divider()

            # ── Step 4: Portfolio Pulse — single full-width line ────────────
            section_label(f"Portfolio Pulse — {_DIM_LABELS.get(sel_metric, sel_metric)} · {sel_yr_mon}")

            _pulse_x = [w[1:].lstrip('0') or '0' for w in W_COLS_DB]
            _pulse_y = [_weekly_totals[w] for w in W_COLS_DB]
            _avg_y = float(np.mean([y for y in _pulse_y if y > 0])) if any(y > 0 for y in _pulse_y) else 0.0

            fig_pulse = go.Figure()
            fig_pulse.add_trace(go.Scatter(
                x=_pulse_x, y=_pulse_y,
                mode='lines+markers',
                line=dict(color=BLUE_ACC, width=2.5),
                marker=dict(size=6, color=BLUE_ACC),
                fill='tozeroy', fillcolor=hex_to_rgba(BLUE_ACC, 0.10),
                name='Total per week',
                hovertemplate='Week %{x}<br>%{y:,.0f}<extra></extra>',
            ))
            # Highlight the most-recent week with a star so the eye lands there.
            if _latest_w is not None:
                _idx_latest = W_COLS_DB.index(_latest_w)
                fig_pulse.add_trace(go.Scatter(
                    x=[_pulse_x[_idx_latest]],
                    y=[_pulse_y[_idx_latest]],
                    mode='markers+text',
                    marker=dict(color=SUCCESS, size=16, symbol='star',
                                line=dict(color='white', width=2)),
                    text=[f"  {_fmt_metric_val(_this_week_total)}"],
                    textposition='middle right',
                    textfont=dict(size=11, color=TEXT_PRI),
                    name='This week',
                    showlegend=False,
                ))
            if _avg_y > 0:
                fig_pulse.add_hline(
                    y=_avg_y, line_dash='dot', line_color=TEXT_SEC, opacity=0.6,
                    annotation_text=f"avg {_fmt_metric_val(_avg_y)}",
                    annotation_position='top left',
                )
            fig_pulse.update_layout(
                height=320, margin=dict(l=4, r=4, t=10, b=40),
                xaxis={**_xaxis(), 'title': 'Week'},
                yaxis=_yaxis(),
                **_chart_base(),
                showlegend=False,
            )
            st.plotly_chart(fig_pulse, use_container_width=True, theme=None)

            # ── Step 5: Top movers this week (gainers / losers) ─────────────
            # Per-merchant change = W{latest} − mean of prior 4 weeks. Filter to
            # merchants with a meaningful baseline so % deltas are interpretable.
            section_label("Top movers — this week vs prior 4-week average")
            _movers = pd.DataFrame()
            if _latest_w is not None and _idx > 0:
                _baseline_per_merch = (df_metric[_prior_window].mean(axis=1)
                                       if _prior_window else pd.Series(dtype=float))
                _movers = df_metric[['MERCHANT_GROUP']].copy()
                _movers['this_week'] = df_metric[_latest_w].values
                _movers['baseline'] = _baseline_per_merch.values
                _movers['delta'] = _movers['this_week'] - _movers['baseline']
                _movers['pct'] = np.where(
                    _movers['baseline'] > 0,
                    _movers['delta'] / _movers['baseline'],
                    np.nan,
                )
                # Require a meaningful baseline so we don't surface 1-trx merchants.
                _meaningful = _movers[(_movers['baseline'] > 0) & _movers['pct'].notna()]
                _gainers = _meaningful.sort_values('pct', ascending=False).head(5)
                _losers  = _meaningful.sort_values('pct', ascending=True ).head(5)
            else:
                _gainers = pd.DataFrame()
                _losers  = pd.DataFrame()

            def _render_mover(row, *, gain: bool):
                _color = SUCCESS if gain else DANGER
                _arrow = "▲" if gain else "▼"
                # 8-week mini-sparkline so the trajectory is visible at a glance.
                _spark_cols = W_COLS_DB[max(0, _idx - 7):_idx + 1] if _latest_w else W_COLS_DB[-8:]
                _spark_vals = (df_metric[df_metric['MERCHANT_GROUP'] == row['MERCHANT_GROUP']]
                               [_spark_cols].iloc[0].tolist()
                               if _spark_cols and not df_metric[df_metric['MERCHANT_GROUP'] == row['MERCHANT_GROUP']].empty
                               else [])
                _c1, _c2, _c3 = st.columns([3, 2, 2])
                with _c1:
                    st.markdown(
                        f"<div style='font-weight:var(--fw-semibold);font-size:var(--fs-sm);'>"
                        f"{row['MERCHANT_GROUP']}"
                        f"</div>"
                        f"<div style='color:var(--btn-text-sec);font-size:var(--fs-xs);'>"
                        f"{_fmt_metric_val(row['this_week'])} this week · {_fmt_metric_val(row['baseline'])} avg"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with _c2:
                    if len(_spark_vals) >= 2:
                        _spark = go.Figure(go.Scatter(
                            x=list(range(len(_spark_vals))),
                            y=_spark_vals,
                            mode='lines',
                            line=dict(color=_color, width=2),
                            fill='tozeroy',
                            fillcolor=hex_to_rgba(_color, 0.15),
                        ))
                        _spark.update_layout(
                            height=44, margin=dict(l=0, r=0, t=0, b=0),
                            showlegend=False,
                            xaxis=dict(visible=False),
                            yaxis=dict(visible=False),
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                        )
                        st.plotly_chart(
                            _spark, use_container_width=True, theme=None,
                            config={'displayModeBar': False, 'staticPlot': True},
                        )
                with _c3:
                    st.markdown(
                        f"<div style='text-align:right;color:{_color};"
                        f"font-weight:var(--fw-bold);font-size:var(--fs-md);'>"
                        f"{_arrow} {fmt_growth(row['pct'], decimals=1, scale=True)}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            _mvl, _mvr = st.columns(2, gap="large")
            with _mvl:
                st.markdown(
                    f"<div style='color:{SUCCESS};font-weight:var(--fw-bold);"
                    f"text-transform:uppercase;letter-spacing:1px;font-size:var(--fs-xs);"
                    f"margin-bottom:6px;'>▲ Top 5 gainers</div>",
                    unsafe_allow_html=True,
                )
                if _gainers.empty:
                    st.caption("Not enough history yet to compute gainers.")
                else:
                    for _, _row in _gainers.iterrows():
                        with st.container(border=True):
                            _render_mover(_row, gain=True)
            with _mvr:
                st.markdown(
                    f"<div style='color:{DANGER};font-weight:var(--fw-bold);"
                    f"text-transform:uppercase;letter-spacing:1px;font-size:var(--fs-xs);"
                    f"margin-bottom:6px;'>▼ Top 5 losers</div>",
                    unsafe_allow_html=True,
                )
                if _losers.empty:
                    st.caption("Not enough history yet to compute losers.")
                else:
                    for _, _row in _losers.iterrows():
                        with st.container(border=True):
                            _render_mover(_row, gain=False)

            # ── Step 6: Compact 12-week × top-10 heatmap ─────────────────────
            styled_divider()
            section_label("Recent 12-week heatmap — top 10 merchants by YTD")
            _ytd_per_merch = df_metric[W_COLS_DB].sum(axis=1)
            _top10_idx = _ytd_per_merch.sort_values(ascending=False).head(10).index
            # Use last 12 *populated* weeks so future empty weeks don't wash out the chart.
            _last12 = _populated[-12:] if len(_populated) >= 1 else W_COLS_DB[-12:]
            _heat_df = df_metric.loc[_top10_idx, ['MERCHANT_GROUP'] + _last12].set_index('MERCHANT_GROUP')

            if _heat_df.empty:
                st.caption("Not enough top-merchant data for a heatmap yet.")
            else:
                # Row-wise z-score: shows each merchant's week-over-week variation regardless
                # of their absolute volume (avoids the all-same-color problem).
                _heat_vals = _heat_df.astype(float)
                _heat_norm = _heat_vals.apply(
                    lambda row: (row - row.mean()) / row.std() if row.std() > 0 else row * 0.0,
                    axis=1,
                )
                fig_heat = px.imshow(
                    _heat_norm.values,
                    x=[w[1:].lstrip('0') or '0' for w in _last12],
                    y=_heat_df.index.tolist(),
                    color_continuous_scale='RdYlGn',
                    color_continuous_midpoint=0,
                    aspect='auto',
                    labels={'x': 'Week', 'y': 'Merchant', 'color': 'vs avg (σ)'},
                )
                fig_heat.update_layout(
                    height=max(220, 36 * len(_heat_df) + 100),
                    margin=dict(l=8, r=8, t=10, b=40),
                    **_chart_base(),
                )
                st.plotly_chart(fig_heat, use_container_width=True, theme=None)

            # ── Step 7: Full matrix demoted to expander (power-user view) ───
            with st.expander(f"Full {sel_yr_mon} weekly matrix · all merchants · all metrics", expanded=False):
                _full = _df_year.copy()
                _W_FULL = sorted(
                    [c for c in _full.columns if c.startswith('W') and len(c) >= 2 and c[1:].isdigit()],
                    key=lambda c: int(c[1:]),
                )
                _grp_cols = [c for c in ('MERCHANT_GROUP', 'DIMENSI', 'PM', 'FY', 'YTD') if c in _full.columns]
                _matrix = _full[_grp_cols + _W_FULL].fillna(0).reset_index(drop=True)
                # Format the W-columns and YTD per row for readability.
                def _fmt_row(v, dim):
                    if pd.isna(v) or v == 0:
                        return "—"
                    if str(dim).upper() == 'TRX':
                        return fmt_count(v)
                    return fmt_currency_idr(v)
                _disp = _matrix.copy()
                if 'DIMENSI' in _disp.columns:
                    for _wc in (_W_FULL + (['YTD'] if 'YTD' in _disp.columns else [])):
                        _disp[_wc] = [_fmt_row(_v, _d) for _v, _d in zip(_disp[_wc], _disp['DIMENSI'])]
                st.dataframe(_disp, use_container_width=True, height=420, hide_index=True)
                st.download_button(
                    "Export full matrix as CSV",
                    _matrix.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                    f"weekly_matrix_{sel_yr_mon}.csv", "text/csv",
                )




with tab3:
    tab_desc("Merchant Segmentation Profiler — automatically groups your portfolio into performance tiers based on volume, growth, fee income, and target achievement. Identify which merchants to prioritize, nurture, or investigate.")

    if not (has_card and has_mon):
        st.warning("Merchant segmentation requires **both** Card Share and Monitoring data to be processed first.")
    else:
        with st.spinner("Analyzing merchant performance tiers..."):
            df_ml = run_ml(df_card, df_mon, df_target)

        if df_ml.empty:
            st.info("No data available for Machine Learning analysis. Please ensure the database has been populated.")
        else:
            all_pm_ml = sorted(df_ml['PM'].dropna().unique().tolist()) if 'PM' in df_ml.columns else []
            all_clusters = sorted(df_ml['CLUSTER'].dropna().unique().tolist())

            # Controls
            mc1, mc2 = st.columns(2, gap="large")
            with mc1:
                sel_pm_ml = st.multiselect("Filter by PM", all_pm_ml, default=all_pm_ml, key="t3_pm")
            with mc2:
                sel_clust = st.multiselect("Show Clusters", all_clusters, default=all_clusters, key="t3_clust")

            df_f = df_ml[df_ml['CLUSTER'].isin(sel_clust)]
            if sel_pm_ml and 'PM' in df_f.columns:
                df_f = df_f[df_f['PM'].isin(sel_pm_ml)]

            filtered = len(sel_pm_ml) < len(all_pm_ml) or len(sel_clust) < len(all_clusters)
            if filtered:
                filter_pill(f"Filter Active: {len(df_f)} of {len(df_ml)} merchants shown")
            else:
                tab_desc(f"Showing all <b>{len(df_f)}</b> merchants across all clusters.")

            # Color mapper — sourced from CLUSTER_COLORS (STYLING_GUIDE.md §1)
            color_lookup = CLUSTER_COLORS.copy()
            fallback_colors = ['#27AE60', '#2F80ED', '#EB5757', '#F39C12', '#9B59B6', '#34495E']
            
            # ── Segment metric grid with action recommendations ─────────────────
            SEGMENT_ICONS = {
                'ELITE': '', 'PREMIUM': '', 'REGULER': '',
                'PASIF': '', 'DORMANT': '',
            }
            SEGMENT_ACTIONS = {
                'ELITE':   "Top performers — target for loyalty program & upsell opportunities.",
                'PREMIUM': "Strong performers — nurture to Elite; schedule quarterly review.",
                'REGULER': "Stable base — identify growth levers; monitor On-Us ratio trend.",
                'PASIF':   "Underperforming — assign PM follow-up; investigate root cause.",
                'DORMANT': "Inactive or at-risk — immediate outreach required.",
            }
            total_merchants = len(df_f)
            _pp3 = _p()

            # ── Tier order: PREMIUM → REGULER → PASIF (stable, not alphabetical) ─
            _TIER_RANK = ['ELITE', 'PREMIUM', 'REGULER', 'PASIF', 'DORMANT']
            _ordered_clusters = (
                [t for t in _TIER_RANK if t in all_clusters]
                + [c for c in all_clusters if c not in _TIER_RANK]
            )

            # ── Tier summary cards with tier economics ──────────────────────────
            total_fbi   = float(df_f['AVG_FBI'].sum()) if 'AVG_FBI' in df_f.columns else 0.0
            _cards_html = '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;">'
            _conc_rows  = []
            for seg in _ordered_clusters:
                _seg_df   = df_f[df_f['CLUSTER'] == seg]
                n         = len(_seg_df)
                pct       = (n / total_merchants * 100) if total_merchants > 0 else 0
                seg_fbi   = float(_seg_df['AVG_FBI'].sum()) if 'AVG_FBI' in _seg_df.columns else 0.0
                fbi_share = (seg_fbi / total_fbi * 100) if total_fbi > 0 else 0
                action    = SEGMENT_ACTIONS.get(seg, "Review this group with your PM team.")
                high_in_seg = (
                    len(df_ml[(df_ml['CLUSTER'] == seg) & (df_ml['CHURN_RISK'] == 'HIGH RISK')])
                    if not df_ml.empty and 'CHURN_RISK' in df_ml.columns else 0
                )
                c = color_lookup.get(seg, '#888888')
                warn_chip = (
                    status_chip_html(f"{high_in_seg} High Risk", "danger")
                    if high_in_seg > 0 else ''
                )
                _cards_html += (
                    f'<div style="flex:1;min-width:175px;border-left:5px solid {c};'
                    f'background:{c}14;border-radius:0 20px 20px 0;padding:18px 20px;'
                    f'box-shadow:0 5px 20px rgba(0,0,0,0.05);">'
                    f'<div class="kpi-label" style="color:{c};">{seg}</div>'
                    f'<div class="kpi-value" style="margin:6px 0 2px;">{n}</div>'
                    f'<div class="kpi-meta" style="margin-bottom:8px;">{pct:.1f}% of fleet</div>'
                    f'<div style="height:4px;border-radius:2px;background:{_pp3["BORDER"]};margin-bottom:14px;">'
                    f'<div style="width:{min(pct,100):.1f}%;height:100%;border-radius:2px;background:{c};"></div>'
                    f'</div>'
                    f'<div style="font-weight:var(--fw-bold);font-size:15px;'
                    f'color:{_pp3["TEXT_PRI"]};font-variant-numeric:tabular-nums;">'
                    f'{fmt_currency_idr(seg_fbi)}</div>'
                    f'<div class="kpi-meta">{fbi_share:.0f}% of portfolio fee income</div>'
                    f'{warn_chip}'
                    f'<div class="kpi-meta" style="margin-top:8px;line-height:1.55;">{action}</div>'
                    f'</div>'
                )
                _conc_rows.append((seg, c, n, seg_fbi, fbi_share))
            _cards_html += '</div>'
            st.markdown(_cards_html, unsafe_allow_html=True)

            # ── Value concentration bar — share of portfolio fee income ─────────
            section_label("Fee Income Concentration")
            if total_fbi > 0:
                _conc_bar = ('<div style="display:flex;height:36px;border-radius:8px;'
                             'overflow:hidden;margin-bottom:10px;">')
                for seg, c, n, seg_fbi, fbi_share in _conc_rows:
                    if fbi_share <= 0:
                        continue
                    _lbl = f'{fbi_share:.0f}%' if fbi_share >= 7 else ''
                    _conc_bar += (
                        f'<div style="width:{fbi_share:.2f}%;background:{c};display:flex;'
                        f'align-items:center;justify-content:center;color:#FFFFFF;'
                        f'font-weight:var(--fw-bold);font-size:12px;">{_lbl}</div>'
                    )
                _conc_bar += '</div>'
                _conc_leg = '<div style="display:flex;gap:20px;flex-wrap:wrap;">'
                for seg, c, n, seg_fbi, fbi_share in _conc_rows:
                    _conc_leg += (
                        f'<div style="display:flex;align-items:center;gap:7px;font-size:12px;'
                        f'color:{_pp3["TEXT_SEC"]};">'
                        f'<span style="width:11px;height:11px;border-radius:3px;'
                        f'background:{c};display:inline-block;"></span>'
                        f'<b style="color:{_pp3["TEXT_PRI"]};">{seg}</b> · '
                        f'{fmt_currency_idr(seg_fbi)} · {n} merchants</div>'
                    )
                _conc_leg += '</div>'
                st.markdown(_conc_bar + _conc_leg, unsafe_allow_html=True)
                st.caption(
                    "Each tier's slice of total portfolio fee income — a short "
                    "bar means few merchants carry most of the revenue."
                )
            else:
                st.info("Fee income data unavailable for the current selection.")

            # ── Tier Map — Volume × Growth quadrants (full width) ───────────────
            # 2D map: log-X volume × Y growth, sized by FBI, divided into named
            # quadrants (STARS / CASH COWS / EMERGING / AT-RISK) by reference
            # lines at the portfolio median volume and growth = 0.
            section_label("Tier Map — Volume vs Growth (Log Scale)")
            _tm = df_f.copy()
            _tm = _tm[(_tm['AVG_SV'] > 0)]  # log axis needs positive x
            _median_sv = float(_tm['AVG_SV'].median()) if not _tm.empty else 0.0

            fig_sc = px.scatter(
                _tm,
                x='AVG_SV', y='SV_GROWTH_CLIPPED',
                color='CLUSTER', size='AVG_FBI', size_max=30,
                hover_name='MERCHANT_GROUP',
                hover_data={'PM': True, 'ACHIEVEMENT_PCT': ':.1f',
                            'AVG_SV': ':,.0f', 'AVG_FBI': ':,.0f',
                            'SV_GROWTH_CLIPPED': ':.2f'},
                color_discrete_map=color_lookup,
                log_x=True,
                labels={'AVG_SV': 'Monthly Volume (IDR, log)',
                        'SV_GROWTH_CLIPPED': 'Growth Trend'},
            )

            # Quadrant reference lines + corner labels for the four named zones.
            fig_sc.add_hline(y=0, line_dash='dot', line_color=TEXT_SEC, opacity=0.5)
            if _median_sv > 0:
                fig_sc.add_vline(x=_median_sv, line_dash='dot', line_color=TEXT_SEC, opacity=0.5)
            # Annotations are positioned by paper-coords so they sit in the
            # corners of the plot regardless of data range.
            fig_sc.add_annotation(xref='paper', yref='paper', x=0.99, y=0.97,
                                  text="<b>STARS</b><br>high vol · growing",
                                  showarrow=False, align='right',
                                  font=dict(size=10, color=SUCCESS), opacity=0.85)
            fig_sc.add_annotation(xref='paper', yref='paper', x=0.99, y=0.03,
                                  text="<b>CASH COWS</b><br>high vol · flat/decline",
                                  showarrow=False, align='right',
                                  font=dict(size=10, color=INFO), opacity=0.85)
            fig_sc.add_annotation(xref='paper', yref='paper', x=0.01, y=0.97,
                                  text="<b>EMERGING</b><br>low vol · growing",
                                  showarrow=False, align='left',
                                  font=dict(size=10, color=WARNING), opacity=0.85)
            fig_sc.add_annotation(xref='paper', yref='paper', x=0.01, y=0.03,
                                  text="<b>AT-RISK</b><br>low vol · declining",
                                  showarrow=False, align='left',
                                  font=dict(size=10, color=DANGER), opacity=0.85)

            fig_sc.update_layout(
                height=480, margin=dict(l=0, r=0, b=48, t=30),
                **_chart_base(),
                xaxis=_xaxis(),
                yaxis=_yaxis(),
            )
            st.plotly_chart(fig_sc, use_container_width=True, theme=None)

            # ── Per-tier merchant lists ─────────────────────────────────────────
            # Explicit "who is in each tier" view — one sub-tab per tier so every
            # dynamic tier's merchant list is visible at a glance, in rank order.
            section_label("Merchants in Each Tier")
            _present = df_f['CLUSTER'].unique()
            tiers_present = (
                [t for t in _TIER_RANK if t in _present]
                + [c for c in _present if c not in _TIER_RANK]
            )

            if not tiers_present:
                st.info("No merchants match the current filters.")
            else:
                tier_tabs = st.tabs([
                    f"{t} ({len(df_f[df_f['CLUSTER'] == t])})" for t in tiers_present
                ])
                for _tt, _tier in zip(tier_tabs, tiers_present):
                    with _tt:
                        _seg = (df_f[df_f['CLUSTER'] == _tier]
                                .sort_values('AVG_SV', ascending=False)
                                .reset_index(drop=True))

                        # Essentials-only view with friendly headers. Columns stay
                        # numeric underneath so interactive sorting works; Styler
                        # handles display formatting via the shared helpers.
                        _rename = {
                            'MERCHANT_GROUP':  'Merchant',
                            'PM':              'PM',
                            'AVG_SV':          'Monthly Volume',
                            'ACHIEVEMENT_PCT': 'Achievement',
                            'RISK_SCORE':      'Risk Score',
                        }
                        _disp_cols = [c for c in _rename if c in _seg.columns]
                        _disp = _seg[_disp_cols].rename(columns=_rename)
                        _tier_fmt = {
                            'Monthly Volume': fmt_currency_idr,
                            'Achievement':    lambda x: fmt_pct(x, decimals=1, scale=False),
                            'Risk Score':     lambda x: f"{x:.1f}" if pd.notna(x) else "—",
                        }
                        _tier_fmt = {k: v for k, v in _tier_fmt.items() if k in _disp.columns}
                        st.dataframe(
                            _disp.style.format(_tier_fmt),
                            use_container_width=True, hide_index=True,
                        )

                        # CSV keeps the CLUSTER column so the export is self-describing.
                        _csv_cols = [c for c in ['MERCHANT_GROUP', 'PM', 'CLUSTER',
                                                 'AVG_SV', 'ACHIEVEMENT_PCT', 'RISK_SCORE']
                                     if c in _seg.columns]
                        st.download_button(
                            "Download CSV",
                            _seg[_csv_cols].to_csv(index=False, encoding='utf-8-sig'),
                            f"merchants_{_tier.lower()}.csv", "text/csv",
                            key=f"t3_tier_csv_{_tier}",
                        )

            # ── Cluster Diagnostics — methodology appendix (collapsed) ──────────
            with st.expander("Cluster Diagnostics — Methodology"):
                # ── How K was chosen — live Elbow + Silhouette sweep ───────────
                # Directly answers "why this K?": the sweep runs on the same scaled
                # six-feature space K-Means uses, on every load, and K is selected
                # dynamically at the Silhouette peak within the operable band [2,5].
                section_label("Choosing K — Elbow Method & Silhouette Sweep")
                _kd = _k_diagnostics(df_card, df_mon, df_target)
                if _kd and _kd.get("k_values"):
                    _ks, _inr, _sil = _kd["k_values"], _kd["inertia"], _kd["silhouette"]
                    fig_k = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_k.add_trace(
                        go.Scatter(x=_ks, y=_inr, name="Inertia (WCSS)",
                                   mode="lines+markers",
                                   line=dict(color=INFO, width=2.5), marker=dict(size=8)),
                        secondary_y=False)
                    fig_k.add_trace(
                        go.Scatter(x=_ks, y=_sil, name="Silhouette",
                                   mode="lines+markers",
                                   line=dict(color=WARNING, width=2.5),
                                   marker=dict(size=8, symbol="square")),
                        secondary_y=True)
                    _chosen = _kd.get("chosen_k", _kd.get("k_silhouette"))
                    fig_k.add_vline(x=_chosen, line_dash="dash",
                                    line_color=DANGER, opacity=0.7)
                    fig_k.add_annotation(x=_chosen, yref="paper", y=1.04,
                                         text=f"K={_chosen} (selected)", showarrow=False,
                                         font=dict(color=DANGER, size=11))
                    fig_k.update_xaxes(title_text="Number of Clusters (K)",
                                       tickmode="array", tickvals=_ks, **_xaxis())
                    fig_k.update_yaxes(title_text="Inertia (WCSS)", secondary_y=False, **_yaxis())
                    fig_k.update_yaxes(title_text="Silhouette", secondary_y=True,
                                       showgrid=False, color=_p()['TEXT_SEC'])
                    fig_k.update_layout(height=380, margin=dict(l=0, r=0, b=40, t=30),
                                        legend=dict(orientation="h", y=-0.22), **_chart_base())
                    st.plotly_chart(fig_k, use_container_width=True, theme=None)
                    st.caption(
                        f"K swept from 2 to {max(_ks)} on the same standardized six-feature space "
                        f"K-Means clusters on. The **inertia (WCSS)** curve bends at K={_kd['k_elbow']} "
                        f"(the *elbow* — adding tiers past it barely tightens clusters), while the "
                        f"**Silhouette** peaks at K={_kd['k_silhouette']}. The portfolio is operated at "
                        f"**K={_chosen}**, selected dynamically at the Silhouette peak within the "
                        f"operable band [2, 5] — a dynamic tiering mechanism the PM team validated."
                    )
                else:
                    st.info("Not enough merchants to run the K-selection sweep — at least 3 are required.")

                # Cohesion — Silhouette Score + Davies-Bouldin Index
                section_label("Cluster Cohesion — How Trustworthy Are These Tiers?")
                if {'SILHOUETTE_SCORE', 'DB_SCORE'}.issubset(df_ml.columns) and len(df_ml) >= N_CLUSTERS:
                    sil = float(df_ml['SILHOUETTE_SCORE'].iloc[0])
                    dbi = float(df_ml['DB_SCORE'].iloc[0])

                    # Silhouette: −1..1, higher = better
                    if   sil > 0.5:  sil_q, sil_c = "Strong",   SUCCESS
                    elif sil > 0.25: sil_q, sil_c = "Moderate", WARNING
                    else:            sil_q, sil_c = "Weak",     DANGER
                    # Davies-Bouldin: 0+, lower = better
                    if   dbi < 0.8:  dbi_q, dbi_c = "Strong",   SUCCESS
                    elif dbi < 1.5:  dbi_q, dbi_c = "Moderate", WARNING
                    else:            dbi_q, dbi_c = "Weak",     DANGER

                    _cohesion_html = '<div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap;">'
                    for _title, _val, _q, _c, _scale in [
                        ("Silhouette Score",     f"{sil:.3f}", sil_q, sil_c, "Range −1 to 1 · higher is better"),
                        ("Davies-Bouldin Index", f"{dbi:.3f}", dbi_q, dbi_c, "Range 0 and up · lower is better"),
                    ]:
                        _cohesion_html += (
                            f'<div style="flex:1;min-width:220px;border-left:5px solid {_c};'
                            f'background:{_c}14;border-radius:0 14px 14px 0;padding:16px 18px;">'
                            f'<div class="kpi-label">{_title}</div>'
                            f'<div class="kpi-value" style="margin:6px 0 4px;color:{_c};">{_val}</div>'
                            f'<div style="display:inline-block;background:{_c};color:#FFFFFF;'
                            f'font-weight:var(--fw-semibold);font-size:11px;letter-spacing:0.04em;'
                            f'padding:3px 12px;border-radius:999px;">{_q.upper()}</div>'
                            f'<div class="kpi-meta" style="margin-top:8px;">{_scale}</div>'
                            f'</div>'
                        )
                    _cohesion_html += '</div>'
                    st.markdown(_cohesion_html, unsafe_allow_html=True)
                    st.caption(
                        "**Cluster cohesion** tells you how trustworthy the 3 merchant tiers are. "
                        "The **Silhouette Score** checks whether each merchant sits comfortably inside "
                        "its own tier rather than near a neighbouring one — *higher is better*. The "
                        "**Davies-Bouldin Index** checks how much the tiers overlap each other — "
                        "*lower is better*. When both look healthy, the tiers are genuinely distinct "
                        "groups, not arbitrary cut-offs."
                    )
                else:
                    st.info("Not enough merchants to evaluate cluster cohesion — at least 3 are required.")

                # Tier Separation (PCA projection) — 6 clustering features
                # compressed onto 2 axes; each tier shows a centroid (diamond)
                # and a ±1σ cohesion ellipse.
                section_label("Tier Separation (PCA Projection)")
                _pca = df_f[(df_f['PCA_X'] != 0) | (df_f['PCA_Y'] != 0)].copy()
                if _pca.empty:
                    st.info("Tier separation plot unavailable — at least 3 merchants are required.")
                else:
                    _v1 = float(df_ml['PCA_VAR1'].iloc[0])
                    _v2 = float(df_ml['PCA_VAR2'].iloc[0])
                    fig_pca = px.scatter(
                        _pca, x='PCA_X', y='PCA_Y',
                        color='CLUSTER', color_discrete_map=color_lookup,
                        hover_name='MERCHANT_GROUP',
                        hover_data={'PM': True, 'AVG_SV': ':,.0f',
                                    'PCA_X': False, 'PCA_Y': False, 'CLUSTER': False},
                        labels={'PCA_X': f'Component 1 ({_v1:.0f}% of variance)',
                                'PCA_Y': f'Component 2 ({_v2:.0f}% of variance)'},
                    )
                    fig_pca.update_traces(marker=dict(size=11, opacity=0.85,
                                                      line=dict(width=1, color='#FFFFFF')))

                    # Per-tier cohesion ellipse (±1 std) + centroid marker.
                    for _cl in all_clusters:
                        _g = _pca[_pca['CLUSTER'] == _cl]
                        if _g.empty:
                            continue
                        _cx, _cy = float(_g['PCA_X'].mean()), float(_g['PCA_Y'].mean())
                        _sx = float(_g['PCA_X'].std(ddof=0)) or 0.30
                        _sy = float(_g['PCA_Y'].std(ddof=0)) or 0.30
                        _col = color_lookup.get(_cl, '#888888')
                        fig_pca.add_shape(
                            type='circle', xref='x', yref='y',
                            x0=_cx - _sx, x1=_cx + _sx, y0=_cy - _sy, y1=_cy + _sy,
                            line=dict(color=_col, width=1, dash='dot'),
                            fillcolor=_col, opacity=0.10, layer='below',
                        )
                        fig_pca.add_trace(go.Scatter(
                            x=[_cx], y=[_cy], mode='markers',
                            marker=dict(symbol='diamond', size=16, color=_col,
                                        line=dict(width=2, color='#FFFFFF')),
                            name=f'{_cl} center', hoverinfo='skip', showlegend=False,
                        ))

                    fig_pca.update_layout(
                        height=450, margin=dict(l=0, r=0, b=48, t=20),
                        legend=dict(orientation='h', y=-0.18),
                        **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis(),
                    )
                    st.plotly_chart(fig_pca, use_container_width=True, theme=None)
                    st.caption(
                        "The 6 clustering features compressed onto 2 axes. Tight, "
                        "non-overlapping point clouds mean the tiers are well separated; "
                        "each diamond marks a tier centre with a ±1σ cohesion ellipse."
                    )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CHURN & RISK
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    tab_desc("Proactive health monitoring for your merchant portfolio. Merchants needing attention are surfaced here based on volume trends, growth trajectory, and target achievement — so your team always knows where to focus.")

    if not (has_card and has_mon):
        st.warning("Health alerts require both Card Share and Monitoring data.")
    else:
        z_thresh_val = Z_THRESH  # detection threshold locked (no longer user-adjustable)

        df_churn_all = run_ml(df_card, df_mon, df_target)
        
        if df_churn_all.empty:
            st.info("No data available for Churn and Risk analysis.")
        else:
            all_pm_c = sorted(df_churn_all['PM'].dropna().unique().tolist()) if 'PM' in df_churn_all.columns else []

            # Controls — inline
            ch1, ch2 = st.columns([3,1])
            with ch1:
                sel_pm_c = st.multiselect("Filter by PM", all_pm_c, default=all_pm_c, key="t4_pm")
            with ch2:
                risk_view = st.radio("Show", ["All", "High Risk Only", "Stable Only"], key="t4_risk")

            df_c4 = df_churn_all.copy()
            if sel_pm_c and 'PM' in df_c4.columns:
                df_c4 = df_c4[df_c4['PM'].isin(sel_pm_c)]

            churn_mask = df_c4['CHURN_RISK'].str.contains("HIGH", na=False)
            if risk_view == "High Risk Only":
                df_c4 = df_c4[churn_mask]
                filter_pill(f"Filter Active: High Risk Only — {len(df_c4)} merchants shown")
            elif risk_view == "Stable Only":
                df_c4 = df_c4[~churn_mask]
                filter_pill(f"Filter Active: Stable Only — {len(df_c4)} merchants shown")

            df_high   = df_c4[df_c4['CHURN_RISK'] == 'HIGH RISK']
            df_medium = df_c4[df_c4['CHURN_RISK'] == 'MEDIUM RISK']
            df_safe   = df_c4[df_c4['CHURN_RISK'] == 'STABLE']
            total     = len(df_c4)

            # KPI — rate based on HIGH + MEDIUM combined (overall at-risk)
            rate = (len(df_high) + len(df_medium)) / total * 100 if total > 0 else 0
            st.markdown(f"""<div class="stats-grid" style="grid-template-columns:repeat(4,1fr);">
                <div class="stat-card red">
                    <div class="stat-label">Action Required</div>
                    <div class="stat-value">{len(df_high)}</div>
                    <div class="stat-meta">immediate follow-up</div>
                </div>
                <div class="stat-card amber">
                    <div class="stat-label">Monitor Closely</div>
                    <div class="stat-value">{len(df_medium)}</div>
                    <div class="stat-meta">proactive check-in</div>
                </div>
                <div class="stat-card green">
                    <div class="stat-label">On Track</div>
                    <div class="stat-value">{len(df_safe)}</div>
                    <div class="stat-meta">performing well</div>
                </div>
                <div class="stat-card blue">
                    <div class="stat-label">Needs Attention</div>
                    <div class="stat-value">{rate:.1f}%</div>
                    <div class="stat-meta">of portfolio</div>
                </div>
            </div>""", unsafe_allow_html=True)

            with st.expander("Portfolio Health Overview", expanded=False):
                if total > 0:
                    # ── Risk Score Distribution ───────────────────────────────────────
                    if 'RISK_SCORE' in df_c4.columns:
                        section_label("Portfolio Health Distribution")
                        _df_c4_disp = df_c4.copy()
                        if 'CHURN_RISK' in _df_c4_disp.columns:
                            _df_c4_disp['Health Status'] = _df_c4_disp['CHURN_RISK'].replace({
                                'HIGH RISK':   'Action Required',
                                'MEDIUM RISK': 'Monitor Closely',
                                'STABLE':       'On Track',
                            })
                        else:
                            _df_c4_disp['Health Status'] = 'Unknown'
                        fig_rs = px.histogram(
                            _df_c4_disp, x='RISK_SCORE', color='Health Status', nbins=20,
                            barmode='overlay',
                            color_discrete_map={
                                'Action Required': '#C0392B',
                                'Monitor Closely': '#F59E0B',
                                'On Track':        '#27AE60',
                            },
                            labels={'RISK_SCORE': 'Health Score (0–100, higher = more attention needed)', 'count': 'Merchants'},
                            title='Merchant Health Score Distribution Across Portfolio'
                        )
                        fig_rs.add_vline(x=30, line_dash='dash', line_color='#F59E0B',
                                         annotation_text='Medium threshold (30)',
                                         annotation_font_color='#F59E0B',
                                         annotation_position='top left')
                        fig_rs.add_vline(x=60, line_dash='dash', line_color='#C0392B',
                                         annotation_text='High threshold (60)',
                                         annotation_font_color='#C0392B',
                                         annotation_position='top left')
                        fig_rs.update_layout(height=300, showlegend=True,
                                             margin=dict(l=48, r=16, t=50, b=52),
                                             **_chart_base(),
                                             xaxis={**_xaxis(), 'range': [0, 100]},
                                             yaxis=_yaxis())
                        st.caption(
                            "**What is the Health Score?** A composite 0–100 score based on three business signals: "
                            "transaction volume trend, fee income consistency, and how far the merchant is from their annual target. "
                            "Merchants scoring **60+** need immediate outreach. **30–60** warrants a proactive check-in. "
                            "**Below 30** means they are on track."
                        )
                        st.plotly_chart(fig_rs, use_container_width=True, theme=None)
                        st.markdown("")

                    # ── Gauge chart — churn rate speedometer ─────────────────────────
                    _pp4 = _p()
                    gauge_col, ch_right_kpi = st.columns([1, 1])
                    with gauge_col:
                        bar_color = "#34D399" if rate < 20 else ("#FBBF24" if rate < 45 else "#F87171")
                        fig_gauge = go.Figure(go.Indicator(
                            mode="gauge+number+delta",
                            value=rate,
                            number={"suffix": "%", "font": {"size": 44, "color": _pp4["TEXT_PRI"]}},
                            delta={"reference": 20, "relative": False,
                                   "increasing": {"color": "#F87171"},
                                   "decreasing": {"color": "#34D399"},
                                   "suffix": "% vs 20% target",
                                   "font": {"size": 13},
                                   "valueformat": ".1f"},
                            # domain: arc uses top 68%, number+delta sit in the bottom 32%
                            domain={"x": [0, 1], "y": [0.32, 1]},
                            gauge={
                                "axis": {
                                    "range": [0, 100],
                                    "tickwidth": 1,
                                    "tickcolor": _pp4["TEXT_SEC"],
                                    "tickfont": {"color": _pp4["TEXT_SEC"], "size": 10},
                                },
                                "bar": {"color": bar_color, "thickness": 0.28},
                                "bgcolor": "rgba(0,0,0,0)",
                                "borderwidth": 0,
                                "steps": [
                                    {"range": [0, 20],  "color": "rgba(52,211,153,0.12)"},
                                    {"range": [20, 45], "color": "rgba(251,191,36,0.12)"},
                                    {"range": [45, 100],"color": "rgba(248,113,113,0.12)"},
                                ],
                                "threshold": {
                                    "line": {"color": "#F87171", "width": 3},
                                    "thickness": 0.8,
                                    "value": 45,
                                },
                            },
                            title={"text": "Portfolio Churn Rate", "font": {"size": 13, "color": _pp4["TEXT_SEC"]}},
                        ))
                        fig_gauge.update_layout(
                            height=360,
                            margin=dict(l=20, r=20, t=40, b=20),
                            paper_bgcolor="rgba(0,0,0,0)",
                            font_color=_pp4["TEXT_PRI"],
                            # Zone labels placed inside each colored zone on the arc
                            annotations=[
                                dict(x=0.15, y=0.44, text="<b>LOW</b>",    showarrow=False,
                                     xref="paper", yref="paper",
                                     font=dict(color="#34D399", size=10)),
                                dict(x=0.34, y=0.60, text="<b>MED</b>",    showarrow=False,
                                     xref="paper", yref="paper",
                                     font=dict(color="#FBBF24", size=10)),
                                dict(x=0.75, y=0.57, text="<b>HIGH</b>",   showarrow=False,
                                     xref="paper", yref="paper",
                                     font=dict(color="#F87171", size=10)),
                            ],
                        )
                        st.plotly_chart(fig_gauge, use_container_width=True, theme=None)
                        st.caption(
                            "**How to read this:** The gauge shows what percentage of your merchant fleet is currently "
                            "flagged as high-risk. The number below the gauge (+/− X%) is how far you are from the "
                            "20% portfolio target — negative means fewer at-risk merchants (good), positive means more (needs action)."
                        )

                    with ch_right_kpi:
                        risk_label = "FLEET HEALTHY" if rate < 20 else ("NEEDS ATTENTION" if rate < 45 else "CRITICAL — ACT NOW")
                        risk_color = "#34D399" if rate < 20 else ("#FBBF24" if rate < 45 else "#F87171")

                        def churn_advisory(pct):
                            if pct >= 75:
                                return "**CRITICAL:** Immediate intervention required. Recommend emergency fee discounts, PM outreach blitz, and escalation to senior leadership."
                            elif pct >= 45:
                                return "**HIGH RISK:** Portfolio is deteriorating. Recommend targeted retention offers, dedicated PM follow-ups for flagged merchants, and weekly monitoring cadence."
                            elif pct >= 20:
                                return "**ELEVATED:** Above benchmark. Recommend proactive check-ins with declining merchants and review of competitive positioning."
                            else:
                                return "**STABLE:** Portfolio churn is within healthy benchmarks. Continue standard monitoring and quarterly business reviews."

                        advisory = churn_advisory(rate)
                        st.markdown(
                            f"""<div style="margin-top:24px;padding:20px;border-radius:14px;
                                border:2px solid {risk_color};background:{risk_color}18;text-align:center;">
                                <div style="font-size:var(--fs-kpi);">{risk_label}</div>
                                <div style="font-size:var(--fs-sm);color:{_pp4['TEXT_SEC']};margin-top:10px;">
                                {len(df_high)} of {total} merchants flagged as high-risk.<br>
                                Benchmark target: &lt;20% portfolio churn.
                                </div>
                            </div>""", unsafe_allow_html=True
                        )
                        st.markdown(
                            f"""<div style="margin-top:12px;padding:14px 16px;border-radius:10px;
                                background:{_pp4['SURFACE2']};border:1px solid {_pp4['BORDER']};
                                font-size:var(--fs-sm);color:{_pp4['TEXT_PRI']};line-height:1.55;">
                                <b>AI Recommendation:</b><br>{advisory}
                            </div>""", unsafe_allow_html=True
                        )

                    # ── Chart Data Audit ──────────────────────────────────────────────
                    with st.expander("Chart Data Audit", expanded=False):
                        st.caption("Raw aggregates feeding the gauge and donut charts:")
                        audit_data = {
                            "Metric": ["High Risk Count", "Stable Count", "Total", "Churn Rate %"],
                            "Value": [str(len(df_high)), str(len(df_safe)), str(total), f"{rate:.2f}%"],
                        }
                        st.dataframe(pd.DataFrame(audit_data), hide_index=True, use_container_width=True)
                        if 'CHURN_RISK' in df_c4.columns:
                            st.write("CHURN_RISK value_counts:")
                            st.dataframe(df_c4['CHURN_RISK'].value_counts().reset_index(), hide_index=True)

                    # ── Donut + PM bar ────────────────────────────────────────────────
                    if 'RISK_SCORE' in df_c4.columns:
                        ch_x, ch_y = st.columns(2, gap="large")
                        with ch_x:
                            fig_rc = px.pie(_df_c4_disp, names='Health Status',
                                            color='Health Status',
                                            color_discrete_map={'Action Required':'#C0392B','Monitor Closely':'#F59E0B','On Track':'#27AE60'},
                                            hole=0.4, title="Portfolio Health Breakdown")
                            fig_rc.update_layout(height=350, **_chart_base())
                            st.plotly_chart(fig_rc, use_container_width=True, theme=None)
                        with ch_y:
                            if 'PM' in df_high.columns and len(df_high) > 0:
                                pm_churn = df_high.groupby('PM').size().reset_index(name='HIGH_RISK_COUNT')
                                fig_pc = px.bar(pm_churn.sort_values('HIGH_RISK_COUNT', ascending=False),
                                                x='PM', y='HIGH_RISK_COUNT',
                                                color='HIGH_RISK_COUNT', color_continuous_scale='Reds',
                                                title="High-Risk Merchants per PM")
                                fig_pc.update_layout(height=350, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                                st.plotly_chart(fig_pc, use_container_width=True, theme=None)

            # ── Action Inbox ──────────────────────────────────────────────────
            # Plan §4.2 — render each at-risk merchant as a unified card with:
            # tier chip + PM owner | 12-week sparkline | reason+CTA. Sort by
            # composite urgency (risk_score + IF-confirmation + below-target
            # bonuses) so multi-method-confirmed alerts bubble to the top.
            _at_risk_inbox = pd.concat([df_high, df_medium], ignore_index=True)
            if not _at_risk_inbox.empty and 'RISK_SCORE' in _at_risk_inbox.columns:
                _at_risk_inbox = _at_risk_inbox.copy()
                _at_risk_inbox['_URGENCY'] = compose_urgency_score(
                    _at_risk_inbox['RISK_SCORE'].astype(float),
                    achievement_pct=_at_risk_inbox.get('ACHIEVEMENT_PCT', pd.Series(dtype=float)),
                    is_iforest_anomaly=_at_risk_inbox.get('IF_IS_ANOMALY', pd.Series(dtype=bool)),
                ).values
                _at_risk_sorted = _at_risk_inbox.sort_values('_URGENCY', ascending=False)

                # Plan F1 — persisted triage. Merchants the user acknowledged
                # or snoozed drop out of the queue (snoozes resurface on expiry).
                # The ML analytics are untouched — only what's shown here changes.
                _triage_map = app_state.active_triage_map(engine=engine)
                _is_triaged = _at_risk_sorted['MERCHANT_GROUP'].astype(str).isin(_triage_map)
                _triaged_rows = _at_risk_sorted[_is_triaged]
                _open_rows    = _at_risk_sorted[~_is_triaged]

                _show_triaged = st.toggle(
                    "Show acknowledged / snoozed merchants",
                    value=False, key="t4_show_triaged",
                    help="Triaged merchants are hidden from the queue. Turn this on to review or restore them.",
                )
                _at_risk_inbox = (_at_risk_sorted if _show_triaged else _open_rows).head(7)

                section_label(f"Action Inbox — {len(_open_rows)} merchant(s) need attention")
                _cap = ("Sorted by composite urgency (risk score + Isolation-Forest "
                        "confirmation + below-target bonus). Acknowledge or snooze a "
                        "merchant to clear it from the queue.")
                if len(_triaged_rows) > 0:
                    _cap += f"  &middot;  {len(_triaged_rows)} merchant(s) currently triaged."
                st.caption(_cap)

                # Pre-filter the weekly frame ONCE to VOL rows so each card's
                # sparkline lookup scans a small frame, not the full weekly table.
                _wk_vol = (df_mon_weekly[df_mon_weekly['DIMENSI'] == 'VOL']
                           if has_mon_weekly and not df_mon_weekly.empty else None)

                for _idx, (_, row) in enumerate(_at_risk_inbox.iterrows()):
                    _merchant = row.get('MERCHANT_GROUP', '—')
                    _cr = row.get('CHURN_RISK', '')
                    _pm_name = row.get('PM', 'N/A')
                    _is_high = 'HIGH' in str(_cr)
                    _is_if   = bool(row.get('IF_IS_ANOMALY', False))
                    _ach     = float(row.get('ACHIEVEMENT_PCT', 0) or 0)
                    _growth  = float(row.get('SV_GROWTH_RATE', 0) or 0)
                    _cluster = row.get('CLUSTER', '')
                    _row_color = DANGER if _is_high else WARNING

                    if _is_high and _is_if:
                        _reason = "Flagged by 2 independent detection methods — highest confidence alert"
                        _action = "Escalate to PM immediately; schedule merchant call this week"
                    elif _is_high:
                        _reason = f"Volume or growth significantly below fleet average (Achievement: {fmt_pct(_ach, decimals=0, scale=False)})"
                        _action = "PM to conduct business review; investigate operational issues"
                    elif _ach < 60:
                        _reason = f"Below 60% of yearly target (Achievement: {fmt_pct(_ach, decimals=0, scale=False)})"
                        _action = "Schedule business review; consider promotional support"
                    else:
                        _reason = f"Growth trend declining (MoM: {fmt_growth(_growth, decimals=1, scale=True)})"
                        _action = "Monitor weekly; check for competitive pressure"

                    # 12-week VOL sparkline (falls back gracefully if no weekly data).
                    _sparkline = []
                    if _wk_vol is not None:
                        try:
                            _sparkline = extract_recent_weeks(
                                _wk_vol, merchant=_merchant, dimensi='VOL',
                                n_weeks=12,
                            )
                        except Exception:
                            _sparkline = []

                    # ── Notification-inbox accordion row (Plan Fix 3) ──────────
                    # Each alert is one st.expander inside a keyed container.
                    # Severity drives BOTH the collapsed-label badge and the
                    # container-key suffix the CSS hooks for the colored left
                    # accent bar (red = high risk, amber = watch). Collapsing the
                    # detail keeps the mobile viewport clean — tap to reveal.
                    if _is_high:
                        _sev, _sev_badge = "high", ":red-background[HIGH RISK]"
                    elif _ach < 60:
                        _sev, _sev_badge = "watch", ":orange-background[BELOW TARGET]"
                    else:
                        _sev, _sev_badge = "watch", ":orange-background[WATCH]"
                    _multi = "  :red-background[2-METHOD]" if (_is_high and _is_if) else ""

                    # One-line summary shown when the row is collapsed.
                    _label = (
                        f"{_sev_badge}{_multi}  **{_merchant}**  ·  "
                        f"{_cluster or 'UNCLASSIFIED'}  ·  PM {_pm_name}"
                    )

                    with st.container(key=f"inbox_row_{_sev}_{_idx}"):
                        # Top (most-urgent) alert opens by default; the rest stay
                        # collapsed so the inbox reads like a notification list.
                        with st.expander(_label, expanded=(_idx == 0)):
                            # Reason + recommended action (full width).
                            st.markdown(
                                f"<div style='color:var(--btn-text-sec);"
                                f"font-size:var(--fs-sm);'>{_reason}</div>"
                                f"<div style='margin-top:4px;color:{_row_color};"
                                f"font-weight:var(--fw-semibold);font-size:var(--fs-sm);'>"
                                f"→ {_action}"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                            _b1, _b2 = st.columns([3, 2])
                            with _b1:
                                if _sparkline and len(_sparkline) >= 2:
                                    # Plotly only accepts 6-digit hex / named / rgba —
                                    # NOT 8-digit hex with alpha. Convert via helper.
                                    _spark = go.Figure(go.Scatter(
                                        x=list(range(len(_sparkline))),
                                        y=_sparkline,
                                        mode='lines',
                                        line=dict(color=_row_color, width=2),
                                        fill='tozeroy',
                                        fillcolor=hex_to_rgba(_row_color, 0.15),
                                        hovertemplate='Week %{x}: %{y:,.0f}<extra></extra>',
                                    ))
                                    _spark.update_layout(
                                        height=70, margin=dict(l=0, r=0, t=0, b=0),
                                        showlegend=False,
                                        xaxis=dict(visible=False),
                                        yaxis=dict(visible=False),
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                    )
                                    st.plotly_chart(
                                        _spark, use_container_width=True, theme=None,
                                        config={'displayModeBar': False, 'staticPlot': True},
                                    )
                                    st.caption(f"VOL last {len(_sparkline)} weeks")
                                else:
                                    st.caption("No weekly trend data")
                            with _b2:
                                try:
                                    st.page_link(
                                        "pages/05_PM_Manager.py",
                                        label="Open in PM Manager →",
                                        icon=":material/arrow_forward:",
                                    )
                                except Exception:
                                    # Older Streamlit versions: graceful fallback.
                                    st.markdown(
                                        "<div style='color:var(--btn-text-sec);"
                                        "font-size:var(--fs-xs);'>"
                                        "Open the PM Manager page</div>",
                                        unsafe_allow_html=True,
                                    )
                                # Plan F1 — triage controls. Persisted via app_state
                                # so a decision survives reruns, sessions, and the
                                # next pipeline run. Keys are sanitized merchant names.
                                _safe_key = "".join(
                                    c if c.isalnum() else "_" for c in str(_merchant)
                                )
                                if _merchant in _triage_map:
                                    st.caption(f"Triaged &middot; {_triage_map[_merchant]}")
                                    if st.button("Restore to queue",
                                                 key=f"untri_{_safe_key}",
                                                 use_container_width=True):
                                        app_state.clear_triage(_merchant, engine=engine)
                                        st.rerun()
                                else:
                                    _ack, _snz = st.columns(2)
                                    if _ack.button("Acknowledge", key=f"ack_{_safe_key}",
                                                   use_container_width=True,
                                                   help="Mark as reviewed — clears it from the queue."):
                                        app_state.set_triage(
                                            _merchant, app_state.TRIAGE_ACKNOWLEDGED,
                                            engine=engine)
                                        st.rerun()
                                    if _snz.button("Snooze 14d", key=f"snz_{_safe_key}",
                                                   use_container_width=True,
                                                   help="Hide for 14 days, then resurface automatically."):
                                        app_state.set_triage(
                                            _merchant, app_state.TRIAGE_SNOOZED,
                                            snooze_until=date.today() + timedelta(days=14),
                                            engine=engine)
                                        st.rerun()

            st.markdown("")

            if total > 0:

                if 'ZSCORE_SV' in df_c4.columns:
                    with st.expander("Statistical Detail — Volume, Fee & Growth Outlier Analysis", expanded=False):
                        st.caption("These charts show the distribution of merchant performance metrics. Red-shaded merchants fall below the detection threshold set in Advanced Settings.")
                        z1, z2, z3 = st.columns(3)

                        def _draw_z_hist(df, col_name, title, threshold):
                            fig_z = px.histogram(df, x=col_name, color='CHURN_RISK',
                                                 nbins=25, barmode='overlay',
                                                 color_discrete_map={'HIGH RISK': RED, 'STABLE': BLUE_ACC},
                                                 title=title)
                            fig_z.add_vline(x=threshold, line_dash='dash', line_color=RED,
                                            annotation_text=f"Threshold ({threshold})",
                                            annotation_font_color=RED,
                                            annotation_position="top right")
                            fig_z.update_layout(
                                height=300, showlegend=False,
                                margin=dict(l=48, r=16, t=40, b=52),
                                **_chart_base(),
                                xaxis={**_xaxis(), "tickangle": -30},
                                yaxis=_yaxis(),
                            )
                            return fig_z

                        z1.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_SV', "Volume Spread", z_thresh_val), use_container_width=True, theme=None)
                        z2.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_FBI', "Fee Income Spread", z_thresh_val), use_container_width=True, theme=None)
                        z3.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_GROWTH', "Growth Spread", z_thresh_val), use_container_width=True, theme=None)

                # ── Fleet-Wide Anomaly Driver Analysis ────────────────────────
                _if_lofo_cols = {
                    'IF_CONTRIB_AVG_SV':      'Avg Settlement Volume',
                    'IF_CONTRIB_AVG_FBI':      'Avg Fee-Based Income',
                    'IF_CONTRIB_RASIO_ONUS':   'On-Us Ratio',
                    'IF_CONTRIB_SV_GROWTH':    'Volume Growth Rate',
                    'IF_CONTRIB_ACHIEVEMENT':  'Target Achievement %',
                    'IF_CONTRIB_WEEKS_ACTIVE': 'Activity Weeks',
                }
                _if_flagged = df_c4[df_c4.get('IF_IS_ANOMALY', pd.Series(False, index=df_c4.index)) == True] \
                    if 'IF_IS_ANOMALY' in df_c4.columns else pd.DataFrame()
                if not _if_flagged.empty and all(c in _if_flagged.columns for c in _if_lofo_cols):
                    section_label("What's Driving the Alerts? — Key Risk Factors Across Portfolio")
                    st.caption(f"Analyzing {len(_if_flagged)} flagged merchant(s). Higher bars = the metric most responsible for triggering alerts. Use this to guide where your team should focus.")

                    _fleet_lofo = _if_flagged[list(_if_lofo_cols.keys())].mean().rename(_if_lofo_cols)
                    _fleet_lofo_df = _fleet_lofo.reset_index()
                    _fleet_lofo_df.columns = ['Feature', 'Avg Contribution']
                    _fleet_lofo_df = _fleet_lofo_df.sort_values('Avg Contribution', ascending=True)

                    # Normalize to share-of-total-pressure so the axis reads as a
                    # human-friendly percentage instead of raw 4-decimal deltas.
                    _total_abs = float(_fleet_lofo_df['Avg Contribution'].abs().sum()) or 1.0
                    _fleet_lofo_df['Share'] = _fleet_lofo_df['Avg Contribution'] / _total_abs * 100

                    _fl_colors = [DANGER if v > 0 else SUCCESS for v in _fleet_lofo_df['Avg Contribution']]
                    fig_fleet_lofo = go.Figure(go.Bar(
                        x=_fleet_lofo_df['Share'],
                        y=_fleet_lofo_df['Feature'],
                        orientation='h',
                        marker=dict(color=_fl_colors, line_width=0, opacity=0.85),
                        text=[f"{v:+.1f}%" for v in _fleet_lofo_df['Share']],
                        textposition='outside',
                        textfont=dict(size=13, color=_p()['TEXT_PRI']),
                        cliponaxis=False,
                        customdata=_fleet_lofo_df['Avg Contribution'].values,
                        hovertemplate=(
                            '<b>%{y}</b><br>'
                            'Share of alert pressure: <b>%{x:+.1f}%</b><br>'
                            'Raw LOFO Δ: %{customdata:+.4f}'
                            '<extra></extra>'
                        ),
                    ))
                    _pp4b = _p()
                    _x_pad = max(abs(_fleet_lofo_df['Share'].min()), abs(_fleet_lofo_df['Share'].max())) * 1.25 + 5
                    fig_fleet_lofo.update_layout(
                        height=440,
                        margin=dict(l=160, r=80, t=20, b=40),
                        bargap=0.35,
                        xaxis=dict(
                            title=dict(text='Share of Alert Pressure (%)',
                                       font=dict(color=_pp4b['TEXT_SEC'], size=12)),
                            showgrid=False, zeroline=False,
                            range=[-_x_pad, _x_pad],
                            tickfont=dict(color=_pp4b['TEXT_SEC'], size=11),
                            ticksuffix='%',
                        ),
                        yaxis=dict(
                            showgrid=False, automargin=True,
                            tickfont=dict(color=_pp4b['TEXT_PRI'], size=13),
                        ),
                        **_chart_base(),
                    )
                    fig_fleet_lofo.add_vline(
                        x=0, line_color=_pp4b['TEXT_SEC'], line_width=1, opacity=0.4
                    )

                    # Subtle legend strip so the user reads color → meaning without
                    # the chart needing a separate Plotly legend.
                    st.markdown(
                        f'<div style="display:flex;gap:18px;align-items:center;'
                        f'font-size:var(--fs-xs);color:var(--btn-text-sec);'
                        f'margin:4px 0 10px;letter-spacing:0.3px;">'
                        f'<span><span style="display:inline-block;width:10px;height:10px;'
                        f'border-radius:2px;background:{DANGER};margin-right:6px;'
                        f'vertical-align:middle;"></span>Pushes toward alert</span>'
                        f'<span><span style="display:inline-block;width:10px;height:10px;'
                        f'border-radius:2px;background:{SUCCESS};margin-right:6px;'
                        f'vertical-align:middle;"></span>Pulls away from alert</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                    st.plotly_chart(fig_fleet_lofo, use_container_width=True, theme=None)

        # ── Weekly Activity Pulse — Sudden Drop Monitor ───────────────────────
        styled_divider()
        section_label("Weekly Activity Pulse — Sudden Drop Monitor")
        st.caption("Scans the most recent week of transaction data and flags any merchant whose volume suddenly crashed below their own 4-week rolling average. Use this to catch new problems the moment they appear — before they become structural health issues.")
        if not has_mon_weekly:
            st.info("Weekly drop monitoring requires Monitoring Weekly data to be processed first.")
        else:
            _sc_wk = df_mon_weekly[df_mon_weekly['YEAR'] == '2026'].copy() if not df_mon_weekly.empty else pd.DataFrame()
            _SC_W_COLS = sorted([c for c in _sc_wk.columns if c.startswith('W') and c[1:].isdigit()])
            if _sc_wk.empty or not _SC_W_COLS:
                st.info("No 2026 weekly data available yet.")
            else:
                _sc_latest_wk = 0
                for _w in reversed(_SC_W_COLS):
                    if _sc_wk[_w].fillna(0).sum() > 0:
                        _sc_latest_wk = int(_w[1:])
                        break
                if _sc_latest_wk < 5:
                    st.info("Not enough weeks logged yet to calculate a 4-week rolling average (need at least 5 weeks of data).")
                else:
                    _wk_curr = f"W{_sc_latest_wk:02d}"
                    _wk_hist = [f"W{_sc_latest_wk-i:02d}" for i in range(1, 5)]
                    _slider_drop = st.slider("Alert me when a merchant drops by more than:", 10, 80, 30, 5,
                                             format="%d%%", key="t4_drop_thresh",
                                             help="30% is a good starting point. Lower values will flag more merchants; higher values only catch severe drops.")
                    _threshold_pct = -1 * (_slider_drop / 100.0)
                    _df_scan = _sc_wk[['MERCHANT_GROUP', 'DIMENSI', 'YTD'] + _wk_hist + [_wk_curr]].copy()
                    _df_scan['4-Week Avg'] = _df_scan[_wk_hist].mean(axis=1)
                    _df_scan['This Week Change'] = np.where(
                        _df_scan['4-Week Avg'] > 0,
                        (_df_scan[_wk_curr] - _df_scan['4-Week Avg']) / _df_scan['4-Week Avg'], 0
                    )
                    _anomalies = _df_scan[(_df_scan['This Week Change'] <= _threshold_pct) & (_df_scan['4-Week Avg'] > 0)].copy()
                    _anomalies = _anomalies.sort_values('This Week Change', ascending=True)
                    if not _anomalies.empty:
                        st.warning(f"**{len(_anomalies)} merchant(s) dropped by {_slider_drop}%+ in {_wk_curr}** compared to their 4-week average.")

                        # Plan §3.6 — replace raw 6-decimal numbers (e.g. 30561.000000,
                        # 656135255.805000) with human-readable formats. Unit depends on
                        # the DIMENSI label per row: VOL/FBI → IDR currency, TRX → count.
                        def _fmt_by_dim(val, dim):
                            if pd.isna(val):
                                return "—"
                            d = str(dim).upper()
                            if d in ('VOL', 'FBI'):
                                return fmt_currency_idr(val)
                            return fmt_count(val)

                        _anom_disp = _anomalies[['MERCHANT_GROUP', 'DIMENSI', '4-Week Avg', _wk_curr, 'This Week Change']].copy()
                        _anom_disp['4-Week Avg'] = _anom_disp.apply(lambda r: _fmt_by_dim(r['4-Week Avg'], r['DIMENSI']), axis=1)
                        _anom_disp[_wk_curr]     = _anom_disp.apply(lambda r: _fmt_by_dim(r[_wk_curr],     r['DIMENSI']), axis=1)

                        # Keep `This Week Change` numeric so the Styler can both format
                        # AND apply a diverging red/green heatmap based on the actual value.
                        _styled_drop = (
                            _anom_disp.style
                            .format({'This Week Change': lambda x: fmt_growth(x, decimals=1, scale=True)})
                            .map(growth_cell_style, subset=['This Week Change'])
                        )
                        st.dataframe(_styled_drop, use_container_width=True, hide_index=True)
                    else:
                        st.success(f"No merchants dropped by {_slider_drop}%+ this week ({_wk_curr}). Portfolio activity looks stable.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    tab_desc(
        "Detects abnormal merchant transaction patterns using three complementary "
        "statistical methods (Threshold, Z-Score, Isolation Forest) to support "
        "fraud and anomaly monitoring."
    )

    @st.cache_data
    def _load_wm_anomaly():
        try:
            if not table_exists(engine, "WEEKLY_MONITOR"):
                return pd.DataFrame()
            df = pd.read_sql_query("SELECT * FROM weekly_monitor WHERE year=2026", engine)
            df.columns = [c.upper() for c in df.columns]
            _write_wm_anomaly_snapshot(df)
            return df
        except Exception:
            snap = _read_wm_anomaly_snapshot()
            return snap if snap is not None else pd.DataFrame()

    _df_wm = _load_wm_anomaly()

    if _df_wm.empty:
        st.info("Run the automated pipeline to populate data first.")
    else:
        # ── Method 1: Threshold ─────────────────────────────────────────────
        _pm_avg = _df_wm.groupby("MERCHANT_GROUP")["WEEKLY_VOL"].transform("mean")
        _thresh_mask = _df_wm["WEEKLY_VOL"] > 3 * _pm_avg
        _thresh_anom = _df_wm[_thresh_mask].copy()
        _thresh_anom["RATIO"] = _thresh_anom["WEEKLY_VOL"] / _pm_avg[_thresh_mask]

        # ── Method 2: Z-Score ───────────────────────────────────────────────
        _df_wm = _df_wm.copy()
        _df_wm["Z_SCORE"] = stats.zscore(_df_wm["WEEKLY_VOL"])
        _zscore_anom = _df_wm[_df_wm["Z_SCORE"].abs() > 3.0].copy()

        # ── Method 3: Isolation Forest ──────────────────────────────────────
        _if_feats = ["WEEKLY_VOL", "WEEKLY_TRX", "WEEKLY_FBI"]
        _if_X = StandardScaler().fit_transform(_df_wm[_if_feats])
        _if_preds = IsolationForest(contamination=0.02, random_state=42).fit_predict(_if_X)
        _if_anom = _df_wm[_if_preds == -1].copy()

        # ── KPI Cards ───────────────────────────────────────────────────────
        st.markdown(f"""<div class="stats-grid" style="grid-template-columns:repeat(3,1fr);">
            <div class="stat-card amber">
                <div class="stat-label">Threshold Flags</div>
                <div class="stat-value">{len(_thresh_anom)}</div>
                <div class="stat-meta">VOL &gt; 3&times; merchant avg</div>
            </div>
            <div class="stat-card red">
                <div class="stat-label">Z-Score Flags</div>
                <div class="stat-value">{len(_zscore_anom)}</div>
                <div class="stat-meta">|Z| &gt; 3.0 global</div>
            </div>
            <div class="stat-card blue">
                <div class="stat-label">Isolation Forest Flags</div>
                <div class="stat-value">{len(_if_anom)}</div>
                <div class="stat-meta">multivariate outliers</div>
            </div>
        </div>""", unsafe_allow_html=True)

        styled_divider()

        # Plan §2 declutter #6 — replace the three expanders (which forced users
        # to scroll through accordion blocks) with a single radio chip group.
        # Plan §3.2 — apply log axes + 2-layer marker encoding (small grey
        # normals + larger anomaly stars with white halo) + universal labels
        # on every flagged point so the eye doesn't have to map color-bar values.
        _method = st.radio(
            "Detection method",
            ["Threshold (VOL > 3× merchant avg)",
             "Z-Score (|Z| > 3.0 global)",
             "Isolation Forest (multivariate)"],
            horizontal=True,
            key="t5_method_pick",
            label_visibility="collapsed",
        )

        # Helper: log-safe transform for plotting (avoids log(0) → -inf).
        def _log_safe(x):
            return np.log10(np.asarray(x, dtype=float) + 1.0)

        if _method.startswith("Threshold"):
            st.caption(
                "Flags merchants whose weekly volume spikes beyond 3× their own 2026 average "
                "— the simplest possible rule."
            )
            if _thresh_anom.empty:
                st.info("No threshold anomalies detected in 2026 data.")
            else:
                _t_disp = _thresh_anom[["MERCHANT_GROUP", "WEEK_NUM", "WEEKLY_VOL", "RATIO"]].copy()
                _t_disp.insert(2, "Avg Vol (B IDR)", (_thresh_anom["WEEKLY_VOL"] / _thresh_anom["RATIO"]) / 1e9)
                _t_disp["Flagged Vol (B IDR)"] = _t_disp["WEEKLY_VOL"] / 1e9
                _t_disp["RATIO"] = _t_disp["RATIO"].map(lambda x: f"{x:.1f}×")
                st.dataframe(
                    _t_disp[["MERCHANT_GROUP", "WEEK_NUM", "Avg Vol (B IDR)", "Flagged Vol (B IDR)", "RATIO"]]
                    .rename(columns={"MERCHANT_GROUP": "Merchant", "WEEK_NUM": "Week", "RATIO": "Ratio"}),
                    use_container_width=True, hide_index=True,
                )
                # Bug fix: the chart was hardcoded to a single merchant
                # (INDOMARET). Plot the top flagged merchant-weeks across the
                # WHOLE portfolio instead — _thresh_anom already holds every row
                # where weekly VOL exceeded 3× that merchant's own 2026 average.
                _top_flagged = _thresh_anom.sort_values("RATIO", ascending=False).head(15).copy()
                _tf_label = (_top_flagged["MERCHANT_GROUP"].astype(str)
                             + "  ·  W" + _top_flagged["WEEK_NUM"].astype(int).astype(str))
                _pp_t = _p()
                _fig_t = go.Figure(go.Bar(
                    x=_top_flagged["RATIO"],
                    y=_tf_label,
                    orientation="h",
                    marker=dict(color=DANGER, line_width=0),
                    # Bold, theme-aware data labels stay legible in light & dark.
                    text=[f"<b>{r:.1f}×</b>" for r in _top_flagged["RATIO"]],
                    textposition="outside",
                    textfont=dict(size=12, color=_pp_t["TEXT_PRI"]),
                    cliponaxis=False,
                    customdata=(_top_flagged["WEEKLY_VOL"] / 1e9),
                    hovertemplate=("<b>%{y}</b><br>Spike: %{x:.1f}× own average<br>"
                                   "Volume: %{customdata:,.2f} B IDR<extra></extra>"),
                ))
                _fig_t.add_vline(
                    x=3, line_dash="dash", line_color=WARNING,
                    annotation_text="3× threshold", annotation_position="top",
                )
                _fig_t.update_layout(
                    title=f"Top {len(_top_flagged)} Volume Spikes — flagged merchant-weeks (VOL > 3× own avg)",
                    xaxis_title="Spike size (× merchant's own 2026 average)",
                    yaxis=dict(autorange="reversed"),   # biggest spike on top
                    height=max(340, 34 * len(_top_flagged) + 130),
                    margin=dict(l=8, r=88, t=56, b=44),
                    showlegend=False,
                )
                apply_plotly_theme(_fig_t)
                st.plotly_chart(_fig_t, use_container_width=True)

        elif _method.startswith("Z-Score"):
            st.caption(
                "Flags transactions more than 3 standard deviations from the global mean "
                "— catches extreme global outliers regardless of merchant identity."
            )
            if _zscore_anom.empty:
                st.info("No Z-score anomalies detected in 2026 data.")
            else:
                _z_disp = _zscore_anom[["MERCHANT_GROUP", "WEEK_NUM", "WEEKLY_VOL", "Z_SCORE"]].copy()
                _z_disp["Vol (B IDR)"] = _z_disp["WEEKLY_VOL"] / 1e9
                _z_disp["Z-Score"] = _z_disp["Z_SCORE"].map(lambda x: f"{x:.1f}")
                st.dataframe(
                    _z_disp[["MERCHANT_GROUP", "WEEK_NUM", "Vol (B IDR)", "Z-Score"]]
                    .rename(columns={"MERCHANT_GROUP": "Merchant", "WEEK_NUM": "Week"}),
                    use_container_width=True, hide_index=True,
                )

            # Plan §3.2 — 2-layer encoding instead of crushed-by-scale colorbar.
            _df_normal = _df_wm[_df_wm["Z_SCORE"].abs() <= 3.0]
            _df_anom_z = _zscore_anom
            _fig_z = go.Figure()
            # Layer 1: normals — small grey, low opacity, no labels.
            _pp_z = _p()
            _fig_z.add_trace(go.Scatter(
                x=_df_normal["WEEK_NUM"],
                y=_df_normal["WEEKLY_VOL"] / 1e9,
                mode="markers",
                marker=dict(color=_pp_z["TEXT_SEC"], opacity=0.5, size=6),
                name="Normal",
                hoverinfo='skip',
            ))
            # Layer 2: anomalies — red DIAMONDS (distinct from the Isolation
            # Forest stars) with a white halo + bold, theme-aware labels so they
            # stay legible in both light and dark mode (#3 markers, #4 contrast).
            if not _df_anom_z.empty:
                _labels = (_df_anom_z["MERCHANT_GROUP"].str.split().str[0]
                           + " W" + _df_anom_z["WEEK_NUM"].astype(int).astype(str))
                _fig_z.add_trace(go.Scatter(
                    x=_df_anom_z["WEEK_NUM"],
                    y=_df_anom_z["WEEKLY_VOL"] / 1e9,
                    mode="markers+text",
                    marker=dict(color=DANGER, size=13, symbol='diamond',
                                line=dict(color='white', width=2)),
                    text=[f"<b>{l}</b>" for l in _labels], textposition="top right",
                    textfont=dict(size=11, color=_pp_z["TEXT_PRI"]),
                    name=f"Anomaly (n={len(_df_anom_z)})",
                ))
            _fig_z.update_layout(
                title="All Merchants — Weekly VOL by Week (anomalies labeled)",
                xaxis_title="Week",
                yaxis_title="Volume (IDR Billions, log)",
                yaxis_type="log",
                # Mobile fix: legend below the plot so the chart spans full width.
                legend=dict(orientation="h", yanchor="top", y=-0.25, x=0, xanchor="left"),
            )
            apply_plotly_theme(_fig_z)
            st.plotly_chart(_fig_z, use_container_width=True)

        else:  # Isolation Forest
            st.caption(
                "Fits an ensemble of random trees on [VOL, TRX, FBI] simultaneously; "
                "rows easiest to isolate are flagged — catches anomalies invisible to any single metric."
            )
            if _if_anom.empty:
                st.info("No Isolation Forest anomalies detected in 2026 data.")
            else:
                _if_disp = _if_anom[["MERCHANT_GROUP", "WEEK_NUM", "WEEKLY_VOL", "WEEKLY_TRX"]].copy()
                _if_disp["Vol (B IDR)"] = _if_disp["WEEKLY_VOL"] / 1e9
                _if_disp["Vol/TRX"] = (
                    _if_disp["WEEKLY_VOL"] / _if_disp["WEEKLY_TRX"].clip(lower=1)
                ).map(lambda x: f"{x:,.0f} IDR/txn")
                st.dataframe(
                    _if_disp[["MERCHANT_GROUP", "WEEK_NUM", "Vol (B IDR)", "WEEKLY_TRX", "Vol/TRX"]]
                    .rename(columns={"MERCHANT_GROUP": "Merchant", "WEEK_NUM": "Week",
                                     "WEEKLY_TRX": "TRX Count"}),
                    use_container_width=True, hide_index=True,
                )

            # Plan §3.2 — log-log scatter with 2-layer encoding so the 90% of
            # points clustered near (0,0) on the linear scale are now spread out.
            _df_if_normal = _df_wm[_if_preds ==  1]
            _df_if_anom   = _df_wm[_if_preds == -1]
            _fig_if = go.Figure()
            _pp_if = _p()
            _fig_if.add_trace(go.Scatter(
                x=_df_if_normal["WEEKLY_TRX"].clip(lower=1),
                y=(_df_if_normal["WEEKLY_VOL"] / 1e9).clip(lower=0.001),
                mode="markers",
                marker=dict(color=_pp_if["TEXT_SEC"], opacity=0.5, size=6),
                name=f"Normal (n={len(_df_if_normal)})",
                hoverinfo='skip',
            ))
            # Anomalies — red STARS (distinct from the Z-Score diamonds) with a
            # white halo + bold, theme-aware labels (#3 markers, #4 contrast).
            if not _df_if_anom.empty:
                _if_labels = (_df_if_anom["MERCHANT_GROUP"].str.split().str[0]
                              + " W" + _df_if_anom["WEEK_NUM"].astype(int).astype(str))
                _fig_if.add_trace(go.Scatter(
                    x=_df_if_anom["WEEKLY_TRX"].clip(lower=1),
                    y=(_df_if_anom["WEEKLY_VOL"] / 1e9).clip(lower=0.001),
                    mode="markers+text",
                    marker=dict(color=DANGER, size=14, symbol='star',
                                line=dict(color='white', width=2)),
                    text=[f"<b>{l}</b>" for l in _if_labels], textposition="top right",
                    textfont=dict(size=11, color=_pp_if["TEXT_PRI"]),
                    name=f"Anomaly (n={len(_df_if_anom)})",
                ))
            _fig_if.update_layout(
                xaxis_title="Weekly TRX Count (log)",
                yaxis_title="Volume (IDR Billions, log)",
                title="Isolation Forest — VOL vs TRX (log-log, 2026)",
                xaxis_type="log",
                yaxis_type="log",
                # Mobile fix: legend below the plot so the chart spans full width.
                legend=dict(orientation="h", yanchor="top", y=-0.25, x=0, xanchor="left"),
            )
            apply_plotly_theme(_fig_if)
            st.plotly_chart(_fig_if, use_container_width=True)

        # ── Insight box ──────────────────────────────────────────────────────
        st.info(
            "The three methods are complementary: Threshold catches simple volume spikes. "
            "Z-Score catches global statistical outliers. Isolation Forest catches "
            "multivariate anomalies that no single metric would flag — like a merchant "
            "with high volume but near-zero transactions."
        )
