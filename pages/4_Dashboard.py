import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.ensemble import IsolationForest
import os
try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing as HoltWinters
    _HW_AVAILABLE = True
except ImportError:
    _HW_AVAILABLE = False
import sys

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)
from utils.theme import (
    apply_theme, page_header, section_label, section_header, styled_divider,
    kpi_card, kpi_row, tab_desc, filter_pill,
    status_card, apply_plotly_theme, get_palette, stale_data_banner,
    NAVY, GOLD, GOLD_DIM, BG, SURFACE, BORDER, TEXT_PRI, TEXT_SEC,
    GREEN, RED, AMBER, BLUE_ACC,
    CLUSTER_COLORS, PAYMENT_COLORS,
)
from utils.cloud_db import build_engine
from sqlalchemy import text

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon=os.path.join(_BASE, "static", "btn_logo.png"),
    layout="wide",
)
apply_theme()

@st.dialog("📂 No Data Found", width="large")
def _show_no_data_dialog():
    st.markdown(
        """
        <div style="text-align:center;padding:16px 0 8px;">
            <div style="font-size:3rem;margin-bottom:12px;">🗄️</div>
            <div style="font-size:1.15rem;font-weight:700;margin-bottom:8px;">
                The dashboard has no data to display.
            </div>
            <div style="font-size:0.92rem;color:#4D4D4D;line-height:1.6;max-width:480px;margin:0 auto 24px;">
                The database is empty or was recently reset. Run the automated pipeline
                to ingest Card Share &amp; Monitoring data before opening the dashboard.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("🚀 Go to Automated Pipeline", use_container_width=True, type="primary"):
            st.switch_page("pages/00_Automated_Pipeline.py")
    with c2:
        if st.button("⚙️ Go to Global Settings", use_container_width=True):
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

# ── PATHS ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_DB     = os.path.join(BASE_DIR, "database", "staging.db")
PATH_CARD   = os.path.join(BASE_DIR, "data", "master", "master_card_share.xlsx")
PATH_MON    = os.path.join(BASE_DIR, "data", "master", "master_monitoring.xlsx")
PATH_RAW_MON = os.path.join(BASE_DIR, "data", "raw", "real", "Monitoring Weekly Anchor 2026.xlsx")

def table_exists(conn_or_engine, name, schema="public"):
    if hasattr(conn_or_engine, "connect"):  # SQLAlchemy Engine
        from sqlalchemy import text
        q = text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :s AND table_name = :t)")
        with conn_or_engine.connect() as conn:
            return bool(conn.execute(q, {"s": schema, "t": name.lower()}).scalar())
    else:  # SQLite Connection
        return pd.read_sql_query(
            f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}'", conn_or_engine
        ).iloc[0, 0] == 1

# ── MACHINE LEARNING ENGINE ───────────────────────────────────────────────────
@st.cache_data
def run_ml(df_c, df_m, df_t=None, k_clusters=3, z_thresh=-1.5):
    """
    BTN Anchor ML Pipeline v2:
    1. Merge Card Share + Monitoring
    2. Feature Engineering — AVG_SV/FBI normalized by actual WEEKS_ACTIVE (not fixed /12)
    3. K-Means++ Clustering — composite multi-metric cluster ranking (SV + achievement + growth)
    4. Modified Z-Score (MAD) — robust anomaly detection, resistant to outliers in small portfolios
    5. Composite Risk Score 0–100 — weighted: Growth 40%, SV 30%, FBI 20%, Achievement 10%
    6. Three-tier CHURN_RISK — HIGH (≥60) / MEDIUM (30–59) / STABLE (<30)
    7. Silhouette Score — cluster separation quality metric
    """
    ML_COLS = ['MERCHANT_GROUP', 'CLUSTER', 'CHURN_RISK', 'RISK_SCORE', 'SILHOUETTE_SCORE',
               'PM', 'WEEKS_ACTIVE', 'SV_GROWTH_RATE', 'ACHIEVEMENT_PCT', 'AVG_SV', 'AVG_FBI',
               'ZSCORE_SV', 'ZSCORE_FBI', 'ZSCORE_GROWTH', 'SV_GROWTH_CLIPPED',
               'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'RASIO_ONUS',
               'IF_ANOMALY_SCORE', 'IF_IS_ANOMALY',
               'IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI', 'IF_CONTRIB_RASIO_ONUS',
               'IF_CONTRIB_SV_GROWTH', 'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']

    if df_c.empty:
        return pd.DataFrame(columns=ML_COLS)

    # ── 1. Merge ──────────────────────────────────────────────────────────────
    if not df_m.empty:
        agg_cols = {c: 'sum' for c in ['TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI'] if c in df_c.columns}
        if 'RASIO_ONUS' in df_c.columns: agg_cols['RASIO_ONUS'] = 'mean'
        df = df_c.groupby('MERCHANT_GROUP').agg(agg_cols).reset_index()
        df = pd.merge(df, df_m, on='MERCHANT_GROUP', how='left')
    else:
        df = df_c.copy()

    if df.empty:
        return pd.DataFrame(columns=ML_COLS)

    for col in ['TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'RASIO_ONUS']:
        if col not in df.columns: df[col] = 0

    # ── 2. Feature Engineering ────────────────────────────────────────────────
    # Normalize monthly avg by actual weeks active, not a hardcoded /12
    df['WEEKS_ACTIVE'] = pd.to_numeric(
        df.get('WEEKS_ACTIVE', pd.Series([12] * len(df))), errors='coerce'
    ).fillna(12).clip(1, 52)
    months_active = (df['WEEKS_ACTIVE'] / 4.33).clip(1, 12)
    df['AVG_SV']     = df['TOTAL_SV'] / months_active
    df['AVG_FBI']    = df['TOTAL_FBI'] / months_active
    df['RASIO_ONUS'] = df['RASIO_ONUS'].clip(0, 1).fillna(0)

    df['SV_GROWTH_RATE'] = pd.to_numeric(
        df.get('SV_GROWTH_RATE', pd.Series([0] * len(df))), errors='coerce'
    ).fillna(0)

    if len(df) > 1:
        low, high = df['SV_GROWTH_RATE'].quantile([0.05, 0.95])
        df['SV_GROWTH_CLIPPED'] = df['SV_GROWTH_RATE'].clip(low, high)
    else:
        df['SV_GROWTH_CLIPPED'] = df['SV_GROWTH_RATE']

    if df_t is not None and not df_t.empty and 'TARGET_VOL_2026' in df_t.columns:
        df = pd.merge(df, df_t[['MERCHANT_GROUP', 'TARGET_VOL_2026']], on='MERCHANT_GROUP', how='left')
        df['ACHIEVEMENT_PCT'] = np.where(
            df['TARGET_VOL_2026'].fillna(0) > 0,
            (df['TOTAL_SV'] / df['TARGET_VOL_2026'] * 100).clip(0, 200), 0
        )
    else:
        df['ACHIEVEMENT_PCT'] = 0

    # ── 3. Clustering ─────────────────────────────────────────────────────────
    FEAT = ['AVG_SV', 'AVG_FBI', 'RASIO_ONUS', 'SV_GROWTH_CLIPPED', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE']
    X = df[FEAT].fillna(0).copy()
    X['AVG_SV']  = np.log1p(X['AVG_SV'])
    X['AVG_FBI'] = np.log1p(X['AVG_FBI'])

    df['SILHOUETTE_SCORE'] = 0.0
    df['RISK_SCORE']       = 0.0

    try:
        if len(df) >= k_clusters:
            X_s = StandardScaler().fit_transform(X)
            km  = KMeans(n_clusters=k_clusters, init='k-means++', n_init=20, random_state=42)
            df['CLUSTER_RAW'] = km.fit_predict(X_s)

            # Multi-metric composite ranking: normalize each metric across clusters
            # then weight: SV 60%, Achievement 25%, Growth 15%
            cs = df.groupby('CLUSTER_RAW').agg(
                {'AVG_SV': 'mean', 'ACHIEVEMENT_PCT': 'mean', 'SV_GROWTH_CLIPPED': 'mean'}
            )
            for col in cs.columns:
                rng = cs[col].max() - cs[col].min()
                cs[col] = (cs[col] - cs[col].min()) / (rng + 1e-9)
            cs['COMPOSITE'] = 0.60 * cs['AVG_SV'] + 0.25 * cs['ACHIEVEMENT_PCT'] + 0.15 * cs['SV_GROWTH_CLIPPED']
            rank = {c: i for i, c in enumerate(cs['COMPOSITE'].sort_values(ascending=False).index)}

            lbl_maps = {
                3: {0: 'PREMIUM', 1: 'REGULER', 2: 'PASIF'},
                4: {0: 'ELITE', 1: 'PREMIUM', 2: 'REGULER', 3: 'PASIF'},
                5: {0: 'ELITE', 1: 'PREMIUM', 2: 'REGULER', 3: 'PASIF', 4: 'DORMANT'}
            }
            lbl = lbl_maps.get(k_clusters, {i: f'TIER {i+1}' for i in range(k_clusters)})
            df['CLUSTER'] = df['CLUSTER_RAW'].map(lambda c: lbl[rank[c]])

            # Silhouette score: measures how well-separated the clusters are
            # Range: -1 to 1 | >0.5 = strong | 0.25–0.5 = moderate | <0.25 = weak
            if len(df) >= 2:
                df['SILHOUETTE_SCORE'] = round(float(silhouette_score(X_s, df['CLUSTER_RAW'])), 4)

            # ── 4a. Isolation Forest — Multivariate Anomaly Detection ─────────
            # Liu et al. (2008): builds n_estimators random trees; anomalies need
            # fewer splits to isolate → shorter average path length → anomaly score.
            # Uses same X_s (scaled, log-transformed) as K-Means for methodological
            # consistency. contamination=0.10 flags ~10% of portfolio (~3-4 merchants).
            try:
                if len(df) >= 4:
                    iso = IsolationForest(
                        n_estimators=100, contamination=0.10,
                        random_state=42, n_jobs=-1
                    )
                    iso.fit(X_s)
                    df['IF_ANOMALY_SCORE'] = (-iso.score_samples(X_s)).round(4)
                    df['IF_IS_ANOMALY']    = (iso.predict(X_s) == -1)

                    # ── LOFO Feature Contribution ──────────────────────────
                    # Leave-One-Feature-Out: for each feature, neutralize it
                    # (set to 0 = portfolio mean in scaled space), re-score,
                    # measure delta. No re-fitting needed — just re-scoring.
                    # Positive delta = feature makes this merchant MORE anomalous.
                    _lofo_keys = [
                        'IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                        'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                        'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE'
                    ]
                    _base_scores = -iso.score_samples(X_s)
                    for _fi, _fk in enumerate(_lofo_keys):
                        _X_abl = X_s.copy()
                        _X_abl[:, _fi] = 0.0
                        df[_fk] = (_base_scores - (-iso.score_samples(_X_abl))).round(4)
                else:
                    df['IF_ANOMALY_SCORE'] = 0.0
                    df['IF_IS_ANOMALY']    = False
                    for _fk in ['IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                                 'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                                 'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']:
                        df[_fk] = 0.0
            except Exception:
                df['IF_ANOMALY_SCORE'] = 0.0
                df['IF_IS_ANOMALY']    = False
                for _fk in ['IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                             'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                             'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']:
                    df[_fk] = 0.0

        else:
            df['CLUSTER'] = 'REGULER'

        # ── 4. Modified Z-Score (MAD) ─────────────────────────────────────────
        # MAD = Median Absolute Deviation — resistant to extreme outliers.
        # Formula: z = 0.6745 * (x - median) / MAD
        # More reliable than standard Z-score for small portfolios (~38 merchants)
        def _mad_zscore(series):
            s = pd.to_numeric(series, errors='coerce').fillna(0)
            if len(s) < 2: return pd.Series(0.0, index=s.index)
            median = s.median()
            mad    = (s - median).abs().median()
            if mad < 1e-9: return pd.Series(0.0, index=s.index)
            return 0.6745 * (s - median) / mad

        if len(df) > 1:
            df['ZSCORE_SV']     = _mad_zscore(np.log1p(df['AVG_SV']))
            df['ZSCORE_FBI']    = _mad_zscore(np.log1p(df['AVG_FBI']))
            df['ZSCORE_GROWTH'] = _mad_zscore(df['SV_GROWTH_CLIPPED'])
        else:
            df['ZSCORE_SV'] = df['ZSCORE_FBI'] = df['ZSCORE_GROWTH'] = 0.0

        # ── 5. Composite Risk Score (0–100) ───────────────────────────────────
        # Weights reflect predictive importance for merchant churn:
        # Growth trend (40%) > Volume anomaly (30%) > FBI anomaly (20%) > Target gap (10%)
        df['RISK_SCORE'] = (
            np.clip(-df['ZSCORE_GROWTH'], 0, 3) / 3 * 40 +
            np.clip(-df['ZSCORE_SV'],     0, 3) / 3 * 30 +
            np.clip(-df['ZSCORE_FBI'],    0, 3) / 3 * 20 +
            np.clip(1 - df['ACHIEVEMENT_PCT'] / 100, 0, 1) * 10
        ).clip(0, 100).round(1)

        # ── 6. Three-tier Churn Risk ──────────────────────────────────────────
        def _risk_tier(score):
            if score >= 60: return 'HIGH RISK ⚠️'
            if score >= 30: return 'MEDIUM RISK 🟡'
            return 'STABLE ✅'
        df['CHURN_RISK'] = df['RISK_SCORE'].apply(_risk_tier)

        # ── z_thresh override: any z-score breach upgrades STABLE → MEDIUM RISK ──
        if z_thresh is not None and len(df) > 1:
            zscore_breach = (
                (df['ZSCORE_SV']     < z_thresh) |
                (df['ZSCORE_FBI']    < z_thresh) |
                (df['ZSCORE_GROWTH'] < z_thresh)
            )
            df.loc[zscore_breach & (df['CHURN_RISK'] == 'STABLE ✅'), 'CHURN_RISK'] = 'MEDIUM RISK 🟡'

    except Exception as e:
        st.warning(f"⚠️ ML pipeline encountered an error and fell back to defaults: {e}")
        df['CLUSTER']    = 'UNKNOWN'
        df['CHURN_RISK'] = 'STABLE ✅'
        df['RISK_SCORE'] = 0.0
        df['ZSCORE_SV']  = df['ZSCORE_FBI'] = df['ZSCORE_GROWTH'] = 0.0

    for col in ML_COLS:
        if col not in df.columns: df[col] = np.nan

    return df


def _hw_forecast(monthly_sv_series, periods_ahead=12):
    """
    Holt-Winters exponential smoothing on historical monthly SV.
    Algorithm: Winters (1960). Decomposes series into level, trend, and optionally
    seasonal components. Parameters optimized via MLE.
    Falls back gracefully to {'success': False} if model fails or data is insufficient.

    seasonal='add' with seasonal_periods=12 requires >= 24 data points for stable
    estimation. With fewer months, uses Holt's Double Smoothing (trend only).
    """
    result = {'forecast': None, 'projected_eoy': None, 'method': 'Estimated Run Rate', 'success': False}
    if not _HW_AVAILABLE:
        return result
    series = pd.to_numeric(monthly_sv_series, errors='coerce').fillna(0)
    series = series[series > 0]
    if len(series) < 6:
        return result
    try:
        use_seasonal = len(series) >= 24
        if use_seasonal:
            model = HoltWinters(
                series.values, trend='add', seasonal='add',
                seasonal_periods=12, initialization_method='estimated'
            )
            method_label = 'AI Trend Forecast'
        else:
            model = HoltWinters(
                series.values, trend='add', seasonal=None,
                initialization_method='estimated'
            )
            method_label = 'AI Trend Forecast'
        fit = model.fit(optimized=True, remove_bias=True)
        forecast_values = np.maximum(fit.forecast(periods_ahead), 0)
        result.update({
            'forecast': forecast_values,
            'projected_eoy': float(np.sum(forecast_values)),
            'method': method_label,
            'success': True
        })
    except Exception:
        pass
    return result


# ── DB LOAD (Cloud-Aware) ─────────────────────────────────────────────────────
neon_url = os.getenv("DATABASE_URL")
if neon_url:
    engine = build_engine()
    has_card       = table_exists(engine, "PROCESSED_CARD_SHARE")
    has_card_hist  = table_exists(engine, "PROCESSED_CARD_HISTORY")
    has_mon        = table_exists(engine, "PROCESSED_MONITORING")
    has_mon_weekly = table_exists(engine, "PROCESSED_MONITORING_WEEKLY")
    has_tgt        = table_exists(engine, "TARGET")

    df_card        = pd.read_sql_query("SELECT * FROM processed_card_share", engine) if has_card else pd.DataFrame()
    df_card_hist   = pd.read_sql_query("SELECT * FROM processed_card_history", engine) if has_card_hist else pd.DataFrame()
    df_mon         = pd.read_sql_query("SELECT * FROM processed_monitoring", engine) if has_mon else pd.DataFrame()
    df_mon_weekly  = pd.read_sql_query("SELECT * FROM processed_monitoring_weekly", engine) if has_mon_weekly else pd.DataFrame()
    df_target      = pd.read_sql_query("SELECT * FROM target", engine) if has_tgt else pd.DataFrame()

    # Column normalization for Postgres (ensure uppercase for dashboard consistency)
    for df in [df_card, df_card_hist, df_mon, df_mon_weekly, df_target]:
        if len(df.columns) > 0:
            df.columns = [c.upper() for c in df.columns]

    has_monthly_tbl = table_exists(engine, "PROCESSED_CARD_MONTHLY")

    # Show popup if Neon is connected but tables are empty (e.g. after a database reset)
    if df_card.empty and df_mon.empty:
        _show_no_data_dialog()
        st.stop()
else:
    if not os.path.exists(PATH_DB):
        st.warning("⚠️ Database not found. Process files in the Processing pages first.")
        st.stop()

    conn = sqlite3.connect(PATH_DB)
    has_card       = table_exists(conn, "PROCESSED_CARD_SHARE")
    has_card_hist  = table_exists(conn, "PROCESSED_CARD_HISTORY")
    has_mon        = table_exists(conn, "PROCESSED_MONITORING")
    has_mon_weekly = table_exists(conn, "PROCESSED_MONITORING_WEEKLY")
    has_tgt        = table_exists(conn, "TARGET")

    df_card        = pd.read_sql_query("SELECT * FROM PROCESSED_CARD_SHARE", conn) if has_card else pd.DataFrame()
    df_card_hist   = pd.read_sql_query("SELECT * FROM PROCESSED_CARD_HISTORY", conn) if has_card_hist else pd.DataFrame()
    df_mon         = pd.read_sql_query("SELECT * FROM PROCESSED_MONITORING", conn) if has_mon else pd.DataFrame()
    df_mon_weekly  = pd.read_sql_query("SELECT * FROM PROCESSED_MONITORING_WEEKLY", conn) if has_mon_weekly else pd.DataFrame()
    df_target      = pd.read_sql_query("SELECT * FROM TARGET", conn) if has_tgt else pd.DataFrame()
    has_monthly_tbl = table_exists(conn, "PROCESSED_CARD_MONTHLY")
    conn.close()

# ── BATCH METADATA & SIGNALS ──────────────────────────────────────────────────
_last_update = "Unknown"
_show_new_badge = False
try:
    if neon_url:
        _df_meta = pd.read_sql_query("SELECT * FROM app_metadata", engine)
        _df_meta.columns = [c.upper() for c in _df_meta.columns]
    elif os.path.exists(PATH_DB):
        _conn_meta = sqlite3.connect(PATH_DB)
        _df_meta = pd.read_sql_query("SELECT * FROM APP_METADATA", _conn_meta)
        _conn_meta.close()
    
    _meta_dict = dict(zip(_df_meta['KEY'], _df_meta['VALUE']))
    _last_update = _meta_dict.get('LAST_DATA_UPDATE', 'Unknown')
    _show_new_badge = _meta_dict.get('NEW_DATA_SIGNAL') == '1'
except:
    pass

# ── HEADER ───────────────────────────────────────────────────────────────────
header_col1, header_col2 = st.columns([0.8, 0.2])
with header_col1:
    st.markdown("## BTN Anchor Merchant Decision Intelligence Platform")
with header_col2:
    if _show_new_badge:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🆕 NEW DATA", help=f"Last updated: {_last_update}. Click to clear.", type="primary"):
            try:
                if neon_url:
                    with engine.begin() as _conn_m:
                        _conn_m.execute(text("UPDATE app_metadata SET value = '0' WHERE key = 'NEW_DATA_SIGNAL'"))
                else:
                    _conn_meta = sqlite3.connect(PATH_DB)
                    _conn_meta.execute("UPDATE APP_METADATA SET value = '0' WHERE key = 'NEW_DATA_SIGNAL'")
                    _conn_meta.commit()
                    _conn_meta.close()
                st.rerun()
            except: pass

# ── Stale Data Banner ─────────────────────────────────────────────────────────
# Only relevant in local mode — Neon data has no local file age to check
if not neon_url:
    stale_data_banner(db_path=PATH_DB, threshold_hours=24)

# ── Global KPI Strip (full-portfolio totals, always unfiltered) ───────────────
_total_merchants = df_card['MERCHANT_GROUP'].nunique()          if not df_card.empty and 'MERCHANT_GROUP' in df_card.columns else 0
_ytd_sv          = df_card['TOTAL_SV'].sum()                    if not df_card.empty and 'TOTAL_SV'        in df_card.columns else 0
_ytd_trx         = df_card['TOTAL_TRX'].sum()                   if not df_card.empty and 'TOTAL_TRX'       in df_card.columns else 0
_avg_onus        = df_card['RASIO_ONUS'].mean()                 if not df_card.empty and 'RASIO_ONUS'      in df_card.columns else 0

_ml_kpi = run_ml(df_card, df_mon, df_target, z_thresh=-1.2) if (has_card and has_mon) else pd.DataFrame()
_high_risk_count = int(_ml_kpi['CHURN_RISK'].str.contains('HIGH', na=False).sum()) if not _ml_kpi.empty and 'CHURN_RISK' in _ml_kpi.columns else 0

_sv_fmt  = f"Rp {_ytd_sv/1e9:,.1f} M"  if _ytd_sv >= 1e9 else f"Rp {_ytd_sv/1e6:,.0f} Jt"
_trx_fmt = f"{_ytd_trx/1e6:,.2f} M"    if _ytd_trx >= 1e6 else f"{_ytd_trx:,.0f}"

_kc1, _kc2, _kc3, _kc4, _kc5 = st.columns([1, 1, 1, 1, 1])
_kc1.metric("🏪 Merchants Tracked",    f"{_total_merchants:,}")
_kc2.metric("💰 YTD Sales Volume",     _sv_fmt)
_kc3.metric("🔄 YTD Transactions",     _trx_fmt)
_kc4.metric("🎯 Avg On-Us Ratio",      f"{_avg_onus*100:.1f}%")
_kc5.metric("⚠️ High Risk Merchants",  _high_risk_count,
            delta=f"-{_high_risk_count}" if _high_risk_count > 0 else None,
            delta_color="inverse")

# ── System Health (DB row counts only — no local file checks on cloud) ─────────
_card_rows = len(df_card)   if has_card and not df_card.empty   else 0
_mon_rows  = len(df_mon)    if has_mon  and not df_mon.empty    else 0
_tgt_rows  = len(df_target) if has_tgt  and not df_target.empty else 0

with st.expander("⚙️ System Health & Data Status", expanded=False):
    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("📊 Card Share DB",
               f"✅ {_card_rows:,} rows" if _card_rows > 0 else ("⚠️ Empty" if has_card else "❌ Missing"))
    sc2.metric("📅 Monitoring DB",
               f"✅ {_mon_rows:,} rows"  if _mon_rows  > 0 else ("⚠️ Empty" if has_mon  else "❌ Missing"))
    sc3.metric("🎯 Target Data",
               f"✅ {_tgt_rows:,} merchants" if _tgt_rows > 0 else "❌ Missing")

styled_divider()

# ── PORTFOLIO FILTERS (directly above tabs) ───────────────────────────────────
f_col1, f_col2 = st.columns(2)

all_groups = ["ALL GROUPS"]
if not df_card.empty:
    all_groups += sorted(df_card['MERCHANT_GROUP'].unique().tolist())
with f_col1:
    sel_group = st.selectbox("🏬 Merchant Group", all_groups, key="sb_group")

filtered_brands = ["TOTAL GROUP"]
if sel_group != "ALL GROUPS" and not df_card.empty:
    brands = df_card[df_card['MERCHANT_GROUP'] == sel_group]['MERCHANT_ANCHOR'].unique().tolist()
    filtered_brands += sorted(brands)
elif sel_group == "ALL GROUPS" and not df_card.empty:
    filtered_brands = ["TOTAL PORTFOLIO"]

with f_col2:
    sel_brand = st.selectbox("⚓ Merchant Brand (Anchor)", filtered_brands, key="sb_brand")

if sel_group != "ALL GROUPS":
    df_card      = df_card[df_card['MERCHANT_GROUP'] == sel_group]
    df_card_hist = df_card_hist[df_card_hist['MERCHANT_GROUP'] == sel_group]
    if not df_mon.empty:        df_mon        = df_mon[df_mon['MERCHANT_GROUP'] == sel_group]
    if not df_mon_weekly.empty: df_mon_weekly = df_mon_weekly[df_mon_weekly['MERCHANT_GROUP'] == sel_group]
    if not df_target.empty:     df_target     = df_target[df_target['MERCHANT_GROUP'] == sel_group]
    if sel_brand not in ["TOTAL GROUP", "TOTAL PORTFOLIO"]:
        df_card      = df_card[df_card['MERCHANT_ANCHOR'] == sel_brand]
        df_card_hist = df_card_hist[df_card_hist['MERCHANT_ANCHOR'] == sel_brand]

st.caption(f"Showing results for: **{sel_group}** > **{sel_brand}**")

CLAMP = CLUSTER_COLORS

# ── TABS ──────────────────────────────────────────────────────────────────────
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "🏠  Overview",
    "💰  Card Share",
    "📅  Weekly Monitor",
    "📊  Merchant Tiers",
    "🔔  Health Alerts",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 0 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab0:
    tab_desc("Portfolio health at a glance — spot risks, track progress against targets, and surface merchants that need immediate attention.")

    # ── High-risk banner ─────────────────────────────────────────────────────
    if _high_risk_count > 0:
        st.warning(f"⚠️ **{_high_risk_count} merchant(s) need immediate attention.** See the **Health Alerts** tab for recommended actions.")

    # ── Fleet Health Zone (visual-first, 3-column) ────────────────────────────
    if not _ml_kpi.empty:
        section_label("📡 Fleet Health Snapshot")
        fh1, fh2, fh3 = st.columns(3)

        with fh1:
            _risk_counts = _ml_kpi['CHURN_RISK'].value_counts().reset_index()
            _risk_counts.columns = ['Status', 'Count']
            _risk_color_map = {'HIGH RISK ⚠️': '#C0392B', 'MEDIUM RISK 🟡': '#F59E0B', 'STABLE ✅': '#27AE60'}
            fig_ov_donut = px.pie(
                _risk_counts, names='Status', values='Count', hole=0.55,
                title='Portfolio Health Status',
                color='Status', color_discrete_map=_risk_color_map,
            )
            fig_ov_donut.update_layout(height=280, margin=dict(t=36, b=10, l=10, r=10), **_chart_base())
            st.plotly_chart(fig_ov_donut, use_container_width=True, theme=None)

        with fh2:
            if 'ACHIEVEMENT_PCT' in _ml_kpi.columns:
                _ach = _ml_kpi['ACHIEVEMENT_PCT'].dropna()
                _avg_ach = _ach.mean()
                _pp_ov = _p()
                _ach_color = "#34D399" if _avg_ach >= 90 else ("#FBBF24" if _avg_ach >= 70 else "#F87171")
                fig_ov_ach = px.histogram(
                    _ml_kpi, x='ACHIEVEMENT_PCT', nbins=10,
                    title='Target Achievement Distribution',
                    labels={'ACHIEVEMENT_PCT': 'Achievement (% of FY Target)'},
                    color_discrete_sequence=[_ach_color],
                )
                fig_ov_ach.add_vline(x=100, line_dash='dash', line_color='#34D399',
                                     annotation_text='100% Target', annotation_font_color='#34D399',
                                     annotation_position='top right')
                fig_ov_ach.update_layout(height=280, margin=dict(t=36, b=52, l=60, r=10),
                                          showlegend=False, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                st.plotly_chart(fig_ov_ach, use_container_width=True, theme=None)

        with fh3:
            if 'CLUSTER' in _ml_kpi.columns:
                _clust_counts = _ml_kpi['CLUSTER'].value_counts().reset_index()
                _clust_counts.columns = ['Tier', 'Merchants']
                _tier_colors = {'ELITE': '#F1C40F', 'PREMIUM': '#27AE60', 'REGULER': '#2F80ED',
                                'PASIF': '#EB5757', 'DORMANT': '#888888'}
                fig_ov_tier = px.bar(
                    _clust_counts.sort_values('Merchants', ascending=True),
                    x='Merchants', y='Tier', orientation='h',
                    title='Merchants by Performance Tier',
                    color='Tier', color_discrete_map=_tier_colors,
                )
                fig_ov_tier.update_layout(height=280, margin=dict(t=36, b=10, l=80, r=10),
                                           showlegend=False, **_chart_base(), xaxis=_xaxis(),
                                           yaxis=dict(showgrid=False, automargin=True))
                st.plotly_chart(fig_ov_tier, use_container_width=True, theme=None)

    # ── PM Coverage Cards (visual, not table) ─────────────────────────────────
    if not df_target.empty and 'PM' in df_target.columns:
        styled_divider()
        section_label("👤 Account Manager Coverage")
        _active_pms = df_target['PM'].nunique()
        _unassigned = int((df_target['PM'].fillna('UNASSIGNED').str.upper() == 'UNASSIGNED').sum())
        _assigned = len(df_target) - _unassigned
        _avg_per_pm = round(_assigned / max(_active_pms, 1), 1)

        # ── Row 1: Individual PM cards ────────────────────────────────────────
        if not _ml_kpi.empty and 'PM' in _ml_kpi.columns:
            _pm_list = sorted(df_target['PM'].dropna().unique().tolist())
            _pm_list = [pm for pm in _pm_list if pm.upper() != 'UNASSIGNED']
            pm_card_cols = st.columns(max(len(_pm_list[:4]), 1))
            for i, _pm in enumerate(_pm_list[:4]):
                _pm_merch = _ml_kpi[_ml_kpi['PM'] == _pm]
                _pm_high  = int(_pm_merch['CHURN_RISK'].str.contains('HIGH', na=False).sum())
                _pm_ach   = _pm_merch['ACHIEVEMENT_PCT'].mean() if 'ACHIEVEMENT_PCT' in _pm_merch.columns else 0
                with pm_card_cols[i]:
                    _pm_color = "#F87171" if _pm_high > 0 else "#34D399"
                    st.markdown(
                        f"""<div style="padding:12px 14px;border-radius:12px;border:1px solid {_pm_color}40;
                            background:{_pm_color}10;margin-bottom:8px;">
                            <div style="font-size:0.8rem;font-weight:700;color:{_pm_color};">{_pm}</div>
                            <div style="font-size:1.4rem;font-weight:800;">{len(_pm_merch)}</div>
                            <div style="font-size:0.72rem;color:#888;">merchants</div>
                            {'<div style="font-size:0.72rem;color:#F87171;margin-top:4px;">⚠️ ' + str(_pm_high) + ' need attention</div>' if _pm_high > 0 else '<div style="font-size:0.72rem;color:#34D399;margin-top:4px;">✅ All on track</div>'}
                            <div style="font-size:0.72rem;color:#888;margin-top:2px;">Avg achievement: {_pm_ach:.0f}%</div>
                        </div>""", unsafe_allow_html=True
                    )

        # ── Row 2: Aggregate summary below the cards ──────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        agg_c1, agg_c2, agg_c3 = st.columns(3)
        with agg_c1:
            st.metric("Active Account Managers", _active_pms)
        with agg_c2:
            st.metric("Avg Merchant Load", f"{_avg_per_pm} merchants each")
        with agg_c3:
            st.metric("Unassigned Merchants", _unassigned,
                      delta=f"+{_unassigned} need assignment" if _unassigned > 0 else None,
                      delta_color="inverse" if _unassigned > 0 else "off")

        with st.expander("📋 View Full PM Assignment Table"):
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
    else:
        st.info("No assignment data loaded. Run the pipeline to populate PM assignments.")

    # ── Batch Impact (merged from former Batch Impact tab) ────────────────────
    styled_divider()
    section_header("📊", "Batch Impact Analysis", "Latest ingestion vs previous cycle")

    def _render_batch_impact(fetch_dates, df_getter):
        """Shared rendering logic for both Neon and SQLite batch sources."""
        if len(fetch_dates) < 1:
            st.info("Not enough batches to compare. Upload data first.")
            return
        latest_date = fetch_dates.iloc[0, 0]
        prev_date   = fetch_dates.iloc[1, 0] if len(fetch_dates) > 1 else None
        st.markdown(f"**Ingestion Batch:** `{latest_date}`")
        df_latest = df_getter(latest_date)
        sum_latest_sv  = df_latest['TOTAL_SV'].sum()
        sum_latest_trx = df_latest['TOTAL_TRX'].sum()
        if prev_date:
            df_prev = df_getter(prev_date)
            sum_prev_sv  = df_prev['TOTAL_SV'].sum()
            sum_prev_trx = df_prev['TOTAL_TRX'].sum()
            delta_sv  = sum_latest_sv  - sum_prev_sv
            pct_sv    = (delta_sv  / sum_prev_sv  * 100) if sum_prev_sv  > 0 else 0
            delta_trx = sum_latest_trx - sum_prev_trx
            pct_trx   = (delta_trx / sum_prev_trx * 100) if sum_prev_trx > 0 else 0
            bi1, bi2 = st.columns(2)
            bi1.metric("Ingested Sales Volume",  f"Rp {sum_latest_sv/1e9:,.2f}B",  f"{delta_sv/1e6:,.1f}M ({pct_sv:+.1f}%)")
            bi2.metric("Ingested Transactions",  f"{sum_latest_trx:,.0f}",          f"{delta_trx:,.0f} ({pct_trx:+.1f}%)")
            merged = pd.merge(
                df_latest.groupby('MERCHANT_GROUP').sum().reset_index(),
                df_prev.groupby('MERCHANT_GROUP').sum().reset_index(),
                on='MERCHANT_GROUP', suffixes=('_new','_old')
            )
            merged['Delta SV']  = merged['TOTAL_SV_new'] - merged['TOTAL_SV_old']
            merged['Growth %']  = (merged['Delta SV'] / merged['TOTAL_SV_old'].replace(0, 1) * 100)
            styled_divider()
            g_col, l_col = st.columns(2)
            _top_gain = merged.sort_values('Delta SV', ascending=False).head(5)
            _top_loss = merged.sort_values('Delta SV', ascending=True).head(5)
            with g_col:
                section_label("🟢 Top 5 Gainers")
                fig_gain = go.Figure(go.Bar(
                    x=_top_gain['Delta SV'] / 1e6,
                    y=_top_gain['MERCHANT_GROUP'],
                    orientation='h',
                    marker_color='#27AE60',
                    text=[f"Rp {v/1e6:,.0f}Jt ({r:.0f}%)" for v, r in zip(_top_gain['Delta SV'], _top_gain['Growth %'])],
                    textposition='outside',
                ))
                fig_gain.update_layout(height=260, margin=dict(l=150, r=110, t=10, b=40),
                                       xaxis={**_xaxis(), 'title': 'Volume Change (Jt Rp)'},
                                       yaxis=dict(showgrid=False, automargin=True), **_chart_base())
                st.plotly_chart(fig_gain, use_container_width=True, theme=None)
            with l_col:
                section_label("🔴 Top 5 Losers")
                fig_loss = go.Figure(go.Bar(
                    x=_top_loss['Delta SV'] / 1e6,
                    y=_top_loss['MERCHANT_GROUP'],
                    orientation='h',
                    marker_color='#EB5757',
                    text=[f"Rp {v/1e6:,.0f}Jt ({r:.0f}%)" for v, r in zip(_top_loss['Delta SV'], _top_loss['Growth %'])],
                    textposition='outside',
                ))
                fig_loss.update_layout(height=260, margin=dict(l=150, r=110, t=10, b=40),
                                       xaxis={**_xaxis(), 'title': 'Volume Change (Jt Rp)'},
                                       yaxis=dict(showgrid=False, automargin=True), **_chart_base())
                st.plotly_chart(fig_loss, use_container_width=True, theme=None)
            with st.expander("📋 View Full Batch Comparison Table"):
                st.dataframe(merged[['MERCHANT_GROUP','Delta SV','Growth %']].sort_values('Delta SV', ascending=False), hide_index=True, use_container_width=True)
        else:
            st.info(f"Only one batch found ({latest_date}). Comparison available after the next update.")
            st.metric("Ingested Sales Volume", f"Rp {sum_latest_sv/1e9:,.2f}B")
            st.metric("Ingested Transactions", f"{sum_latest_trx:,.0f}")

    if neon_url:
        try:
            fetch_dates = pd.read_sql_query(
                "SELECT DISTINCT edw_fetch_date FROM card_share ORDER BY edw_fetch_date DESC LIMIT 2", engine
            )
            fetch_dates.columns = [c.upper() for c in fetch_dates.columns]
            fetch_dates = fetch_dates.rename(columns={"EDW_FETCH_DATE": fetch_dates.columns[0]})

            def _neon_getter(date_val):
                df = pd.read_sql_query(
                    "SELECT merchant_group, total_sv, total_trx FROM card_share WHERE edw_fetch_date = %(d)s",
                    engine, params={"d": date_val}
                )
                df.columns = [c.upper() for c in df.columns]
                return df

            _render_batch_impact(fetch_dates, _neon_getter)
        except Exception as e:
            st.error(f"Error Analyzing Batch: {e}")
    elif not os.path.exists(PATH_DB):
        st.warning("Database not found.")
    else:
        try:
            conn_b = sqlite3.connect(PATH_DB)

            def _sqlite_getter(date_val):
                return pd.read_sql_query(
                    f"SELECT MERCHANT_GROUP, TOTAL_SV, TOTAL_TRX FROM CARD_SHARE WHERE EDW_FETCH_DATE = '{date_val}'", conn_b
                )

            fetch_dates = pd.read_sql_query(
                "SELECT DISTINCT EDW_FETCH_DATE FROM CARD_SHARE ORDER BY EDW_FETCH_DATE DESC LIMIT 2", conn_b
            )
            _render_batch_impact(fetch_dates, _sqlite_getter)
            conn_b.close()
        except Exception as e:
            st.error(f"Error Analyzing Batch: {e}")

    # ── Merchant Explorer (formerly Merchant Detail tab) ─────────────────────
    styled_divider()
    with st.expander("🔍 Merchant Explorer & Export", expanded=False):
        st.caption("Fully interactive explorer. Apply any combination of filters, search, sort, and export to CSV.")
        if has_card and has_mon:
            df_exp = run_ml(df_card, df_mon, df_target)
        elif has_card:
            df_exp = df_card.copy()
        else:
            df_exp = df_mon.copy() if not df_mon.empty else pd.DataFrame()

        if df_exp.empty:
            st.info("ℹ️ No merchants found to explore. Please populate the database first.")
        else:
            section_label("🎛️ Explorer Filters")
            ef1, ef2, ef3, ef4 = st.columns(4)
            with ef1:
                if 'CLUSTER' in df_exp.columns:
                    _ec_opts = sorted(df_exp['CLUSTER'].dropna().unique().tolist())
                    sel_ec = st.multiselect("Cluster", _ec_opts, default=_ec_opts, key="e_clust")
                    df_exp = df_exp[df_exp['CLUSTER'].isin(sel_ec)]
            with ef2:
                if 'PM' in df_exp.columns:
                    all_pm_e = sorted(df_exp['PM'].dropna().unique().tolist())
                    sel_ep = st.multiselect("PM", all_pm_e, default=all_pm_e, key="e_pm")
                    df_exp = df_exp[df_exp['PM'].isin(sel_ep)]
            with ef3:
                if 'CHURN_RISK' in df_exp.columns:
                    cr_opts = ['All'] + df_exp['CHURN_RISK'].dropna().unique().tolist()
                    sel_cr = st.selectbox("Churn Risk", cr_opts, key="e_cr")
                    if sel_cr != 'All':
                        df_exp = df_exp[df_exp['CHURN_RISK'] == sel_cr]
            with ef4:
                srch = st.text_input("🔎 Search merchant name", key="e_srch")
                if srch:
                    df_exp = df_exp[df_exp['MERCHANT_GROUP'].str.contains(srch.upper(), na=False)]

            active_count = len(df_exp)
            all_count    = len(run_ml(df_card, df_mon, df_target)) if (has_card and has_mon) else len(df_exp)
            if active_count < all_count:
                filter_pill(f"Filter Active: Showing {active_count:,} of {all_count:,} merchants")
            else:
                st.info(f"No filters applied — showing all **{active_count:,}** merchants.")

            show_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','CHURN_RISK',
                                      'TOTAL_SV','TOTAL_TRX','TOTAL_FBI','RASIO_ONUS',
                                      'WEEKS_ACTIVE','YTD_VOL','ACHIEVEMENT_PCT',
                                      'SV_GROWTH_RATE','ZSCORE_SV'] if c in df_exp.columns]
            es1, es2 = st.columns([3, 1])
            sort_e = es1.selectbox("Sort by", show_cols, key="e_sort")
            asc_e  = es2.radio("Order", ["Desc", "Asc"], horizontal=True, key="e_asc")
            df_exp_s = df_exp[show_cols].sort_values(sort_e, ascending=(asc_e == 'Asc')).reset_index(drop=True) \
                       if sort_e else df_exp[show_cols].reset_index(drop=True)
            st.dataframe(df_exp_s, use_container_width=True, height=480)
            st.download_button("⬇️ Export Filtered View as CSV",
                               df_exp_s.to_csv(index=False, encoding='utf-8-sig'),
                               "merchant_explorer_export.csv", "text/csv", type="primary")

    # ── AI Insights (formerly AI Insights tab) ────────────────────────────────
    with st.expander("🤖 AI Insights & Recommendations", expanded=False):
        if not has_mon_weekly:
            st.warning("⚠️ AI Insights require processed Monitoring Weekly data in the database.")
        else:
            df_ai_wk = df_mon_weekly[df_mon_weekly['YEAR'] == '2026'].copy()
            W_COLS = sorted([c for c in df_ai_wk.columns if c.startswith('W') and c[1:].isdigit()])
            if df_ai_wk.empty:
                st.info("ℹ️ No 2026 monitoring data found for the current filter.")
            else:
                # Derive current week number (used by Deep Dive risk score calculation)
                latest_wk_num = 0
                for _w in reversed(W_COLS):
                    if df_ai_wk[_w].fillna(0).sum() > 0:
                        latest_wk_num = int(_w[1:])
                        break

                # --- Deep Dive & Projection ---
                st.markdown("<br>", unsafe_allow_html=True)
                section_label("🔍 Deep Dive & Projection (Specific Merchant)")
                all_merch_ai = sorted(df_ai_wk['MERCHANT_GROUP'].unique().tolist())
                sel_merch = st.selectbox("Select Merchant Entity to Profile:", all_merch_ai, key="ai_sel_merch")
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
                    _remaining_months = max(0, 12 - len(merch_hist))
                    # Always forecast at least 6 months so the chart is visible even
                    # when all 12 months of current-year data are present.
                    _forecast_periods = _remaining_months if _remaining_months > 0 else 6
                    _hw_result = _hw_forecast(
                        merch_hist.sort_values('TRX_MONTH')['TOTAL_SV'] if not merch_hist.empty else pd.Series([], dtype=float),
                        periods_ahead=_forecast_periods
                    )
                    if _hw_result['success']:
                        proj_eoy          = ytd_actual + (_hw_result['projected_eoy'] if _remaining_months > 0 else 0)
                        _proj_method      = _hw_result['method']
                        _hw_forecast_vals = _hw_result['forecast']
                    else:
                        proj_eoy          = (ytd_actual / active_weeks_count * 52) if active_weeks_count > 0 else 0
                        _proj_method      = 'Estimated Run Rate'
                        _hw_forecast_vals = None
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
                        section_label(f"🤖 AI Insight Summary: {sel_merch}")
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
                        exec_icon  = "🟢" if rate_pct >= 100 else ("🟡" if rate_pct >= 80 else "🔴")
                        exec_label = "ON TRACK" if rate_pct >= 100 else ("AT RISK" if rate_pct >= 80 else "CRITICAL — INTERVENTION REQUIRED")
                        st.markdown(
                            f"""<div style="border-left:5px solid {exec_color};background:{exec_color}18;
                                border-radius:0 12px 12px 0;padding:16px 20px;margin-bottom:14px;">
                                <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;
                                            letter-spacing:.08em;color:{exec_color};">{exec_icon} STATUS: {exec_label}</div>
                                <div style="font-size:0.88rem;margin-top:8px;color:{_pp6['TEXT_PRI']};line-height:1.65;">
                                    <b>{sel_merch}</b> has accumulated <code>Rp {ytd_actual/1e9:,.2f}B</code> YTD across
                                    <b>{active_weeks_count}</b> active weeks.<br>{status_str}
                                </div>
                            </div>""", unsafe_allow_html=True
                        )
                        if seasonality_str != "No historical seasonality data found.":
                            st.markdown(
                                f"""<div style="background:{_pp6['SURFACE2']};border:1px solid {_pp6['BORDER']};
                                    border-radius:10px;padding:12px 16px;font-size:0.84rem;
                                    color:{_pp6['TEXT_PRI']};margin-bottom:14px;">
                                    <b>🌊 Seasonality Intelligence:</b> {seasonality_str}
                                </div>""", unsafe_allow_html=True
                            )
                        st.metric(
                            label=f"Projected Year-End Run Rate ({_proj_method})",
                            value=f"Rp {proj_eoy/1e9:,.2f} B",
                            delta=f"{rate_pct:.1f}% of Target",
                            delta_color="normal" if rate_pct >= 100 else "inverse"
                        )
                        st.markdown("<br>", unsafe_allow_html=True)
                        with st.expander("🧠 What's Driving This Merchant's Risk?", expanded=True):
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
                            with st.expander("🤖 Isolation Forest Feature Contribution (Model-Based)", expanded=True):
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
                                    "🔴 Red = feature drives the anomaly (neutralizing it lowers the score). "
                                    "🟢 Green = feature reduces anomaly risk. "
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
                            fig_sea.update_layout(height=350, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                            st.plotly_chart(fig_sea, use_container_width=True, theme=None)
                        else:
                            st.info(f"Insufficient historical Realisasi monthly data to chart statistical seasonality for {sel_merch}.")

                        # ── Holt-Winters Forecast Chart ───────────────────────
                        if _hw_forecast_vals is not None and len(_hw_forecast_vals) > 0:
                            hist_sv = merch_hist.sort_values('TRX_MONTH')[['TRX_MONTH', 'TOTAL_SV']].copy()
                            hist_sv['Label'] = hist_sv['TRX_MONTH'].astype(str)
                            hist_sv['Type']  = 'Historical'

                            # Generate future month labels (YYYYMM integers)
                            last_month = int(hist_sv['TRX_MONTH'].max())
                            future_months = []
                            y_m, m_m = last_month // 100, last_month % 100
                            for _ in range(len(_hw_forecast_vals)):
                                m_m += 1
                                if m_m > 12:
                                    m_m = 1
                                    y_m += 1
                                future_months.append(y_m * 100 + m_m)

                            fc_sv = pd.DataFrame({
                                'TRX_MONTH': future_months,
                                'TOTAL_SV':  _hw_forecast_vals,
                                'Label':     [str(x) for x in future_months],
                                'Type':      'Forecast'
                            })

                            combined = pd.concat([hist_sv, fc_sv], ignore_index=True)
                            fig_hw = px.line(
                                combined, x='Label', y='TOTAL_SV', color='Type',
                                title=f"Holt-Winters Forecast — {sel_merch}",
                                labels={'TOTAL_SV': 'Settlement Volume (Rp)', 'Label': 'Month'},
                                color_discrete_map={
                                    'Historical': get_palette()['GOLD'],
                                    'Forecast':   '#60A5FA'
                                }
                            )
                            fig_hw.update_traces(line=dict(width=2.5))
                            fig_hw.update_layout(
                                height=350,
                                **_chart_base(),
                                xaxis=dict(**_xaxis(), tickangle=-45),
                                yaxis=_yaxis(),
                                legend=dict(orientation="h", y=1.1)
                            )
                            st.plotly_chart(fig_hw, use_container_width=True, theme=None)
                            st.caption(f"Forecast model: {_proj_method}. Blue = model projection for remaining months of the year.")



# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARD SHARE
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    tab_desc("Monthly payment type breakdown — TRANSACTION / SALES VOLUME / FEE BASED INCOME. Data is sourced directly from the database and respects the sidebar Filters.")

    # KPIs from DB (already filtered in sidebar)
    if not df_card.empty:
        avg_onus = df_card['RASIO_ONUS'].mean() if 'RASIO_ONUS' in df_card.columns else 0
        st.markdown(f"""<div class="stats-grid">
            <div class="stat-card amber">
                <div class="stat-label">YTD Sales Volume</div>
                <div class="stat-value">Rp {df_card['TOTAL_SV'].sum()/1e9:,.1f}M</div>
                <div class="stat-meta">total sales</div>
            </div>
            <div class="stat-card green">
                <div class="stat-label">YTD Fee-Based Income</div>
                <div class="stat-value">Rp {df_card['TOTAL_FBI'].sum()/1e6:,.0f}Jt</div>
                <div class="stat-meta">fee income</div>
            </div>
            <div class="stat-card blue">
                <div class="stat-label">YTD Transactions</div>
                <div class="stat-value">{df_card['TOTAL_TRX'].sum()/1e6:,.2f}M</div>
                <div class="stat-meta">total transactions</div>
            </div>
            <div class="stat-card purple">
                <div class="stat-label">Avg On-Us Ratio</div>
                <div class="stat-value">{avg_onus*100:.1f}%</div>
                <div class="stat-meta">on-us share</div>
            </div>
        </div>""", unsafe_allow_html=True)

    # Reconstruct Monthly Matrix from PROCESSED_CARD_MONTHLY
    if has_monthly_tbl:
        if neon_url:
            df_monthly_raw = pd.read_sql_query("SELECT * FROM processed_card_monthly", engine)
            df_monthly_raw.columns = [c.upper() for c in df_monthly_raw.columns]
        else:
            conn = sqlite3.connect(PATH_DB)
            df_monthly_raw = pd.read_sql_query("SELECT * FROM PROCESSED_CARD_MONTHLY", conn)
            conn.close()
        
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
            st.info("ℹ️ No monthly trend data found for the current filter.")
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
                sel_yr = st.selectbox("📅 Year", ['All'] + [str(y) for y in avail_years], key="t1_year")
            with col_vm:
                chart_type = st.radio("📊 Chart Style", ["Stacked Bar", "Line Trend", "Both"], horizontal=True, key="t1_chart")

            if sel_yr != 'All':
                df_monthly_agg = df_monthly_agg[df_monthly_agg['YEAR'] == int(sel_yr)]

            # Core Payment Type Mapping
            SECTIONS = {
                'TRANSACTION':      ('🔄', BLUE_ACC, ['TRX_DEBIT_ONUS','TRX_DEBIT_OFFUS','TRX_CREDIT_OFFUS','TRX_QRIS_ONUS','TRX_QRIS_OFFUS'], 'TOTAL_TRX'),
                'SALES VOLUME':     ('💰', GREEN,    ['SV_DEBIT_ONUS','SV_DEBIT_OFFUS','SV_CREDIT_OFFUS','SV_QRIS_ONUS','SV_QRIS_OFFUS'],  'TOTAL_SV'),
                'FEE BASED INCOME': ('📈', AMBER,    ['FBI_DEBIT_ONUS','FBI_DEBIT_OFFUS','FBI_CREDIT_OFFUS','FBI_QRIS_ONUS','FBI_QRIS_OFFUS'],'TOTAL_FBI'),
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

                # YTD row (needed for donut chart and table)
                ytd_vals = display.drop(columns=['Bulan']).sum()

                # ── Charts first (visual priority) ─────────────────────────────
                ch_left, ch_right = st.columns([1, 1])
                with ch_left:
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
                            height=340, margin=dict(l=0, r=0, t=36, b=64),
                            xaxis=dict(tickangle=-30),
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
                            height=340, margin=dict(l=0, r=0, t=36, b=64),
                            xaxis=dict(tickangle=-30),
                            **_chart_base(),
                        )
                        st.plotly_chart(fig_l, use_container_width=True, theme=None)
                with ch_right:
                    section_label("🍩 Mix Composition (Selected Period)")
                    fig_pie = px.pie(
                        values=ytd_vals.drop('TOTAL'),
                        names=[clean_map[c] for c in valid_sub],
                        hole=0.6,
                        color=[clean_map[c] for c in valid_sub],
                        color_discrete_map=PAYMENT_COLORS
                    )
                    fig_pie.update_layout(height=340, margin=dict(t=10, b=50, l=10, r=10), **_chart_base())
                    st.plotly_chart(fig_pie, use_container_width=True, theme=None)

                # ── Monthly breakdown table in expander (drill-down) ────────────
                with st.expander("📋 View Monthly Breakdown Table"):
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

                styled_divider()
    else:
        conn.close()
        st.warning("⚠️ PROCESSED_CARD_MONTHLY table is missing. Re-run the Automated Pipeline.")

    styled_divider()


    # Top Merchants overview from DB
    if not df_card.empty:
        section_label("🏆 Top Merchants Analytics (YTD)")
        
        # Create a rich dataframe with calculated metrics
        df_c = df_card.copy()
        df_c['AVG_TRX_VAL'] = np.where(df_c['TOTAL_TRX'] > 0, df_c['TOTAL_SV'] / df_c['TOTAL_TRX'], 0)
        df_c['FBI_YIELD'] = np.where(df_c['TOTAL_SV'] > 0, (df_c['TOTAL_FBI'] / df_c['TOTAL_SV']) * 100, 0)
        
        top_n_c = st.slider("Top N Merchants", 10, 50, 20, key="t1_topn")

        df_top = df_c.sort_values('TOTAL_SV', ascending=False).head(top_n_c)
        
        # Format display dataframe
        disp_top = df_top[['MERCHANT_GROUP', 'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'AVG_TRX_VAL', 'FBI_YIELD', 'RASIO_ONUS']].copy()
        
        # Add formatted strings
        format_dict = {
            'Sales Volume': lambda x: f"Rp {x/1e9:,.2f} M",
            'Fee Based Income': lambda x: f"Rp {x/1e6:,.1f} Jt",
            'Transactions': lambda x: f"{x:,.0f}",
            'Avg Trx Size': lambda x: f"Rp {x:,.0f}",
            'FBI Yield': lambda x: f"{x:.4f}%",
            'On-Us Ratio': lambda x: f"{x*100:.1f}%",
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

        with st.expander("📋 Raw Card Share Data"):
            st.dataframe(df_c.reset_index(drop=True), use_container_width=True)
            st.download_button("⬇️ Download CSV", df_c.to_csv(index=False, encoding='utf-8-sig'), "card_share_data.csv", "text/csv")

        # ── GROWTH ANALYTICS (Realisasi) ──────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        # We can now use df_card_hist from DB for growth instead of re-parsing Excel
        if not df_card_hist.empty:
            max_month = df_card_hist['TRX_MONTH'].max()
            try:
                curr_yr = int(str(max_month)[:4])
                curr_mo = int(str(max_month)[4:])
                prev_yr = curr_yr - 1
                prev_month = int(f"{prev_yr}{curr_mo:02d}")
                
                MONTH_ABB = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                col_curr = f"{MONTH_ABB[curr_mo-1]}-{str(curr_yr)[2:]}"
                col_prev = f"{MONTH_ABB[curr_mo-1]}-{str(prev_yr)[2:]}"
                col_fy_prev = f"FY-{str(prev_yr)[2:]}"
                
                gh1, gh2 = st.columns([3, 1])
                with gh1:
                    section_label("📈 Top & Bottom Merchant Growth (MoM YoY)")
                    _freshness_txt = f"📅 Comparing **{col_curr}** vs **{col_prev}** (year-ago same month)"
                    if _last_update != 'Unknown':
                        _freshness_txt += f" | Last pipeline run: **{_last_update}**"
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
                # FY Previous
                df_fy = df_card_hist[df_card_hist['YEAR'] == prev_yr].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_fy_prev)
                
                df_growth = pd.merge(df_curr, df_prev, on='MERCHANT_GROUP', how='outer')
                df_growth = pd.merge(df_growth, df_fy, on='MERCHANT_GROUP', how='outer').fillna(0)
                
                df_growth['Delta'] = df_growth[col_curr] - df_growth[col_prev]
                df_growth['Growth %'] = np.where(df_growth[col_prev] > 0, 
                                                (df_growth['Delta'] / df_growth[col_prev]) * 100, 
                                                np.where(df_growth[col_curr] > 0, 100, 0))
                
                df_growth = df_growth[(df_growth[col_curr] > 0) | (df_growth[col_prev] > 0) | (df_growth[col_fy_prev] > 0)]
                top_10 = df_growth.sort_values('Growth %', ascending=False).head(10)
                bot_10 = df_growth.sort_values('Growth %', ascending=True).head(10)
                
                def val_fmt_g(x):
                    if 'TOTAL_TRX' in m_col: return f"{x:,.0f}"
                    if x >= 1e9 or x <= -1e9: return f"{x/1e9:,.2f} M"
                    return f"{x/1e6:,.0f} Jt"
                
                def style_growth(row):
                    styles = [''] * len(row)
                    pct = row['Growth %']
                    color = '#27AE60' if pct > 0 else ('#EB5757' if pct < 0 else '#888')
                    styles[4] = f'color: {color}; font-weight: bold;'
                    styles[5] = f'color: {color}; font-weight: bold;'
                    return styles
                    
                formatters = {
                    col_curr: val_fmt_g, col_prev: val_fmt_g, col_fy_prev: val_fmt_g,
                    'Delta': val_fmt_g, 'Growth %': lambda x: f"{x:,.0f}%"
                }
                
                c1, c2 = st.columns(2)
                with c1:
                    section_label(f"🟢 Top 10 by {metric_sel} Growth")
                    st.dataframe(top_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), use_container_width=True)
                with c2:
                    section_label(f"🔴 Bottom 10 by {metric_sel} Growth")
                    st.dataframe(bot_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), use_container_width=True)
            except Exception as e:
                st.error(f"Growth calculation failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WEEKLY MONITORING (reads from '2026' sheet directly)
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    tab_desc("Weekly monitoring — merchant-level weekly matrix read directly from the <b>2026</b> sheet. "
             "Filter by <b>PM</b> and <b>Metric</b> (TRX / VOL / FBI) to drill down.")

    if not has_mon_weekly:
        st.warning("⚠️ Weekly Monitoring database table is missing. Please run the Automated Pipeline first.")
    else:
        # ── 1. Filters ───────────
        f_col1, f_col2, f_col3 = st.columns([1, 1, 1])
        
        with f_col1:
            avail_years_mon = []
            if not df_mon_weekly.empty and 'YEAR' in df_mon_weekly.columns:
                avail_years_mon = sorted(df_mon_weekly['YEAR'].unique().tolist(), reverse=True)
            
            sel_yr_mon = st.selectbox("📅 Year", [str(y) for y in avail_years_mon] if avail_years_mon else ["No Data"], key="t2_year_mon")
            df_mon_yr = df_mon_weekly[df_mon_weekly['YEAR'] == str(sel_yr_mon)] if (not df_mon_weekly.empty and 'YEAR' in df_mon_weekly.columns) else pd.DataFrame()
        
        with f_col2:
            pm_names_mon = []
            if not df_mon_yr.empty and 'PM' in df_mon_yr.columns:
                pm_names_mon = sorted([p for p in df_mon_yr['PM'].dropna().unique() if str(p).strip().upper() not in ['NAN', 'NONE', 'UNKNOWN', 'UNASSIGNED']])
            sel_pm_mon = st.selectbox("👤 Filter by PM", ["All PMs"] + pm_names_mon, key="t2_pm_mon")
        
        with f_col3:
            avail_ket_mon = sorted(df_mon_yr['DIMENSI'].dropna().unique()) if (not df_mon_yr.empty and 'DIMENSI' in df_mon_yr.columns) else []
            sel_ket_mon = st.multiselect("📊 Metric (Dimensi)", avail_ket_mon, default=avail_ket_mon, key="t2_ket_mon")

        # ── 2. Data Processing ───────────
        df_filt_mon = df_mon_yr.copy()
        if sel_pm_mon != "All PMs":
            df_filt_mon = df_filt_mon[df_filt_mon['PM'] == sel_pm_mon]
        if sel_ket_mon:
            df_filt_mon = df_filt_mon[df_filt_mon['DIMENSI'].isin(sel_ket_mon)]
        
        W_COLS_DB = sorted([c for c in df_filt_mon.columns if c.startswith('W') and c[1:].isdigit()])
        
        if df_filt_mon.empty:
            st.info("ℹ️ No monitoring data matches the current filters.")
        else:
            grp_cols_mon = ['MERCHANT_GROUP', 'DIMENSI', 'PM', 'FY', 'YTD']
            avail_grp_mon = [c for c in grp_cols_mon if c in df_filt_mon.columns]
            
            # YTD from df_mon_weekly is often object dtype — coerce before summing
            _total_ytd_mon = pd.to_numeric(
                df_filt_mon[df_filt_mon['DIMENSI'] == 'VOL']['YTD'], errors='coerce'
            ).sum()
            _total_ytd_mon = float(_total_ytd_mon) if not pd.isna(_total_ytd_mon) else 0.0
            
            def fmt_ytd_mon(v):
                if v >= 1e12: return f"Rp {v/1e12:,.2f}T"
                if v >= 1e9:  return f"Rp {v/1e9:,.1f}M"
                if v >= 1e6:  return f"Rp {v/1e6:,.0f}Jt"
                return f"{v:,.0f}"

            st.markdown(f"""<div class="stats-grid" style="grid-template-columns:repeat(3,1fr);">
                <div class="stat-card green">
                    <div class="stat-label">Filtered YTD Volume</div>
                    <div class="stat-value">{fmt_ytd_mon(_total_ytd_mon)}</div>
                    <div class="stat-meta">volume total</div>
                </div>
                <div class="stat-card blue">
                    <div class="stat-label">Selected Year</div>
                    <div class="stat-value">{sel_yr_mon}</div>
                    <div class="stat-meta">fiscal year</div>
                </div>
                <div class="stat-card purple">
                    <div class="stat-label">Total Metrics Tracked</div>
                    <div class="stat-value">{len(df_filt_mon)}</div>
                    <div class="stat-meta">metric rows</div>
                </div>
            </div>""", unsafe_allow_html=True)

            # ── 3. Trend & Visuals (visuals-first) ───────────
            st.markdown("<br>", unsafe_allow_html=True)
            section_label("📈 Weekly Aggregated Trend")

            _WEEKLY_PALETTE = [
                "#2563EB","#DC2626","#16A34A","#D97706","#7C3AED",
                "#0891B2","#BE185D","#65A30D","#EA580C","#4338CA",
            ]

            avail_dim = sorted(df_filt_mon['DIMENSI'].unique().tolist())
            sel_dim = st.multiselect("📊 Filter Metrics (DIMENSI)", avail_dim, default=avail_dim, key="t2_dim_filter")
            df_filt_for_plot = df_filt_mon[df_filt_mon['DIMENSI'].isin(sel_dim)] if sel_dim else df_filt_mon

            all_merch_mon = sorted(df_filt_for_plot['MERCHANT_GROUP'].unique().tolist())
            _vol_rows = df_filt_for_plot[df_filt_for_plot['DIMENSI']=='VOL'] if 'VOL' in sel_dim else df_filt_for_plot
            def_merch_mon = _vol_rows.sort_values('YTD', ascending=False)['MERCHANT_GROUP'].head(5).tolist()

            sel_plot_merch = st.multiselect("🔍 Select Merchants to Plot", all_merch_mon, default=def_merch_mon, key="t2_plot_merch")

            if sel_plot_merch:
                df_plot_mon = df_filt_for_plot[df_filt_for_plot['MERCHANT_GROUP'].isin(sel_plot_merch)].copy()
                # Truncate long merchant names so labels don't crash the chart
                def _abbrev(name, n=16):
                    return name if len(name) <= n else name[:n].rstrip() + '…'
                df_plot_mon['LABEL'] = df_plot_mon['MERCHANT_GROUP'].apply(_abbrev) + ' (' + df_plot_mon['DIMENSI'] + ')'
                df_long_mon = df_plot_mon.melt(id_vars='LABEL', value_vars=W_COLS_DB, var_name='Week', value_name='Value')
                df_long_mon = df_long_mon.sort_values(['LABEL', 'Week'])

                fig_trend_mon = px.line(
                    df_long_mon, x='Week', y='Value', color='LABEL', markers=True,
                    title=f"Weekly Trend Analysis — {sel_yr_mon}",
                    color_discrete_sequence=_WEEKLY_PALETTE,
                )
                fig_trend_mon.update_layout(
                    height=480,
                    margin=dict(l=0, r=0, t=36, b=80),
                    legend=dict(orientation='h', y=-0.35),
                    **_chart_base(),
                )
                fig_trend_mon.update_xaxes(tickangle=-45, **_xaxis())
                st.plotly_chart(fig_trend_mon, use_container_width=True, theme=None)

                # Heatmap
                st.markdown("<br>", unsafe_allow_html=True)
                section_label("🔥 Performance Heatmap")
                heat_data_mon = df_plot_mon.set_index('LABEL')[W_COLS_DB].fillna(0).apply(pd.to_numeric, errors='coerce').fillna(0)

                fig_heat_mon = px.imshow(
                    heat_data_mon,
                    color_continuous_scale='Viridis',
                    aspect='auto',
                    title=f"Weekly Performance Patterns — {sel_yr_mon}"
                )
                n_rows = len(heat_data_mon)
                fig_heat_mon.update_layout(
                    height=max(280, 36 * n_rows + 120),
                    margin=dict(l=180, r=20, t=50, b=40),
                    **_chart_base(),
                )
                fig_heat_mon.update_yaxes(tickfont=dict(size=11))
                st.plotly_chart(fig_heat_mon, use_container_width=True, theme=None)

            # ── 4. Main Data Matrix (details below charts) ───────────
            st.markdown("<br>", unsafe_allow_html=True)
            section_label(f"🗓️ Weekly Matrix — {sel_yr_mon}")
            st.dataframe(df_filt_mon[avail_grp_mon + W_COLS_DB].fillna(0).reset_index(drop=True), use_container_width=True, height=400)

            st.download_button("⬇️ Export Table",
                df_filt_mon[avail_grp_mon + W_COLS_DB].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                f"monitoring_{sel_yr_mon}_export.csv", "text/csv")




with tab3:
    tab_desc("Merchant Segmentation Profiler — automatically groups your portfolio into performance tiers based on volume, growth, fee income, and target achievement. Identify which merchants to prioritize, nurture, or investigate.")

    if not (has_card and has_mon):
        st.warning("⚠️ Merchant segmentation requires **both** Card Share and Monitoring data to be processed first.")
    else:
        with st.expander("⚙️ Advanced: Adjust Segmentation Granularity", expanded=False):
            k_val = st.slider("Number of Groups", min_value=3, max_value=5, value=3,
                              help="3 = broad view (fewer, larger groups). 5 = detailed view (more precise tiers). Start with 3 if you're unsure.")
        if 'k_val' not in dir():
            k_val = 3
        with st.spinner("Analyzing merchant performance tiers..."):
            df_ml = run_ml(df_card, df_mon, df_target, k_clusters=k_val)

        if df_ml.empty:
            st.info("ℹ️ No data available for Machine Learning analysis. Please ensure the database has been populated.")
        else:
            all_pm_ml = sorted(df_ml['PM'].dropna().unique().tolist()) if 'PM' in df_ml.columns else []
            all_clusters = sorted(df_ml['CLUSTER'].dropna().unique().tolist())

            # Controls
            mc1, mc2 = st.columns(2)
            with mc1:
                sel_pm_ml = st.multiselect("👤 Filter by PM", all_pm_ml, default=all_pm_ml, key="t3_pm")
            with mc2:
                sel_clust = st.multiselect("🏷️ Show Clusters", all_clusters, default=all_clusters, key=f"t3_clust_{k_val}")

            df_f = df_ml[df_ml['CLUSTER'].isin(sel_clust)]
            if sel_pm_ml and 'PM' in df_f.columns:
                df_f = df_f[df_f['PM'].isin(sel_pm_ml)]

            filtered = len(sel_pm_ml) < len(all_pm_ml) or len(sel_clust) < len(all_clusters)
            if filtered:
                filter_pill(f"Filter Active: {len(df_f)} of {len(df_ml)} merchants shown")
            else:
                tab_desc(f"Showing all <b>{len(df_f)}</b> merchants across all clusters.")

            # Cluster counts
            cols = st.columns(len(all_clusters))
            
            # Color mapper fallback
            color_lookup = {
                'ELITE': '#F1C40F', 'PREMIUM': '#27AE60', 'REGULER': '#2F80ED', 
                'PASIF': '#EB5757', 'DORMANT': '#888888'
            }
            fallback_colors = ['#27AE60', '#2F80ED', '#EB5757', '#F39C12', '#9B59B6', '#34495E']
            
            # ── Segment metric grid with action recommendations ─────────────────
            SEGMENT_ICONS = {
                'ELITE': '👑', 'PREMIUM': '🌟', 'REGULER': '🔵',
                'PASIF': '🔴', 'DORMANT': '⚫',
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

            tier_cols = st.columns(max(len(all_clusters), 1))
            for idx, seg in enumerate(all_clusters):
                n = len(df_f[df_f['CLUSTER'] == seg])
                pct = (n / total_merchants * 100) if total_merchants > 0 else 0
                icon = SEGMENT_ICONS.get(seg, '🔹')
                action = SEGMENT_ACTIONS.get(seg, "Review this group with your PM team.")
                high_in_seg = len(df_ml[(df_ml['CLUSTER'] == seg) & (df_ml['CHURN_RISK'] == 'HIGH RISK ⚠️')]) if not df_ml.empty and 'CHURN_RISK' in df_ml.columns else 0
                with tier_cols[idx]:
                    st.metric(label=f"{icon} {seg}", value=n, delta=f"{pct:.1f}% of fleet")
                    if high_in_seg > 0:
                        st.warning(f"⚠️ {high_in_seg} need attention")
                    st.caption(f"💡 {action}")

            # ── Grouping Confidence (business-friendly silhouette translation) ──
            if 'SILHOUETTE_SCORE' in df_ml.columns:
                sil = float(df_ml['SILHOUETTE_SCORE'].iloc[0])
                if sil > 0.5:
                    sil_label, sil_color = "High ✅ — Groups are well-defined and distinct", "#34D399"
                elif sil > 0.25:
                    sil_label, sil_color = "Moderate — Groups are reasonable; consider adjusting granularity", "#FBBF24"
                else:
                    sil_label, sil_color = "Low — Groups overlap; try a different grouping level", "#F87171"
                st.markdown(
                    f"""<div style="padding:10px 16px;border-radius:10px;border:1px solid {sil_color};
                        background:{sil_color}18;margin-bottom:12px;">
                        <b>Grouping Confidence:</b>
                        <span style="color:{sil_color};font-weight:bold;font-size:1.05rem;margin-left:8px;">{sil_label}</span>
                    </div>""",
                    unsafe_allow_html=True
                )

            st.markdown("")
            sc1, sc2 = st.columns(2)

            with sc1:
                counts = df_f['CLUSTER'].value_counts().reset_index()
                counts.columns = ['CLUSTER','COUNT']
                fig_pie = px.pie(counts, names='CLUSTER', values='COUNT', hole=0.45,
                                 title=f'Merchant Segmentation (K={k_val})',
                                 color='CLUSTER', color_discrete_map=color_lookup)
                fig_pie.update_layout(height=450, **_chart_base())
                st.plotly_chart(fig_pie, use_container_width=True, theme=None)

            with sc2:
                fig_sc = px.scatter_3d(df_f, x='AVG_SV', y='AVG_FBI', z='SV_GROWTH_CLIPPED',
                                    color='CLUSTER', hover_name='MERCHANT_GROUP',
                                    hover_data=['PM','ACHIEVEMENT_PCT','WEEKS_ACTIVE'],
                                    title="Merchant Performance Map",
                                    labels={'AVG_SV': 'Monthly Volume', 'AVG_FBI': 'Fee Income', 'SV_GROWTH_CLIPPED': 'Growth Trend'},
                                    color_discrete_map=color_lookup)
                fig_sc.update_layout(height=450, margin=dict(l=0, r=0, b=48, t=30), **_chart_base())
                st.plotly_chart(fig_sc, use_container_width=True, theme=None)

            section_label("Tier Characteristic Profile")
            radar_m = ['AVG_SV','AVG_FBI','RASIO_ONUS','ACHIEVEMENT_PCT','WEEKS_ACTIVE']
            _radar_labels = {
                'AVG_SV':         'Monthly Volume',
                'AVG_FBI':        'Fee Income',
                'RASIO_ONUS':     'On-Us Share',
                'ACHIEVEMENT_PCT':'Target Achievement',
                'WEEKS_ACTIVE':   'Activity Weeks',
            }
            cm = df_f.groupby('CLUSTER')[radar_m].mean()
            norm = (cm - cm.min()) / (cm.max() - cm.min() + 1e-9)
            norm.columns = [_radar_labels[c] for c in norm.columns]
            fig_r = go.Figure()
            for clust in all_clusters:
                if clust in norm.index:
                    fig_r.add_trace(go.Bar(
                        y=list(norm.columns),
                        x=norm.loc[clust].tolist(),
                        name=clust,
                        orientation='h',
                        marker_color=color_lookup.get(clust, '#888'),
                        hovertemplate='<b>%{fullData.name}</b><br>%{y}: %{x:.2f}<extra></extra>',
                    ))
            _pp = _p()
            fig_r.update_layout(
                barmode='group',
                height=430,
                title="How each merchant tier scores across key business metrics",
                xaxis=dict(title="Normalised Score (0–1)", range=[0, 1], **_xaxis()),
                yaxis=dict(title="Metric", **_yaxis()),
                legend=dict(orientation='h', y=-0.18),
                **_chart_base(),
            )
            st.plotly_chart(fig_r, use_container_width=True, theme=None)

            if 'PM' in df_f.columns:
                section_label("Account Manager × Merchant Tier Breakdown")
                pm_cl = df_f.groupby(['PM','CLUSTER']).size().reset_index(name='COUNT')
                fig_stk = px.bar(pm_cl, x='PM', y='COUNT', color='CLUSTER',
                                 barmode='stack', title="Merchant Tier Distribution per Account Manager",
                                 color_discrete_map=color_lookup)
                fig_stk.update_layout(height=380, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                st.plotly_chart(fig_stk, use_container_width=True, theme=None)

            with st.expander("📋 View ML Results Table"):
                show_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','RISK_SCORE',
                                         'AVG_SV','AVG_FBI','ACHIEVEMENT_PCT',
                                         'WEEKS_ACTIVE','ZSCORE_SV','ZSCORE_FBI','ZSCORE_GROWTH'] if c in df_f.columns]
                st.dataframe(df_f[show_cols].sort_values('RISK_SCORE', ascending=False).reset_index(drop=True), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CHURN & RISK
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    tab_desc("Proactive health monitoring for your merchant portfolio. Merchants needing attention are surfaced here based on volume trends, growth trajectory, and target achievement — so your team always knows where to focus.")

    if not (has_card and has_mon):
        st.warning("⚠️ Health alerts require both Card Share and Monitoring data.")
    else:
        with st.expander("⚙️ Advanced: Adjust Detection Sensitivity", expanded=False):
            z_col, _ = st.columns([1, 2])
            z_thresh_val = z_col.slider(
                "Detection Sensitivity",
                min_value=-3.0, max_value=-0.5, value=-1.2, step=0.1,
                help="Higher sensitivity (closer to -0.5) flags more merchants for review. Lower (-2.0 or below) flags only the most extreme outliers.",
            )
        if 'z_thresh_val' not in dir():
            z_thresh_val = -1.2
        
        df_churn_all = run_ml(df_card, df_mon, df_target, z_thresh=z_thresh_val)
        
        if df_churn_all.empty:
            st.info("ℹ️ No data available for Churn and Risk analysis.")
        else:
            all_pm_c = sorted(df_churn_all['PM'].dropna().unique().tolist()) if 'PM' in df_churn_all.columns else []

            # Controls — inline
            ch1, ch2 = st.columns([3,1])
            with ch1:
                sel_pm_c = st.multiselect("👤 Filter by PM", all_pm_c, default=all_pm_c, key="t4_pm")
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

            df_high   = df_c4[df_c4['CHURN_RISK'] == 'HIGH RISK ⚠️']
            df_medium = df_c4[df_c4['CHURN_RISK'] == 'MEDIUM RISK 🟡']
            df_safe   = df_c4[df_c4['CHURN_RISK'] == 'STABLE ✅']
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

            # ── Action Inbox ──────────────────────────────────────────────────
            _at_risk_inbox = pd.concat([df_high, df_medium], ignore_index=True)
            if not _at_risk_inbox.empty and 'RISK_SCORE' in _at_risk_inbox.columns:
                _at_risk_inbox = _at_risk_inbox.sort_values('RISK_SCORE', ascending=False).head(7)
                _pp4_inbox = _p()
                inbox_rows = ""
                for _, row in _at_risk_inbox.iterrows():
                    _cr = row.get('CHURN_RISK', '')
                    _pm_name = row.get('PM', 'N/A')
                    _is_high = 'HIGH' in str(_cr)
                    _is_if   = bool(row.get('IF_IS_ANOMALY', False))
                    _ach     = row.get('ACHIEVEMENT_PCT', 0)
                    _growth  = row.get('SV_GROWTH_RATE', 0)
                    _row_color = "#F87171" if _is_high else "#FBBF24"
                    _icon      = "🔴" if _is_high else "🟡"
                    if _is_high and _is_if:
                        _reason = "Flagged by 2 independent detection methods — highest confidence alert"
                        _action = "Escalate to PM immediately; schedule merchant call this week"
                    elif _is_high:
                        _reason = f"Volume or growth significantly below fleet average (Achievement: {_ach:.0f}%)"
                        _action = "PM to conduct business review; investigate operational issues"
                    elif _ach < 60:
                        _reason = f"Below 60% of yearly target (Achievement: {_ach:.0f}%)"
                        _action = "Schedule business review; consider promotional support"
                    else:
                        _reason = f"Growth trend declining (MoM: {_growth*100:.1f}%)"
                        _action = "Monitor weekly; check for competitive pressure"
                    inbox_rows += f"""<div style="padding:12px 16px;border-left:4px solid {_row_color};
                        background:{_row_color}0d;margin-bottom:8px;border-radius:0 8px 8px 0;">
                        <div style="display:flex;justify-content:space-between;align-items:center;">
                            <div>
                                <span style="font-size:0.85rem;font-weight:700;color:{_pp4_inbox['TEXT_PRI']};">{_icon} {row['MERCHANT_GROUP']}</span>
                                <span style="font-size:0.75rem;color:#888;margin-left:10px;">PM: {_pm_name}</span>
                            </div>
                        </div>
                        <div style="font-size:0.78rem;color:{_pp4_inbox['TEXT_SEC']};margin-top:4px;">{_reason}</div>
                        <div style="font-size:0.78rem;color:{_row_color};margin-top:3px;font-weight:600;">→ {_action}</div>
                    </div>"""
                st.markdown(
                    f"""<div style="border:1px solid {_pp4_inbox['BORDER']};border-radius:12px;
                        padding:16px;margin:16px 0;">
                        <div style="font-size:0.9rem;font-weight:700;margin-bottom:12px;
                            color:{_pp4_inbox['TEXT_PRI']};">🔔 Action Inbox — {len(_at_risk_inbox)} merchants need attention</div>
                        {inbox_rows}
                    </div>""",
                    unsafe_allow_html=True,
                )

            st.markdown("")

            if total > 0:
                # ── Risk Score Distribution ───────────────────────────────────────
                if 'RISK_SCORE' in df_c4.columns:
                    section_label("Portfolio Health Distribution")
                    _df_c4_disp = df_c4.copy()
                    if 'CHURN_RISK' in _df_c4_disp.columns:
                        _df_c4_disp['Health Status'] = _df_c4_disp['CHURN_RISK'].replace({
                            'HIGH RISK ⚠️':   'Action Required',
                            'MEDIUM RISK 🟡': 'Monitor Closely',
                            'STABLE ✅':       'On Track',
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
                        "📊 **What is the Health Score?** A composite 0–100 score based on three business signals: "
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
                        "📌 **How to read this:** The gauge shows what percentage of your merchant fleet is currently "
                        "flagged as high-risk. The number below the gauge (+/− X%) is how far you are from the "
                        "20% portfolio target — negative means fewer at-risk merchants (good), positive means more (needs action)."
                    )

                with ch_right_kpi:
                    risk_label = "🟢 FLEET HEALTHY" if rate < 20 else ("🟡 NEEDS ATTENTION" if rate < 45 else "🔴 CRITICAL — ACT NOW")
                    risk_color = "#34D399" if rate < 20 else ("#FBBF24" if rate < 45 else "#F87171")
                    
                    # Dynamic risk advisory text
                    def churn_advisory(pct):
                        if pct >= 75:
                            return "🚨 **CRITICAL:** Immediate intervention required. Recommend emergency fee discounts, PM outreach blitz, and escalation to senior leadership."
                        elif pct >= 45:
                            return "⚠️ **HIGH RISK:** Portfolio is deteriorating. Recommend targeted retention offers, dedicated PM follow-ups for flagged merchants, and weekly monitoring cadence."
                        elif pct >= 20:
                            return "🟡 **ELEVATED:** Above benchmark. Recommend proactive check-ins with declining merchants and review of competitive positioning."
                        else:
                            return "✅ **STABLE:** Portfolio churn is within healthy benchmarks. Continue standard monitoring and quarterly business reviews."
                    
                    advisory = churn_advisory(rate)
                    st.markdown(
                        f"""<div style="margin-top:24px;padding:20px;border-radius:14px;
                            border:2px solid {risk_color};background:{risk_color}18;text-align:center;">
                            <div style="font-size:2.2rem;">{risk_label}</div>
                            <div style="font-size:0.8rem;color:{_pp4['TEXT_SEC']};margin-top:10px;">
                            {len(df_high)} of {total} merchants flagged as high-risk.<br>
                            Benchmark target: &lt;20% portfolio churn.
                            </div>
                        </div>""", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""<div style="margin-top:12px;padding:14px 16px;border-radius:10px;
                            background:{_pp4['SURFACE2']};border:1px solid {_pp4['BORDER']};
                            font-size:0.84rem;color:{_pp4['TEXT_PRI']};line-height:1.55;">
                            <b>AI Recommendation:</b><br>{advisory}
                        </div>""", unsafe_allow_html=True
                    )
                    
                # ── Chart Data Audit — full-width row, isolated from gauge/KPI columns ──
                with st.expander("🔬 Chart Data Audit", expanded=False):
                    st.caption("Raw aggregates feeding the gauge and donut charts:")
                    audit_data = {
                        "Metric": ["High Risk Count", "Stable Count", "Total", "Churn Rate %"],
                        "Value": [str(len(df_high)), str(len(df_safe)), str(total), f"{rate:.2f}%"],
                    }
                    st.dataframe(pd.DataFrame(audit_data), hide_index=True, use_container_width=True)
                    if 'CHURN_RISK' in df_c4.columns:
                        st.write("CHURN_RISK value_counts:")
                        st.dataframe(df_c4['CHURN_RISK'].value_counts().reset_index(), hide_index=True)

                # ── Donut + PM bar as before ──────────────────────────────────────
                ch_x, ch_y = st.columns(2)
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

                if 'ZSCORE_SV' in df_c4.columns:
                    st.markdown("<br>", unsafe_allow_html=True)
                    with st.expander("📊 Statistical Detail — Volume, Fee & Growth Outlier Analysis", expanded=False):
                        st.caption("These charts show the distribution of merchant performance metrics. Red-shaded merchants fall below the detection threshold set in Advanced Settings.")
                        z1, z2, z3 = st.columns(3)

                        def _draw_z_hist(df, col_name, title, threshold):
                            fig_z = px.histogram(df, x=col_name, color='CHURN_RISK',
                                                 nbins=25, barmode='overlay',
                                                 color_discrete_map={'HIGH RISK ⚠️': RED, 'STABLE ✅': BLUE_ACC},
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
                    st.markdown("<br>", unsafe_allow_html=True)
                    section_label("📉 What's Driving the Alerts? — Key Risk Factors Across Portfolio")
                    st.caption(f"Analyzing {len(_if_flagged)} flagged merchant(s). Higher bars = the metric most responsible for triggering alerts. Use this to guide where your team should focus.")
                    _fleet_lofo = _if_flagged[list(_if_lofo_cols.keys())].mean().rename(_if_lofo_cols)
                    _fleet_lofo_df = _fleet_lofo.reset_index()
                    _fleet_lofo_df.columns = ['Feature', 'Avg Contribution']
                    _fleet_lofo_df = _fleet_lofo_df.sort_values('Avg Contribution', ascending=True)
                    _fl_colors = ['#F87171' if v > 0 else '#34D399' for v in _fleet_lofo_df['Avg Contribution']]
                    fig_fleet_lofo = go.Figure(go.Bar(
                        x=_fleet_lofo_df['Avg Contribution'],
                        y=_fleet_lofo_df['Feature'],
                        orientation='h',
                        marker_color=_fl_colors,
                        marker_line_width=0,
                        text=[f"{v:+.4f}" for v in _fleet_lofo_df['Avg Contribution']],
                        textposition='outside',
                        hovertemplate='<b>%{y}</b><br>Avg LOFO Delta: <b>%{x:+.4f}</b><extra></extra>',
                    ))
                    _pp4b = _p()
                    fig_fleet_lofo.update_layout(
                        title='Which Business Metric Is Driving the Most Alerts?',
                        height=300,
                        margin=dict(l=0, r=80, t=44, b=32),
                        xaxis=dict(title='Avg Anomaly Score Delta', showgrid=False,
                                   tickfont=dict(color=_pp4b['TEXT_SEC'])),
                        yaxis=dict(showgrid=False, tickfont=dict(color=_pp4b['TEXT_PRI'])),
                        **_chart_base(),
                    )
                    st.plotly_chart(fig_fleet_lofo, use_container_width=True, theme=None)

            # Show HIGH + MEDIUM merchants sorted by Risk Score descending
            df_at_risk = pd.concat([df_high, df_medium], ignore_index=True)
            if len(df_at_risk) > 0:
                section_label("📋 Merchant Detail — Action Required & Monitor Closely")

                # ── Highest-confidence dual-flagged alert ─────────────────────
                if 'IF_IS_ANOMALY' in df_at_risk.columns:
                    ensemble_hits = df_at_risk[
                        (df_at_risk['CHURN_RISK'] == 'HIGH RISK ⚠️') &
                        (df_at_risk['IF_IS_ANOMALY'] == True)
                    ]
                    if len(ensemble_hits) > 0:
                        names = ', '.join(ensemble_hits['MERCHANT_GROUP'].tolist())
                        st.error(
                            f"🚨 **HIGHEST PRIORITY — {len(ensemble_hits)} merchant(s) confirmed by 2 independent methods:** {names}\n\n"
                            f"These merchants are flagged by both trend analysis AND anomaly detection, giving the highest confidence that immediate action is needed. "
                            f"**Recommended: assign PM for direct outreach this week.**"
                        )

                risk_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','CHURN_RISK','RISK_SCORE',
                                          'WEEKS_ACTIVE','SV_GROWTH_RATE',
                                          'ACHIEVEMENT_PCT','ZSCORE_SV','ZSCORE_FBI','ZSCORE_GROWTH',
                                          'IF_ANOMALY_SCORE','IF_IS_ANOMALY'] if c in df_at_risk.columns]
                df_rd = df_at_risk[risk_cols].sort_values('RISK_SCORE', ascending=False).copy() if 'RISK_SCORE' in df_at_risk.columns else df_at_risk[risk_cols].copy()
                if 'SV_GROWTH_RATE' in df_rd.columns:
                    df_rd['SV_GROWTH_RATE'] = (df_rd['SV_GROWTH_RATE']*100).round(1).astype(str)+'%'
                if 'ACHIEVEMENT_PCT' in df_rd.columns:
                    df_rd['ACHIEVEMENT_PCT'] = df_rd['ACHIEVEMENT_PCT'].round(1).astype(str)+'%'
                if 'CHURN_RISK' in df_rd.columns:
                    df_rd['CHURN_RISK'] = df_rd['CHURN_RISK'].replace({
                        'HIGH RISK ⚠️':   'Action Required ⚠️',
                        'MEDIUM RISK 🟡': 'Monitor Closely 🟡',
                        'STABLE ✅':       'On Track ✅',
                    })
                if 'IF_IS_ANOMALY' in df_rd.columns:
                    df_rd['IF_IS_ANOMALY'] = df_rd['IF_IS_ANOMALY'].map({True: '⚡ Anomaly Detected', False: '✅ Normal'})

                def style_risk_table(row):
                    styles = [''] * len(row)
                    for idx, col in enumerate(df_rd.columns):
                        if col.startswith('ZSCORE') and pd.to_numeric(row[col], errors='coerce') < z_thresh_val:
                            styles[idx] = f'color: {RED}; font-weight: bold;'
                    return styles

                fmt = {c: "{:.3f}" for c in ['ZSCORE_SV','ZSCORE_FBI','ZSCORE_GROWTH'] if c in df_rd.columns}
                if 'RISK_SCORE' in df_rd.columns: fmt['RISK_SCORE'] = "{:.1f}"
                if 'IF_ANOMALY_SCORE' in df_rd.columns: fmt['IF_ANOMALY_SCORE'] = "{:.4f}"
                st.dataframe(df_rd.style.apply(style_risk_table, axis=1).format(fmt).hide(axis="index"), use_container_width=True)
                st.download_button("⬇️ Export Merchant Action List", df_rd.to_csv(index=False, encoding='utf-8-sig'),
                                   "merchant_action_list.csv", "text/csv")

        # ── Weekly Activity Pulse — Sudden Drop Monitor ───────────────────────
        styled_divider()
        section_label("🟠 Weekly Activity Pulse — Sudden Drop Monitor")
        st.caption("Scans the most recent week of transaction data and flags any merchant whose volume suddenly crashed below their own 4-week rolling average. Use this to catch new problems the moment they appear — before they become structural health issues.")
        if not has_mon_weekly:
            st.info("⚠️ Weekly drop monitoring requires Monitoring Weekly data to be processed first.")
        else:
            _sc_wk = df_mon_weekly[df_mon_weekly['YEAR'] == '2026'].copy() if not df_mon_weekly.empty else pd.DataFrame()
            _SC_W_COLS = sorted([c for c in _sc_wk.columns if c.startswith('W') and c[1:].isdigit()])
            if _sc_wk.empty or not _SC_W_COLS:
                st.info("ℹ️ No 2026 weekly data available yet.")
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
                        st.warning(f"⚠️ **{len(_anomalies)} merchant(s) dropped by {_slider_drop}%+ in {_wk_curr}** compared to their 4-week average.")
                        _anom_disp = _anomalies[['MERCHANT_GROUP', 'DIMENSI', '4-Week Avg', _wk_curr, 'This Week Change']].copy()
                        _anom_disp['This Week Change'] = (_anom_disp['This Week Change']*100).round(1).astype(str) + "%"
                        st.dataframe(_anom_disp.style.map(lambda x: f"color: {RED}; font-weight: bold", subset=['This Week Change']),
                                     use_container_width=True, hide_index=True)
                    else:
                        st.success(f"✅ No merchants dropped by {_slider_drop}%+ this week ({_wk_curr}). Portfolio activity looks stable.")


