import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import os
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

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="BTN Anchor Dashboard", page_icon="📈", layout="wide")
apply_theme()

def _p():
    """Get current palette dict for theme-aware chart colours."""
    return get_palette()

def _chart_base():
    """Return common Plotly layout kwargs for the active palette."""
    p = _p()
    return dict(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=p['TEXT_PRI'], family='Inter, sans-serif'),
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

def table_exists(conn, name):
    return pd.read_sql_query(
        f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}'", conn
    ).iloc[0, 0] == 1

# ── EXCEL PARSERS ────────────────────────────────────────────────────────────
@st.cache_data
def parse_highlight(path):
    """Parse the Highlight sheet: merchant group monthly TRX/SV/FBI breakdown."""
    try:
        raw = pd.read_excel(path, sheet_name='Highlight', header=None)
    except Exception:
        return pd.DataFrame()
    # Row 16 = section headers (TRANSACTION / SALES VOLUME / FEE BASED INCOME)
    # Row 17 = col headers; col 0 = merchant group name or month code
    # Find header row (contains 'TRANSACTION')
    hdr_row = None
    for i, row in raw.iterrows():
        if 'TRANSACTION' in str(row.values):
            hdr_row = i
            break
    if hdr_row is None:
        return pd.DataFrame()
    col_row   = hdr_row + 1
    data_start = col_row + 1
    cols = raw.iloc[col_row].tolist()
    # Build column name list by forward-filling section headers
    section_row = raw.iloc[hdr_row].tolist()
    section = ''
    named_cols = []
    for i, (sec, col) in enumerate(zip(section_row, cols)):
        if pd.notna(sec) and str(sec).strip() not in ('', 'nan'):
            section = str(sec).strip()
        named_cols.append(f"{section}__{str(col).strip()}" if pd.notna(col) and str(col).strip() not in ('', 'nan') else f'__col{i}')
    df = raw.iloc[data_start:].copy()
    df.columns = named_cols
    # Extract merchant group from the first non-NaN entry in col 0 area
    merch_col = named_cols[0]
    label_col = named_cols[1]
    df = df.rename(columns={merch_col: 'MONTH_CODE', label_col: 'LABEL'})
    df['MONTH_CODE'] = df['MONTH_CODE'].ffill()
    
    # We must explicitly drop rows where LABEL is NaN AND there's no transaction data
    # The trailing rows at the bottom of the Excel sheet usually have NaN across all value cols
    val_cols = [c for c in df.columns if c not in ['MONTH_CODE', 'LABEL']]
    df = df.dropna(subset=val_cols, how='all')
    
    # We must also drop trailing Summary blocks that just list Years (e.g. 2024, 2025, 2026) 
    # instead of datetime dates or YTD/Average, otherwise they inherit the last forward-filled MONTH_CODE
    df = df.dropna(subset=['LABEL'])
    df = df[~df['LABEL'].astype(str).str.match(r'^\s*20\d{2}\s*$', na=False)]
    
    # Now we can safely keep only rows that are month codes (6-digit ints like 202401) or YTD/Average
    df = df[df['MONTH_CODE'].astype(str).str.match(r'\d{6}|YTD|Average', na=False)].copy()
    df['MONTH_CODE'] = df['MONTH_CODE'].astype(str)
    df['YEAR'] = df['MONTH_CODE'].str[:4]
    return df


@st.cache_data
def parse_realisasi(path):
    try:
        df = pd.read_excel(path, sheet_name='Realisasi')
        trx_cols = [c for c in df.columns if c.startswith('TRX_') and c != 'TRX_MONTH']
        sv_cols  = [c for c in df.columns if c.startswith('SV_')]
        fbi_cols = [c for c in df.columns if c.startswith('FBI_')]
        df['TRX'] = df[trx_cols].sum(axis=1)
        df['SV']  = df[sv_cols].sum(axis=1)
        df['FBI'] = df[fbi_cols].sum(axis=1)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data
def parse_monitoring_sheet(path, sheet, _mtime=None):
    """Parse PerPM or PerMerchant sheet into a clean long DataFrame."""
    try:
        raw = pd.read_excel(path, sheet_name=sheet, header=None)
    except Exception:
        return pd.DataFrame()
    hdr_idx = None
    for i, row in raw.iterrows():
        vals = [str(v) for v in row if pd.notna(v)]
        if 'KET' in vals and 'Periode' in vals:
            hdr_idx = i
    if hdr_idx is None:
        return pd.DataFrame()
    headers = raw.iloc[hdr_idx].tolist()
    def ci(name):
        for idx, h in enumerate(headers):
            if str(h).strip() == name: return idx
        return None
    c_name    = next((i for i,h in enumerate(headers) if str(h).strip() in ('SEGMEN','PM') and i > 0), 1)
    c_ket     = ci('KET')
    c_periode = ci('Periode')
    c_fy      = ci('FY')
    c_ytd     = ci('YTD')
    week_start = next((i for i,h in enumerate(headers) if 'Week-01' in str(h)), None)
    if week_start is None:
        return pd.DataFrame()
    week_labels = []
    week_indices = []
    for i, h in enumerate(headers[week_start:], start=week_start):
        h_str = str(h).strip()
        if h_str.startswith('Week-'):
            num_part = h_str.split('-')[-1]
            if num_part.isdigit():
                week_labels.append(f"W{int(num_part):02d}")
                week_indices.append(i)
    data_rows = raw.iloc[hdr_idx+2:].reset_index(drop=True)
    records = []
    for _, row in data_rows.iterrows():
        name_val    = str(row.iloc[c_name]).strip()   if c_name is not None and pd.notna(row.iloc[c_name]) else None
        ket_val     = str(row.iloc[c_ket]).strip()    if c_ket  is not None and pd.notna(row.iloc[c_ket])  else ''
        periode_val = str(row.iloc[c_periode]).strip() if c_periode is not None and pd.notna(row.iloc[c_periode]) else ''
        fy_val      = row.iloc[c_fy]  if c_fy  is not None else None
        ytd_val     = row.iloc[c_ytd] if c_ytd is not None else None
        merch_code  = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
        row_num     = pd.to_numeric(row.iloc[0], errors='coerce')
        rec = {'NAME': name_val, 'KET': ket_val, 'PERIODE': periode_val,
               'FY': fy_val, 'YTD': ytd_val,
               'MERCHANT_CODE': merch_code, 'ROW_NUM': row_num}
        for lbl, idx in zip(week_labels, week_indices):
            val = row.iloc[idx]
            rec[lbl] = pd.to_numeric(val, errors='coerce') if pd.notna(val) else 0
        records.append(rec)
    df_out = pd.DataFrame(records)
    df_out['NAME'] = df_out['NAME'].replace('', np.nan).ffill()
    for w in week_labels:
        df_out[w] = pd.to_numeric(df_out[w], errors='coerce').fillna(0)
    return df_out


@st.cache_data
def parse_2026_sheet(path, _mtime=None):
    """Parse the '2026' sheet — flat table with MERCHANT_GROUP, DIMENSI, PM, weekly data.
    Returns a DataFrame with columns: NAME, KET, PERIODE, PM, FY, YTD, W01..W53."""
    try:
        df = pd.read_excel(path, sheet_name='2026')
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    # Rename to match the existing UI column conventions
    rename_map = {'MERCHANT_GROUP': 'NAME', 'DIMENSI': 'KET', 'VALUE': 'PERIODE'}
    df = df.rename(columns=rename_map)
    # Rename numeric week columns (1,2,3...) to W01, W02, W03...
    week_rename = {}
    for c in df.columns:
        try:
            n = int(c)
            if 1 <= n <= 53:
                week_rename[c] = f'W{n:02d}'
        except (ValueError, TypeError):
            pass
    df = df.rename(columns=week_rename)
    # Clean up PM column
    if 'PM' in df.columns:
        df['PM'] = df['PM'].fillna('–').astype(str).str.strip().str.title()
    # Ensure week columns are numeric
    w_cols = sorted([c for c in df.columns if c.startswith('W') and c[1:].isdigit()])
    for w in w_cols:
        df[w] = pd.to_numeric(df[w], errors='coerce').fillna(0)
    # Ensure FY, YTD are numeric
    for col in ['FY', 'YTD']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # Drop helper columns not needed for display
    df = df.drop(columns=['KEY1', 'KEY2'], errors='ignore')
    return df

# ── DB LOAD ───────────────────────────────────────────────────────────────────
if not os.path.exists(PATH_DB):
    st.warning("⚠️ Database not found. Process files in the Processing pages first.")
    st.stop()

conn = sqlite3.connect(PATH_DB)
has_card = table_exists(conn, "raw_card_share")
has_mon  = table_exists(conn, "raw_monitoring")
has_tgt  = table_exists(conn, "raw_target")
df_card   = pd.read_sql_query("SELECT * FROM raw_card_share", conn) if has_card else pd.DataFrame()
df_mon    = pd.read_sql_query("SELECT * FROM raw_monitoring", conn) if has_mon  else pd.DataFrame()
df_target = pd.read_sql_query("SELECT * FROM raw_target", conn) if has_tgt else pd.DataFrame(columns=['MERCHANT_GROUP','TARGET_VOL_2026'])
conn.close()

# ── ML PIPELINE ──────────────────────────────────────────────────────────────
@st.cache_data
def run_ml(card, mon, tgt, k_clusters=3, z_thresh=-1.2):
    df = pd.merge(card, mon, on='MERCHANT_GROUP', how='inner')
    df = pd.merge(df, tgt, on='MERCHANT_GROUP', how='left')
    if 'PM_x' in df.columns:
        df['PM'] = df['PM_x'].fillna(df.get('PM_y', '')).fillna('Unassigned')
    elif 'PM' not in df.columns:
        df['PM'] = 'Unassigned'
    df['AVG_SV']  = df['TOTAL_SV']  / df['N_BULAN'].clip(lower=1)
    df['AVG_FBI'] = df['TOTAL_FBI'] / df['N_BULAN'].clip(lower=1)
    df['AVG_TRX'] = df['TOTAL_TRX'] / df['N_BULAN'].clip(lower=1)
    df['RASIO_ONUS'] = df['RASIO_ONUS'].clip(0, 1)
    df['SV_GROWTH_RATE'] = pd.to_numeric(df.get('SV_GROWTH_RATE', pd.Series([0]*len(df))), errors='coerce').fillna(0)
    low, high = df['SV_GROWTH_RATE'].quantile([0.05, 0.95])
    df['SV_GROWTH_CLIPPED'] = df['SV_GROWTH_RATE'].clip(low, high)
    if 'TARGET_VOL_2026' in df.columns and 'YTD_VOL' in df.columns:
        df['ACHIEVEMENT_PCT'] = np.where(
            df['TARGET_VOL_2026'].fillna(0) > 0,
            (df['YTD_VOL'] / df['TARGET_VOL_2026'] * 100).clip(0, 200), 0
        )
    else:
        df['ACHIEVEMENT_PCT'] = 0
    df['WEEKS_ACTIVE'] = df.get('WEEKS_ACTIVE', pd.Series([0]*len(df))).fillna(0)
    FEAT = ['AVG_SV', 'AVG_FBI', 'RASIO_ONUS', 'SV_GROWTH_CLIPPED', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE']
    X = df[FEAT].fillna(0).copy()
    X['AVG_SV']  = np.log1p(X['AVG_SV'])
    X['AVG_FBI'] = np.log1p(X['AVG_FBI'])
    X_s = StandardScaler().fit_transform(X)
    
    km = KMeans(n_clusters=k_clusters, init='k-means++', n_init=20, random_state=42)
    df['CLUSTER_RAW'] = km.fit_predict(X_s)
    sv_order = df.groupby('CLUSTER_RAW')['AVG_SV'].mean().sort_values(ascending=False)
    rank = {c: i for i, c in enumerate(sv_order.index)}
    
    # Dynamic Cluster Naming based on K
    if k_clusters == 3:
        lbl = {0: 'PREMIUM', 1: 'REGULER', 2: 'PASIF'}
    elif k_clusters == 4:
        lbl = {0: 'ELITE', 1: 'PREMIUM', 2: 'REGULER', 3: 'PASIF'}
    elif k_clusters == 5:
        lbl = {0: 'ELITE', 1: 'PREMIUM', 2: 'REGULER', 3: 'PASIF', 4: 'DORMANT'}
    else:
        lbl = {i: f'TIER {i+1}' for i in range(k_clusters)}
        
    df['CLUSTER'] = df['CLUSTER_RAW'].map(lambda c: lbl[rank[c]])
    
    # Tri-Factor Z-Score Calculations
    df['ZSCORE_SV'] = stats.zscore(np.log1p(df['AVG_SV']))
    df['ZSCORE_FBI'] = stats.zscore(np.log1p(df['AVG_FBI']))
    df['ZSCORE_GROWTH'] = stats.zscore(df['SV_GROWTH_CLIPPED'])
    
    # Outlier Detection Engine
    df['CHURN_RISK'] = (
        (df['WEEKS_ACTIVE'] <= 2) |
        ((df['SV_GROWTH_RATE'] <= -0.99) & (df['ACHIEVEMENT_PCT'] < 5)) |
        ((df['CLUSTER'].isin(['PASIF', 'DORMANT'])) & (df['ACHIEVEMENT_PCT'] < 1)) |
        (df['ZSCORE_SV'] < z_thresh) |
        (df['ZSCORE_FBI'] < z_thresh) |
        (df['ZSCORE_GROWTH'] < z_thresh)
    ).map({True: 'HIGH RISK ⚠️', False: 'STABLE ✅'})
    
    return df

# ── HEADER + STATUS STRIP ────────────────────────────────────────────────────
page_header("🏦", "BTN Anchor Merchant", "Decision Intelligence Platform")

# ── Stale Data Banner ─────────────────────────────────────────────────────────
# Show amber notice if staging.db is older than 24 hours
stale_data_banner(db_path=PATH_DB, threshold_hours=24)

# ── Global KPI Summary Row ────────────────────────────────────────────────────
# Sourced from live DB data; shows zeros gracefully if tables are empty
_total_merchants = df_card['MERCHANT_GROUP'].nunique()          if not df_card.empty and 'MERCHANT_GROUP' in df_card.columns else 0
_ytd_sv          = df_card['TOTAL_SV'].sum()                    if not df_card.empty and 'TOTAL_SV'        in df_card.columns else 0
_ytd_trx         = df_card['TOTAL_TRX'].sum()                   if not df_card.empty and 'TOTAL_TRX'       in df_card.columns else 0
_avg_onus        = df_card['RASIO_ONUS'].mean()                 if not df_card.empty and 'RASIO_ONUS'      in df_card.columns else 0

# High-risk merchant count: attempt lightweight ML estimate from card data only
_high_risk_count = 0
if not df_card.empty and 'SV_GROWTH_RATE' in df_card.columns and 'WEEKS_ACTIVE' in df_card.columns:
    _high_risk_count = int((
        (df_card['WEEKS_ACTIVE'].fillna(0) <= 2) |
        (df_card['SV_GROWTH_RATE'].fillna(0) <= -0.99)
    ).sum())

_sv_fmt  = f"Rp {_ytd_sv/1e9:,.1f} M"  if _ytd_sv >= 1e9 else f"Rp {_ytd_sv/1e6:,.0f} Jt"
_trx_fmt = f"{_ytd_trx/1e6:,.2f} M"    if _ytd_trx >= 1e6 else f"{_ytd_trx:,.0f}"

kpi_row([
    kpi_card(f"{_total_merchants:,}",      "🏪 Merchants Tracked"),
    kpi_card(_sv_fmt,                       "💰 YTD Sales Volume",  "accent"),
    kpi_card(_trx_fmt,                      "🔄 YTD Transactions"),
    kpi_card(f"{_avg_onus*100:.1f}%",       "🎯 Avg On-Us Ratio",   "success" if _avg_onus >= 0.5 else "default"),
    kpi_card(f"{_high_risk_count}",         "⚠️ High Risk Merchants", "danger" if _high_risk_count > 0 else "success"),
])

# ── Neat status strip ──
_sp = get_palette()

def _sc(icon, label, ok, ok_text="Ready", fail_text="Missing", warn=False):
    kind  = "ok" if ok else ("warn" if warn else "err")
    value = ok_text if ok else (fail_text)
    color = {"ok": _sp['GREEN'], "warn": _sp['AMBER'], "err": _sp['RED']}[kind]
    bg    = _sp['SURFACE']
    bdr   = _sp['BORDER']
    txt   = _sp['TEXT_PRI']
    txt2  = _sp['TEXT_SEC']
    return f"""
    <div style="background:{bg};border:1px solid {bdr};border-left:4px solid {color};
                border-radius:10px;padding:10px 14px;display:flex;align-items:center;
                gap:10px;height:100%;">
        <span style="font-size:1.4rem;">{icon}</span>
        <div>
            <div style="font-size:0.68rem;text-transform:uppercase;letter-spacing:.06em;color:{txt2};">{label}</div>
            <div style="font-size:0.88rem;font-weight:700;color:{color};margin-top:2px;">{value}</div>
        </div>
    </div>"""

sc1, sc2, sc3, sc4, sc5 = st.columns(5)
sc1.markdown(_sc("📊", "Card Share DB",   has_card,                          "Loaded",       "Not processed"), unsafe_allow_html=True)
sc2.markdown(_sc("📅", "Monitoring DB",   has_mon,                           "Loaded",       "Not processed"), unsafe_allow_html=True)
sc3.markdown(_sc("🎯", "Target Data",     has_tgt,                           "Loaded",       "Not uploaded",  warn=not has_tgt), unsafe_allow_html=True)
sc4.markdown(_sc("📄", "Card Share File", os.path.exists(PATH_CARD),         "Configured",   "Upload in Settings"), unsafe_allow_html=True)
sc5.markdown(_sc("📄", "Monitoring File", os.path.exists(PATH_MON),          "Configured",   "Upload in Settings"), unsafe_allow_html=True)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
styled_divider()


CLAMP = CLUSTER_COLORS

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "💰  Card Share",
    "📅  Weekly Monitoring",
    "🤖  ML Segmentation",
    "⚠️  Churn & Risk",
    "🔍  Merchant Explorer",
    "🔮  AI Insights",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARD SHARE
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    tab_desc("Monthly payment type breakdown — TRANSACTION / SALES VOLUME / FEE BASED INCOME. Use <b>Year Filter</b> to focus on one year.")

    # KPIs from DB
    if not df_card.empty:
        avg_onus = df_card['RASIO_ONUS'].mean() if 'RASIO_ONUS' in df_card.columns else 0
        kpi_row([
            kpi_card(f"Rp {df_card['TOTAL_SV'].sum()/1e9:,.1f}M",          "💰 YTD Sales Volume"),
            kpi_card(f"Rp {df_card['TOTAL_FBI'].sum()/1e6:,.0f}Jt",         "📈 YTD Fee-Based Income"),
            kpi_card(f"{df_card['TOTAL_TRX'].sum()/1e6:,.2f}M",             "🔄 YTD Transactions"),
            kpi_card(f"{avg_onus*100:.1f}%",                                  "🎯 Avg On-Us Ratio"),
        ])

    has_hl_file = os.path.exists(PATH_CARD)
    if not has_hl_file:
        st.warning("⚠️ Master Card Share file not configured. Upload it in ⚙️ Master Configuration.")
    else:
        df_hl = parse_highlight(PATH_CARD)
        if df_hl.empty:
            st.warning("⚠️ Could not parse the Highlight sheet.")
        else:
            MONTH_ABB = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

            def month_label(row):
                lbl = row.get('LABEL', '')
                
                # 1. If it's YTD 202X or Average
                if pd.notna(lbl) and any(x in str(lbl) for x in ['YTD', 'Average']):
                    return str(lbl).strip()
                    
                # 2. Try to use the DATE column (row['LABEL']) if it's parsed as datetime
                if pd.notna(lbl) and hasattr(lbl, 'strftime'):
                    return lbl.strftime('%b-%y')
                
                # 3. Fallback to MONTH_CODE parsing
                code = str(row['MONTH_CODE']).replace('.0','')
                if len(code) == 6 and code.isdigit():
                    yr, mo = code[:4], int(code[4:])
                    if 1 <= mo <= 12:
                        return f"{MONTH_ABB[mo-1]}-{yr[2:]}"
                    
                return str(code)

            def fmt_num(v, sec):
                try:
                    v = float(v)
                except:
                    return str(v)
                if 'SALES' in sec or 'FEE' in sec:
                    if abs(v) >= 1e9: return f"Rp {v/1e9:,.2f}M"
                    if abs(v) >= 1e6: return f"Rp {v/1e6:,.1f}Jt"
                    return f"Rp {v:,.0f}"
                return f"{v:,.0f}"

            # Detect rows that are strictly Data (has month code or YTD/Avg label)
            data_rows = df_hl[df_hl['MONTH_CODE'].str.match(r'\d{6}|YTD|Average', na=False)].copy()
            avail_years = sorted(data_rows['YEAR'].unique(), reverse=True)

            col_yr, col_vm = st.columns([2,3])
            with col_yr:
                sel_yr = st.selectbox("📅 Year", ['All'] + avail_years, key="t1_year")
            with col_vm:
                chart_type = st.radio("📊 Chart Style", ["Stacked Bar", "Line Trend", "Both"], horizontal=True, key="t1_chart")

            if sel_yr != 'All':
                data_rows = data_rows[data_rows['YEAR'] == sel_yr]

            data_rows = data_rows.copy()
            data_rows['Bulan'] = data_rows.apply(month_label, axis=1)

            TYPE_COLORS = PAYMENT_COLORS

            SECTIONS = {
                'TRANSACTION':     ('🔄', BLUE_ACC),
                'SALES VOLUME':    ('💰', GREEN),
                'FEE BASED INCOME':('📈', AMBER),
            }

            for sec, (icon, accent) in SECTIONS.items():
                sec_cols = [c for c in df_hl.columns if c.startswith(f'{sec}__') and '__col' not in c]
                if not sec_cols: continue

                section_header(icon, sec, accent_color=accent)

                display = data_rows[['Bulan'] + sec_cols].copy()
                raw_col_names = [c.split('__', 1)[1] for c in sec_cols]
                display.columns = ['Bulan'] + raw_col_names

                # Convert to numeric
                for col in raw_col_names:
                    display[col] = pd.to_numeric(display[col], errors='coerce').fillna(0)

                # Identify the TOTAL col and the 5 type cols
                total_col = next((c for c in raw_col_names if 'TOTAL' in c.upper()), None)
                type_cols = [c for c in raw_col_names if c != total_col]

                # YTD row
                ytd_nums   = display[raw_col_names].sum()
                ytd_row    = pd.DataFrame([['YTD'] + ytd_nums.tolist()], columns=['Bulan'] + raw_col_names)
                disp_full  = pd.concat([display, ytd_row], ignore_index=True)

                # Formatted display table
                disp_fmt = disp_full.copy()
                for col in raw_col_names:
                    disp_fmt[col] = disp_fmt[col].apply(lambda v: fmt_num(v, sec))

                def style_table(row):
                    is_ytd = row.name == len(disp_fmt) - 1
                    styles = []
                    for col in disp_fmt.columns:
                        if is_ytd:
                            styles.append(f'background-color:{accent};color:white;font-weight:bold;')
                        elif total_col and col == total_col:
                            styles.append(f'font-weight:600;')
                        else:
                            styles.append('')
                    return styles

                st.dataframe(
                    disp_fmt.style.apply(style_table, axis=1),
                    width='stretch', hide_index=True, height=min(38 * len(disp_fmt) + 40, 520)
                )

                # ── Charts — always visible, side-by-side layout ───────────
                chart_data = display.copy()   # excludes the YTD summary row
                _pp = _p()

                ch_left, ch_right = st.columns([3, 2])

                with ch_left:
                    if chart_type in ("Stacked Bar", "Both") and type_cols:
                        melted = chart_data.melt(
                            id_vars="Bulan", value_vars=type_cols,
                            var_name="Type", value_name="Value",
                        )
                        color_map = {t: TYPE_COLORS.get(t, "#999") for t in type_cols}
                        fig_s = px.bar(
                            melted, x="Bulan", y="Value", color="Type",
                            color_discrete_map=color_map,
                            barmode="stack",
                            title=f"{sec} — Payment Type Composition",
                        )
                        fig_s.update_traces(
                            marker_line_width=0,
                            hovertemplate=(
                                "<b>%{x}</b><br>"
                                "%{fullData.name}: <b>%{y:,.0f}</b>"
                                "<extra></extra>"
                            ),
                        )
                        fig_s.update_layout(
                            height=340,
                            margin=dict(l=0, r=0, t=36, b=0),
                            legend=dict(
                                orientation="h", y=-0.28, x=0,
                                font=dict(size=10, color=_pp["TEXT_PRI"]),
                                bgcolor="rgba(0,0,0,0)",
                            ),
                            **_chart_base(),
                            xaxis={**_xaxis(), "showgrid": False},
                            yaxis={**_yaxis(), "title": ""},
                        )
                        st.plotly_chart(fig_s, width="stretch")

                    if chart_type in ("Line Trend", "Both") and total_col:
                        cht = chart_data[["Bulan", total_col]].copy()
                        cht["MoM"] = cht[total_col].pct_change() * 100
                        text_labels = []
                        for _, _row in cht.iterrows():
                            _val = fmt_num(_row[total_col], sec)
                            _mom = _row["MoM"]
                            if pd.isna(_mom):
                                text_labels.append(_val)
                            else:
                                _sign = "+" if _mom > 0 else ""
                                text_labels.append(f"{_val} ({_sign}{_mom:.1f}%)")
                        fig_l = go.Figure()
                        fig_l.add_trace(go.Scatter(
                            x=cht["Bulan"], y=cht[total_col],
                            mode="lines+markers+text",
                            name=total_col,
                            line=dict(color=accent, width=2.5),
                            marker=dict(size=7, color=accent,
                                        line=dict(color=_pp["BG"], width=1.5)),
                            text=text_labels,
                            textposition="top center",
                            textfont=dict(size=9, color=_pp["TEXT_SEC"]),
                            hovertemplate=(
                                "<b>%{x}</b><br>"
                                "Total: <b>%{y:,.0f}</b>"
                                "<extra></extra>"
                            ),
                        ))
                        fig_l.update_layout(
                            title=f"{sec} — {total_col} Trend",
                            height=340,
                            showlegend=False,
                            margin=dict(l=0, r=0, t=36, b=0),
                            **_chart_base(),
                            xaxis={**_xaxis(), "showgrid": False},
                            yaxis={**_yaxis(), "title": "", "zeroline": False},
                        )
                        st.plotly_chart(fig_l, width="stretch")

                with ch_right:
                    if type_cols:
                        ytd_type = {t: float(ytd_nums.get(t, 0)) for t in type_cols}
                        if sum(ytd_type.values()) > 0:
                            section_label("🍩 YTD Mix")
                            fig_pie = go.Figure(go.Pie(
                                labels=list(ytd_type.keys()),
                                values=list(ytd_type.values()),
                                hole=0.60,
                                marker_colors=[TYPE_COLORS.get(t, "#999") for t in ytd_type],
                                textinfo="percent",
                                textfont_size=11,
                                hovertemplate=(
                                    "<b>%{label}</b><br>"
                                    "%{value:,.0f}<br>%{percent}"
                                    "<extra></extra>"
                                ),
                            ))
                            fig_pie.update_layout(
                                height=340,
                                showlegend=True,
                                margin=dict(t=10, b=50, l=10, r=10),
                                **_chart_base(),
                                legend=dict(
                                    orientation="h", y=-0.15, x=0.5,
                                    xanchor="center",
                                    font=dict(size=9, color=_pp["TEXT_PRI"]),
                                    bgcolor="rgba(0,0,0,0)",
                                ),
                            )
                            st.plotly_chart(fig_pie, width="stretch")

                styled_divider()

        # Top Merchants overview from DB
        if not df_card.empty:
            section_label("🏆 Top Merchants Analytics (YTD)")
            
            # Create a rich dataframe with calculated metrics
            df_c = df_card.copy()
            df_c['AVG_TRX_VAL'] = np.where(df_c['TOTAL_TRX'] > 0, df_c['TOTAL_SV'] / df_c['TOTAL_TRX'], 0)
            df_c['FBI_YIELD'] = np.where(df_c['TOTAL_SV'] > 0, (df_c['TOTAL_FBI'] / df_c['TOTAL_SV']) * 100, 0)
            
            cc1s, cc2s = st.columns([3, 1])
            top_n_c = cc1s.slider("Top N Merchants", 10, 50, 20, key="t1_topn")
            sort_by = cc2s.selectbox("Rank By", ['TOTAL_SV','TOTAL_TRX','TOTAL_FBI','RASIO_ONUS', 'FBI_YIELD'], key="t1_sort")
            
            df_top = df_c.sort_values(sort_by, ascending=False).head(top_n_c)
            
            # Format display dataframe
            disp_top = df_top[['MERCHANT_GROUP', 'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'AVG_TRX_VAL', 'FBI_YIELD', 'RASIO_ONUS']].copy()
            
            # Add formatted strings (keys match col_names / _disp_top)
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
                _disp_top.style.format(format_dict)
                .background_gradient(cmap='Blues', subset=['Sales Volume', 'Transactions'])
                .background_gradient(cmap='Greens', subset=['Fee Based Income', 'FBI Yield']),
                width='stretch', height=min(38 * len(disp_top) + 40, 500)
            )

            with st.expander("📋 Raw Card Share Data"):
                st.dataframe(df_c.reset_index(drop=True), width='stretch')
                st.download_button("⬇️ Download CSV", df_c.to_csv(index=False, encoding='utf-8-sig'), "card_share_data.csv", "text/csv")


            # ── GROWTH ANALYTICS (Realisasi) ──────────────────────────────────
            st.markdown("<br>", unsafe_allow_html=True)
            df_real = parse_realisasi(PATH_CARD)
            
            if not df_real.empty:
                max_month = df_real['TRX_MONTH'].max()
                try:
                    curr_yr = int(str(max_month)[:4])
                    curr_mo = int(str(max_month)[4:])
                    prev_yr = curr_yr - 1
                    prev_month = int(f"{prev_yr}{curr_mo:02d}")
                    
                    MONTH_ABB = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                    col_curr = f"{MONTH_ABB[curr_mo-1]}-{str(curr_yr)[2:]}"
                    col_prev = f"{MONTH_ABB[curr_mo-1]}-{str(prev_yr)[2:]}"
                    col_fy_prev = f"FY-{str(prev_yr)[2:]}"
                    
                    # Target metric selection — inline with section header
                    gh1, gh2 = st.columns([3, 1])
                    with gh1:
                        section_label("📈 Top & Bottom Merchant Growth (MoM YoY)")
                    with gh2:
                        metric_sel = st.selectbox(
                            "Metric",
                            ["SALES VOLUME", "TRANSACTION", "FEE BASED INCOME"],
                            key="t1_metric_growth",
                            label_visibility="collapsed"
                        )
                    m_col = 'SV' if 'SALES' in metric_sel else ('TRX' if 'TRANS' in metric_sel else 'FBI')
                    
                    # Group data
                    # Current month
                    df_curr = df_real[df_real['TRX_MONTH'] == max_month].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_curr)
                    # Previous month
                    df_prev = df_real[df_real['TRX_MONTH'] == prev_month].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_prev)
                    # FY Previous
                    df_fy = df_real[df_real['YEAR'] == prev_yr].groupby('MERCHANT_GROUP')[m_col].sum().reset_index(name=col_fy_prev)
                    
                    # Merge all
                    df_growth = pd.merge(df_curr, df_prev, on='MERCHANT_GROUP', how='outer')
                    df_growth = pd.merge(df_growth, df_fy, on='MERCHANT_GROUP', how='outer').fillna(0)
                    
                    # Calculate Growth and Delta
                    df_growth['Delta'] = df_growth[col_curr] - df_growth[col_prev]
                    df_growth['Growth %'] = np.where(df_growth[col_prev] > 0, 
                                                    (df_growth['Delta'] / df_growth[col_prev]) * 100, 
                                                    np.where(df_growth[col_curr] > 0, 100, 0))
                    
                    # Clean zeroes
                    df_growth = df_growth[(df_growth[col_curr] > 0) | (df_growth[col_prev] > 0) | (df_growth[col_fy_prev] > 0)]
                    
                    # Split Top and Bottom
                    top_10 = df_growth.sort_values('Growth %', ascending=False).head(10)
                    bot_10 = df_growth.sort_values('Growth %', ascending=True).head(10)
                    
                    # Formatter
                    def val_fmt(x):
                        if m_col == 'TRX': return f"{x:,.0f}"
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
                        col_curr: val_fmt, 
                        col_prev: val_fmt, 
                        col_fy_prev: val_fmt,
                        'Delta': val_fmt,
                        'Growth %': lambda x: f"{x:,.0f}%"
                    }
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        section_label(f"🟢 Top 10 by {metric_sel} Growth")
                        st.dataframe(top_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), width='stretch')
                    with c2:
                        section_label(f"🔴 Bottom 10 by {metric_sel} Growth")
                        st.dataframe(bot_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), width='stretch')
                        
                except Exception as e:
                    st.error(f"Could not calculate growth from Realisasi dates: {e}")
            else:
                st.info("Realisasi data for growth analytics not available in Master file.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WEEKLY MONITORING (reads from '2026' sheet directly)
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    tab_desc("Weekly monitoring — merchant-level weekly matrix read directly from the <b>2026</b> sheet. "
             "Filter by <b>PM</b> and <b>Metric</b> (TRX / VOL / FBI) to drill down.")

    # Determine which monitoring file to read from
    _mon_path = PATH_RAW_MON if os.path.exists(PATH_RAW_MON) else PATH_MON
    has_mon_file = os.path.exists(_mon_path)

    if not has_mon_file:
        st.warning("⚠️ Monitoring file not found. Place 'Monitoring Weekly Anchor 2026.xlsx' in data/raw/real/.")
    else:
        mon_mtime = os.path.getmtime(_mon_path)
        df_2026_raw = parse_2026_sheet(_mon_path, mon_mtime)

        if df_2026_raw.empty:
            st.warning("⚠️ Could not parse the '2026' sheet from the monitoring file.")
        else:
            W_COLS = sorted([c for c in df_2026_raw.columns if c.startswith('W') and c[1:].isdigit()])

            # ── Filters ───────────────────────────────────────────────────────
            f_col1, f_col2, f_col3 = st.columns([2, 2, 2])

            # PM filter
            with f_col1:
                _SKIP_PM = {'–', '', 'Nan', 'nan', 'None', 'Unknown'}
                pm_names = sorted(
                    p for p in df_2026_raw['PM'].dropna().unique()
                    if str(p).strip() not in _SKIP_PM
                ) if 'PM' in df_2026_raw.columns else []
                sel_pm = st.selectbox(
                    "👤 Filter by PM",
                    ["All PMs"] + pm_names,
                    index=0,
                    key="t2_pm_2026",
                )

            # Metric filter (DIMENSI = TRX / VOL / FBI)
            with f_col2:
                avail_ket = sorted(df_2026_raw['KET'].dropna().unique())
                sel_ket = st.multiselect(
                    "📊 Metric (Dimensi)",
                    avail_ket,
                    default=avail_ket,
                    key="t2_ket_2026",
                )

            # Apply filters
            df_filt = df_2026_raw.copy()
            if sel_pm != "All PMs" and 'PM' in df_filt.columns:
                df_filt = df_filt[df_filt['PM'] == sel_pm]
            if sel_ket:
                df_filt = df_filt[df_filt['KET'].isin(sel_ket)]

            active_weeks = W_COLS

            # ── KPI summary for filtered data ─────────────────────────────────
            _n_merchants = df_filt['NAME'].nunique()
            _n_pms = df_filt['PM'].nunique() if 'PM' in df_filt.columns else 0
            _total_ytd = df_filt['YTD'].sum() if 'YTD' in df_filt.columns else 0

            # Smart format
            if _total_ytd >= 1e12:
                _ytd_label = f"Rp {_total_ytd/1e12:,.2f}T"
            elif _total_ytd >= 1e9:
                _ytd_label = f"Rp {_total_ytd/1e9:,.1f}M"
            elif _total_ytd >= 1e6:
                _ytd_label = f"Rp {_total_ytd/1e6:,.0f}Jt"
            else:
                _ytd_label = f"{_total_ytd:,.0f}"

            kpi_row([
                kpi_card(f"{_n_merchants}", "🏪 Merchants"),
                kpi_card(f"{_n_pms}", "👤 PMs"),
                kpi_card(_ytd_label, "📊 Total YTD (filtered)"),
                kpi_card(f"{len(df_filt)}", "📋 Rows Displayed"),
            ])

            # ── MAIN TABLE ────────────────────────────────────────────────────
            section_label("🏪 Merchant Weekly Matrix")
            filter_pill(f"PM: {sel_pm} · Metrics: {', '.join(sel_ket) if sel_ket else 'All'} · {_n_merchants} merchants")

            disp_cols = ['NAME', 'KET', 'PM', 'PERIODE', 'FY', 'YTD'] + active_weeks
            available_disp = [c for c in disp_cols if c in df_filt.columns]
            st.dataframe(df_filt[available_disp].fillna(0).reset_index(drop=True),
                         width='stretch', height=430)

            # ── CHARTS SECTION ────────────────────────────────────────────────
            df_2026 = df_filt.copy()

            if not df_2026.empty:
                st.markdown("---")
                section_label("📊 Visual Analysis")

                all_names = sorted(df_2026['NAME'].unique())
                default_names = df_2026.sort_values('YTD', ascending=False)['NAME'].unique().tolist()[:10]

                c_filt1, c_filt2 = st.columns([3, 1])
                with c_filt1:
                    sel_chart_names = st.multiselect(
                        "🔍 Select Merchants to Chart",
                        all_names,
                        default=default_names,
                        key="t2_chart_names_2026"
                    )

                df_chart = df_2026[df_2026['NAME'].isin(sel_chart_names)].copy() if sel_chart_names else pd.DataFrame()

            # ── WEEKLY HEATMAP ────────────────────────────────────────────────
            data_weeks = [c for c in W_COLS if (df_chart[c].fillna(0) != 0).any()] if not df_chart.empty else []
            if not df_chart.empty and data_weeks:
                section_label("🗓️ Weekly Activity Heatmap (2026)")
                df_heat = df_chart.copy()
                df_heat['LABEL'] = df_heat['NAME'] + ' (' + df_heat['KET'].astype(str) + ')'
                df_heat[data_weeks] = df_heat[data_weeks].apply(pd.to_numeric, errors='coerce').fillna(0)
                heat_data = df_heat.set_index('LABEL')[data_weeks]

                _pp = _p()
                _hm_scale = [
                    [0.0, _pp['BG']],
                    [0.1, _pp['SURFACE']],
                    [0.3, _pp['NAVY']],
                    [0.6, _pp['BLUE_ACC']],
                    [1.0, _pp['GOLD']]
                ]
                fig_heat = px.imshow(
                    heat_data,
                    color_continuous_scale=_hm_scale,
                    aspect='auto',
                    title="Weekly Heatmap (2026)",
                    labels=dict(x="Week Number", y="", color="Value")
                )
                h_calc = max(280, 40 * len(heat_data) + 100)
                fig_heat.update_layout(
                    height=h_calc,
                    xaxis=dict(dtick=2, color=_pp['TEXT_SEC'], side='top'),
                    coloraxis_showscale=True,
                    margin=dict(l=10, r=10, t=80, b=10),
                    **_chart_base(),
                )
                fig_heat.update_traces(hovertemplate='<b>%{y}</b><br>%{x}: %{z:,.0f}<extra></extra>')
                st.plotly_chart(fig_heat, width='stretch')

            # ── WEEKLY TREND LINE ─────────────────────────────────────────────
            if not df_chart.empty and data_weeks:
                section_label("📈 Weekly Trend & WoW Growth — 2026")
                df_trend = df_chart.copy()
                df_trend['LABEL'] = df_trend['NAME'] + ' (' + df_trend['KET'].astype(str) + ')'
                df_trend[data_weeks] = df_trend[data_weeks].apply(pd.to_numeric, errors='coerce').fillna(0)

                df_long = df_trend[['LABEL'] + data_weeks].melt(id_vars='LABEL', var_name='Week', value_name='Value')
                df_long = df_long.sort_values(['LABEL', 'Week'])
                df_long['WoW'] = df_long.groupby('LABEL')['Value'].pct_change() * 100

                def _wk_lbl(row):
                    v = row['Value']
                    mom = row['WoW']
                    if v >= 1e9:
                        vlbl = f"{v/1e9:,.1f}M"
                    elif v >= 1e6:
                        vlbl = f"{v/1e6:,.0f}Jt"
                    elif v >= 1e3:
                        vlbl = f"{v/1e3:,.0f}K"
                    else:
                        vlbl = f"{v:,.0f}"
                    if pd.notna(mom) and mom > 0:
                        return f"{vlbl}<br>(+{mom:.1f}%)"
                    elif pd.notna(mom) and mom < 0:
                        return f"{vlbl}<br>({mom:.1f}%)"
                    return vlbl

                df_long['Text'] = df_long.apply(_wk_lbl, axis=1)

                fig_line = px.line(
                    df_long, x='Week', y='Value', color='LABEL', text='Text',
                    markers=True, title="Weekly Trend by Merchant"
                )
                fig_line.update_traces(marker=dict(size=6), line=dict(width=2.5), textposition='top center', textfont_size=9)
                fig_line.update_layout(
                    height=460,
                    legend=dict(orientation='h', y=-0.35, title=None, font=dict(color=_p()['TEXT_PRI'])),
                    **_chart_base(),
                    xaxis={**_xaxis(), 'dtick':2},
                    yaxis={**_yaxis(), 'title':''},
                )
                st.plotly_chart(fig_line, width='stretch')

            st.download_button("⬇️ Export Table",
                df_filt[available_disp].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                "monitoring_2026_export.csv", "text/csv")

    # KPI footer from DB
    if not df_mon.empty:
        styled_divider()
        avg_wa = df_mon['WEEKS_ACTIVE'].mean() if 'WEEKS_ACTIVE' in df_mon.columns else 0
        ytd_v  = df_mon['YTD_VOL'].sum() if 'YTD_VOL' in df_mon.columns else 0
        kpi_row([
            kpi_card(f"{len(df_mon):,}",        "🏪 Merchants in DB"),
            kpi_card(f"{avg_wa:.1f}",            "📆 Avg Weeks Active"),
            kpi_card(f"Rp {ytd_v/1e9:,.2f}M", "💰 YTD Volume Total"),
        ])


with tab3:
    tab_desc("K-Means++ Clustering segments merchants based on multivariate performance metrics. Use the slider below to dynamically discover hidden micro-segments.")

    if not (has_card and has_mon):
        st.warning("⚠️ ML analysis requires **both** Card Share and Monitoring data to be processed first.")
    else:
        k_val = st.slider("Select K (Number of Clusters)", min_value=3, max_value=5, value=3)
        with st.spinner(f"Running K-Means++ (K={k_val}) Machine Learning Pipeline..."):
            df_ml = run_ml(df_card, df_mon, df_target, k_clusters=k_val)

        all_pm_ml = sorted(df_ml['PM'].dropna().unique().tolist()) if 'PM' in df_ml.columns else []
        all_clusters = sorted(df_ml['CLUSTER'].dropna().unique().tolist())

        # Controls
        mc1, mc2 = st.columns(2)
        with mc1:
            sel_pm_ml = st.multiselect("👤 Filter by PM", all_pm_ml, default=all_pm_ml, key="t3_pm")
        with mc2:
            sel_clust = st.multiselect("🏷️ Show Clusters", all_clusters, default=all_clusters, key="t3_clust")

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
        
        # ── Upgraded segment metric grid ────────────────────────────────────
        SEGMENT_ICONS = {
            'ELITE': '👑', 'PREMIUM': '🌟', 'REGULER': '🔵',
            'PASIF': '🔴', 'DORMANT': '⚫',
        }
        total_merchants = len(df_f)
        _pp3 = _p()

        cols = st.columns(max(len(all_clusters), 1))
        for idx, (col, seg) in enumerate(zip(cols, all_clusters)):
            n = len(df_f[df_f['CLUSTER'] == seg])
            pct = (n / total_merchants * 100) if total_merchants > 0 else 0
            color = color_lookup.get(seg, fallback_colors[idx % len(fallback_colors)])
            icon = SEGMENT_ICONS.get(seg, '🔹')
            col.markdown(
                f"""<div style="background:linear-gradient(135deg,{color}22,{color}44);
                    border:1.5px solid {color};border-radius:14px;padding:20px 12px;
                    text-align:center;margin-bottom:8px;position:relative;overflow:hidden;">
                    <div style="font-size:1.6rem;margin-bottom:4px;">{icon}</div>
                    <div style="font-size:2rem;font-weight:800;color:{color};line-height:1;">{n}</div>
                    <div style="font-size:0.78rem;font-weight:700;color:{_pp3['TEXT_PRI']};
                                margin-top:6px;text-transform:uppercase;letter-spacing:.06em;">{seg}</div>
                    <div style="font-size:0.72rem;color:{_pp3['TEXT_SEC']};margin-top:2px;">{pct:.1f}% of fleet</div>
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
            st.plotly_chart(fig_pie, width='stretch')

        with sc2:
            fig_sc = px.scatter_3d(df_f, x='AVG_SV', y='AVG_FBI', z='SV_GROWTH_CLIPPED',
                                color='CLUSTER', hover_name='MERCHANT_GROUP',
                                hover_data=['PM','ACHIEVEMENT_PCT','WEEKS_ACTIVE'],
                                title="3D Mathematical Structure (SV x FBI x Growth)",
                                color_discrete_map=color_lookup)
            fig_sc.update_layout(height=450, margin=dict(l=0, r=0, b=0, t=30), **_chart_base())
            st.plotly_chart(fig_sc, width='stretch')

        section_label("Cluster Radar Profile")
        radar_m = ['AVG_SV','AVG_FBI','RASIO_ONUS','ACHIEVEMENT_PCT','WEEKS_ACTIVE']
        cm = df_f.groupby('CLUSTER')[radar_m].mean()
        norm = (cm - cm.min()) / (cm.max() - cm.min() + 1e-9)
        fig_r = go.Figure()
        for clust in all_clusters:
            if clust in norm.index:
                vals = norm.loc[clust].tolist() + [norm.loc[clust].tolist()[0]]
                cats = radar_m + [radar_m[0]]
                fig_r.add_trace(go.Scatterpolar(r=vals, theta=cats, fill='toself',
                    name=clust, line_color=color_lookup.get(clust, '#FFF')))
        _pp = _p()
        fig_r.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0,1],
                                                       gridcolor=_pp['BORDER'], tickfont=dict(color=_pp['TEXT_SEC'])),
                                       angularaxis=dict(color=_pp['TEXT_SEC']),
                                       bgcolor='rgba(0,0,0,0)'),
                             **_chart_base(),
                             height=430, title="Each cluster's normalised characteristic profile")
        st.plotly_chart(fig_r, width='stretch')

        if 'PM' in df_f.columns:
            section_label("PM × Cluster Breakdown")
            pm_cl = df_f.groupby(['PM','CLUSTER']).size().reset_index(name='COUNT')
            fig_stk = px.bar(pm_cl, x='PM', y='COUNT', color='CLUSTER',
                             barmode='stack', title="Cluster Distribution per Account Manager",
                             color_discrete_map=CLAMP)
            fig_stk.update_layout(height=380, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
            st.plotly_chart(fig_stk, width='stretch')

        with st.expander("📋 View ML Results Table"):
            show_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','AVG_SV','AVG_FBI',
                                     'ACHIEVEMENT_PCT','WEEKS_ACTIVE','ZSCORE_SV'] if c in df_f.columns]
            st.dataframe(df_f[show_cols].sort_values('AVG_SV', ascending=False).reset_index(drop=True), width='stretch')

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CHURN & RISK
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    tab_desc("Merchants flagged as <b>HIGH RISK ⚠️</b> meet at least one churn condition: low activity weeks, severe negative growth, PASIF cluster with near-zero achievement, or extreme anomaly Z-Scores across Volume, FBI, or Growth.")

    if not (has_card and has_mon):
        st.warning("⚠️ Churn analysis requires both Card Share and Monitoring data.")
    else:
        # Dynamic Z-Score Input Slider
        z_col, _ = st.columns([1, 2])
        z_thresh_val = z_col.slider("Z-Score Anomaly Tripwire (Standard Deviations)", 
                                    min_value=-3.0, max_value=-0.5, value=-1.2, step=0.1,
                                    help="Adjust how aggressively the AI flags merchants for statistical drops. -1.2 means they are worse than ~88% of the data. -2.0 means worse than ~98%.")
        
        df_churn_all = run_ml(df_card, df_mon, df_target, z_thresh=z_thresh_val)
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

        df_high = df_c4[df_c4['CHURN_RISK'].str.contains("HIGH", na=False)]
        df_safe = df_c4[~df_c4['CHURN_RISK'].str.contains("HIGH", na=False)]
        total   = len(df_c4)

        # KPI
        ch_a, ch_b, ch_c = st.columns(3)
        ch_a.markdown(kpi_card(str(len(df_high)), "⚠️ High Churn Risk", "danger"), unsafe_allow_html=True)
        ch_b.markdown(kpi_card(str(len(df_safe)), "✅ Stable", "success"), unsafe_allow_html=True)
        rate = len(df_high)/total*100 if total > 0 else 0
        ch_c.markdown(kpi_card(f"{rate:.1f}%", "Churn Rate (filtered)"), unsafe_allow_html=True)

        st.markdown("")

        if total > 0:
            # ── Gauge chart — churn rate speedometer ─────────────────────────
            _pp4 = _p()
            gauge_col, ch_right_kpi = st.columns([1, 1])
            with gauge_col:
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=rate,
                    number={"suffix": "%", "font": {"size": 36, "color": _pp4["TEXT_PRI"]}},
                    delta={"reference": 20, "relative": False,
                           "increasing": {"color": "#F87171"},
                           "decreasing": {"color": "#34D399"},
                           "suffix": "% vs 20% bench"},
                    gauge={
                        "axis": {
                            "range": [0, 100],
                            "tickwidth": 1,
                            "tickcolor": _pp4["TEXT_SEC"],
                            "tickfont": {"color": _pp4["TEXT_SEC"]},
                        },
                        "bar": {"color": (
                            "#34D399" if rate < 20 else
                            "#FBBF24" if rate < 45 else "#F87171"
                        ), "thickness": 0.28},
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
                    title={"text": "Portfolio Churn Rate", "font": {"size": 14, "color": _pp4["TEXT_SEC"]}},
                ))
                fig_gauge.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color=_pp4["TEXT_PRI"],
                    annotations=[
                        dict(x=0.18, y=0.08, text="<b>LOW</b>",   showarrow=False,
                             font=dict(color="#34D399", size=10)),
                        dict(x=0.50, y=0.08, text="<b>MEDIUM</b>", showarrow=False,
                             font=dict(color="#FBBF24", size=10)),
                        dict(x=0.82, y=0.08, text="<b>HIGH</b>",   showarrow=False,
                             font=dict(color="#F87171", size=10)),
                    ],
                )
                st.plotly_chart(fig_gauge, width="stretch")

            with ch_right_kpi:
                risk_label = "🟢 LOW RISK" if rate < 20 else ("🟡 MEDIUM RISK" if rate < 45 else "🔴 HIGH RISK")
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
                
                # Debug audit — verify chart input data
                with st.expander("🔬 Chart Data Audit", expanded=False):
                    st.caption("Raw aggregates feeding the gauge and donut charts:")
                    audit_data = {
                        "Metric": ["High Risk Count", "Stable Count", "Total", "Churn Rate %"],
                        "Value": [len(df_high), len(df_safe), total, f"{rate:.2f}%"],
                    }
                    st.dataframe(pd.DataFrame(audit_data), hide_index=True, width='stretch')
                    if 'CHURN_RISK' in df_c4.columns:
                        st.write("CHURN_RISK value_counts:")
                        st.dataframe(df_c4['CHURN_RISK'].value_counts().reset_index(), hide_index=True)

            # ── Donut + PM bar as before ──────────────────────────────────────
            ch_x, ch_y = st.columns(2)
            with ch_x:
                fig_rc = px.pie(df_c4, names='CHURN_RISK',
                                color='CHURN_RISK',
                                color_discrete_map={'HIGH RISK ⚠️':'#C0392B','STABLE ✅':'#27AE60'},
                                hole=0.4, title="Churn Risk Breakdown")
                fig_rc.update_layout(height=350, **_chart_base())
                st.plotly_chart(fig_rc, width='stretch')
            with ch_y:
                if 'PM' in df_high.columns and len(df_high) > 0:
                    pm_churn = df_high.groupby('PM').size().reset_index(name='HIGH_RISK_COUNT')
                    fig_pc = px.bar(pm_churn.sort_values('HIGH_RISK_COUNT', ascending=False),
                                    x='PM', y='HIGH_RISK_COUNT',
                                    color='HIGH_RISK_COUNT', color_continuous_scale='Reds',
                                    title="High-Risk Merchants per PM")
                    fig_pc.update_layout(height=350, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                    st.plotly_chart(fig_pc, width='stretch')

            if 'ZSCORE_SV' in df_c4.columns:
                st.markdown("<br>", unsafe_allow_html=True)
                section_label(f"Tri-Dimensional Z-Score Distributions (Anomaly Threshold: {z_thresh_val})")
                
                z1, z2, z3 = st.columns(3)
                
                # Helper function for drawing Z-Histograms
                def _draw_z_hist(df, col_name, title, threshold):
                    fig_z = px.histogram(df, x=col_name, color='CHURN_RISK',
                                         nbins=25, barmode='overlay',
                                         color_discrete_map={'HIGH RISK ⚠️': RED, 'STABLE ✅': BLUE_ACC},
                                         title=title)
                    fig_z.add_vline(x=threshold, line_dash='dash', line_color=RED,
                                    annotation_text=f"Cutoff ({threshold})",
                                    annotation_font_color=RED)
                    fig_z.update_layout(height=300, showlegend=False, margin=dict(l=10, r=10, t=40, b=10), **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                    return fig_z

                z1.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_SV', "Volume Outlier Map", z_thresh_val), width='stretch')
                z2.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_FBI', "FBI Outlier Map", z_thresh_val), width='stretch')
                z3.plotly_chart(_draw_z_hist(df_c4, 'ZSCORE_GROWTH', "Growth Outlier Map", z_thresh_val), width='stretch')

        if len(df_high) > 0:
            section_label("⚠️ High-Risk Merchant Details")
            risk_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','CHURN_RISK',
                                      'WEEKS_ACTIVE','SV_GROWTH_RATE',
                                      'ACHIEVEMENT_PCT','ZSCORE_SV', 'ZSCORE_FBI', 'ZSCORE_GROWTH'] if c in df_high.columns]
            df_rd = df_high[risk_cols].copy()
            if 'SV_GROWTH_RATE' in df_rd.columns:
                df_rd['SV_GROWTH_RATE'] = (df_rd['SV_GROWTH_RATE']*100).round(1).astype(str)+'%'
            if 'ACHIEVEMENT_PCT' in df_rd.columns:
                df_rd['ACHIEVEMENT_PCT'] = df_rd['ACHIEVEMENT_PCT'].round(1).astype(str)+'%'
                
            def style_z_scores(row):
                styles = [''] * len(row)
                for idx, col in enumerate(df_rd.columns):
                    if col.startswith('ZSCORE') and row[col] < z_thresh_val:
                        styles[idx] = f'color: {RED}; font-weight: bold;'
                return styles
                
            st.dataframe(df_rd.style.apply(style_z_scores, axis=1).format({c: "{:.3f}" for c in ['ZSCORE_SV', 'ZSCORE_FBI', 'ZSCORE_GROWTH']}).hide(axis="index"), width='stretch')
            st.download_button("⬇️ Export High-Risk List", df_rd.to_csv(index=False, encoding='utf-8-sig'),
                               "churn_risk_merchants.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — MERCHANT EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    tab_desc("Fully interactive explorer. Apply any combination of filters, search, sort, and export to CSV. Your personal decision-making workspace.")

    if has_card and has_mon:
        df_exp = run_ml(df_card, df_mon, df_target)
    elif has_card:
        df_exp = df_card.copy()
    else:
        df_exp = df_mon.copy()

    # ── All Controls Inline ──
    section_label("🎛️ Explorer Filters")
    ef1, ef2, ef3, ef4 = st.columns(4)

    with ef1:
        if 'CLUSTER' in df_exp.columns:
            sel_ec = st.multiselect("Cluster", ['PREMIUM','REGULER','PASIF'],
                                    default=['PREMIUM','REGULER','PASIF'], key="e_clust")
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

    # ── Sort & Display ──
    show_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','CHURN_RISK',
                              'TOTAL_SV','TOTAL_TRX','TOTAL_FBI','RASIO_ONUS',
                              'WEEKS_ACTIVE','YTD_VOL','ACHIEVEMENT_PCT',
                              'SV_GROWTH_RATE','ZSCORE_SV'] if c in df_exp.columns]

    es1, es2 = st.columns([3,1])
    sort_e = es1.selectbox("Sort by", show_cols, key="e_sort")
    asc_e  = es2.radio("Order", ["Desc","Asc"], horizontal=True, key="e_asc")

    if sort_e:
        df_exp_s = df_exp[show_cols].sort_values(sort_e, ascending=(asc_e=='Asc')).reset_index(drop=True)
    else:
        df_exp_s = df_exp[show_cols].reset_index(drop=True)
    
    st.dataframe(df_exp_s, width='stretch', height=480)

    st.download_button("⬇️ Export Filtered View as CSV",
                       df_exp_s.to_csv(index=False, encoding='utf-8-sig'),
                       "merchant_explorer_export.csv", "text/csv", type="primary")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — AI INSIGHTS & DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    tab_desc("Predictive AI capabilities that calculate Run-Rate projections vs Targets, track Historical Seasonality, and hunt for sudden 'Silent Churn' drop anomalies across the entire fleet.")
    
    if not (has_card and has_mon):
        st.warning("⚠️ AI Insights require both Card Share and Monitoring data to be processed.")
    else:
        # Re-fetch raw monitoring for weeks and realisasi for months
        # Pass file mtime to bust cache when master file is updated
        mon_mtime = os.path.getmtime(PATH_MON) if os.path.exists(PATH_MON) else None
        dt_mon = parse_monitoring_sheet(PATH_MON, "PerMerchant", mon_mtime) if has_mon else pd.DataFrame()
        dt_real = parse_realisasi(PATH_CARD) if has_card else pd.DataFrame()
        
        if dt_mon.empty or dt_real.empty:
            st.error("Error loading underlying matrix data for AI insights.")
        else:
            # Prepare Target DB merging
            df_curr_yr = dt_mon[dt_mon['PERIODE'].astype(str) == '2026'].copy()
            W_COLS = sorted([c for c in df_curr_yr.columns if c.startswith('W') and c[1:].isdigit()])
            for w in W_COLS:
                df_curr_yr[w] = pd.to_numeric(df_curr_yr[w], errors='coerce').fillna(0)
                
            # --- FEATURE 1: SILENT CHURN ANOMALY SCANNER ---
            st.markdown("<br>", unsafe_allow_html=True)
            section_label("🚨 Fleet-Wide Sudden Drop Monitor (Silent Churn)")
            st.markdown("This algorithm scans the 53-week matrix and identifies merchants whose most recent active week crashed below a dynamic threshold compared to their own 4-week moving average.")
            
            # Find the most recent active week across the whole fleet dynamically
            # We look for the highest week number where total volume > 0
            latest_wk_num = 0
            for w in reversed(W_COLS):
                if df_curr_yr[w].sum() > 0:
                    latest_wk_num = int(w[1:])
                    break
                    
            if latest_wk_num < 5:
                st.info("Insufficient 2026 weeks logged to calculate a 4-week trailing average for fleet drop detection.")
            else:
                wk_curr = f"W{latest_wk_num:02d}"
                wk_t1   = f"W{latest_wk_num-1:02d}"
                wk_t2   = f"W{latest_wk_num-2:02d}"
                wk_t3   = f"W{latest_wk_num-3:02d}"
                wk_t4   = f"W{latest_wk_num-4:02d}"
                
                slider_drop = st.slider("Drop Threshold Alert Trigger", min_value=10, max_value=80, value=30, step=5, format="%d%%")
                threshold_pct = -1 * (slider_drop / 100.0)
                
                # Filter to merchant-level rows only (exclude PM aggregates like RIFALDI)
                _scan_cols = ['NAME', 'KET', 'YTD', wk_t4, wk_t3, wk_t2, wk_t1, wk_curr]
                if 'ROW_NUM' in df_curr_yr.columns:
                    _scan_cols.append('ROW_NUM')
                    df_scan = df_curr_yr[_scan_cols].copy()
                    df_scan = df_scan[df_scan['ROW_NUM'].notna()].drop(columns=['ROW_NUM'])
                else:
                    df_scan = df_curr_yr[_scan_cols].copy()
                df_scan['Trailing_4W_Avg'] = df_scan[[wk_t4, wk_t3, wk_t2, wk_t1]].mean(axis=1)
                
                # Formula: (Current - Avg) / Avg. Guard against 0 div.
                df_scan['WoW_Variance'] = np.where(
                    df_scan['Trailing_4W_Avg'] > 0,
                    (df_scan[wk_curr] - df_scan['Trailing_4W_Avg']) / df_scan['Trailing_4W_Avg'],
                    0
                )
                
                # We only care about drops (negative variance) exceeding the threshold
                # And we don't care about entirely dead merchants (Avg == 0)
                anomalies = df_scan[(df_scan['WoW_Variance'] <= threshold_pct) & (df_scan['Trailing_4W_Avg'] > 0)].copy()
                anomalies = anomalies.sort_values('WoW_Variance', ascending=True)
                
                st.markdown(f"**Anomalies found for current week ({wk_curr}):** `{len(anomalies)}` merchants dropped by `{slider_drop}%` or worse.")
                
                if not anomalies.empty:
                    # Formatting
                    anom_disp = anomalies[['NAME', 'Trailing_4W_Avg', wk_curr, 'WoW_Variance']].copy()
                    anom_disp['WoW_Variance'] = (anom_disp['WoW_Variance']*100).round(1).astype(str) + "%"
                    anom_disp['Trailing_4W_Avg'] = anom_disp['Trailing_4W_Avg'].apply(lambda x: f"Rp {x/1e6:,.1f}Jt" if x>=1e6 else f"{x:,.0f}")
                    anom_disp[wk_curr] = anom_disp[wk_curr].apply(lambda x: f"Rp {x/1e6:,.1f}Jt" if x>=1e6 else f"{x:,.0f}")
                    anom_disp.rename(columns={'Trailing_4W_Avg': '4-Week Avg', wk_curr: 'Latest Week'}, inplace=True)
                    
                    st.dataframe(
                        anom_disp.style.map(lambda x: f"color: {RED}; font-weight: bold", subset=['WoW_Variance']),
                        width='stretch', hide_index=True
                    )
                else:
                    st.success(f"No massive {slider_drop}% drops detected fleet-wide! The portfolio is stable.")
            

            # --- FEATURE 2: MERCHANT DEEP DIVE (AI Text & Seasonality) ---
            st.markdown("<br>", unsafe_allow_html=True)
            section_label("🔍 Deep Dive & Projection (Specific Merchant)")
            
            # ── FIX: Exclude PM-aggregate rows (they have no MERCHANT_CODE/ROW_NUM)
            # The PerMerchant sheet has PM-level summary rows (e.g. "RIFALDI") that
            # should NOT appear in the merchant selectbox. Real merchant rows have
            # a numeric ROW_NUM (col 0) and a MERCHANT_CODE (col 1, e.g. "ANCOLVOL").
            if 'ROW_NUM' in df_curr_yr.columns:
                df_merch_only = df_curr_yr[df_curr_yr['ROW_NUM'].notna()].copy()
            else:
                df_merch_only = df_curr_yr.copy()
            all_merch = sorted(df_merch_only['NAME'].dropna().unique())
            
            # Debug audit — verify column mapping (toggle off in production)
            with st.expander("🔬 Data Audit: Column Mapping Debug", expanded=False):
                st.caption("Use this panel to verify which rows are merchants vs PM aggregates.")
                audit_cols = [c for c in ['NAME','MERCHANT_CODE','ROW_NUM','KET','PERIODE','YTD'] if c in df_curr_yr.columns]
                st.dataframe(
                    df_curr_yr[df_curr_yr['PERIODE'].astype(str)=='2026'][audit_cols].drop_duplicates('NAME').sort_values('NAME').reset_index(drop=True),
                    width='stretch', hide_index=True, height=200
                )
                st.info(f"Selectbox populated with **{len(all_merch)}** unique merchant names (PM aggregates excluded).")
            
            sel_merch = st.selectbox("Select Merchant Entity to Profile:", all_merch)
            
            if sel_merch:
                col_txt, col_graph = st.columns([1, 1], gap="large")
                
                ## 1. Calculating Run Rate vs Target
                df_m_2026 = df_curr_yr[df_curr_yr['NAME'] == sel_merch]
                df_m_tgt  = dt_mon[(dt_mon['NAME'] == sel_merch) & (dt_mon['PERIODE'].astype(str) == 'Target')]
                
                ytd_actual = float(df_m_2026['YTD'].iloc[0]) if not df_m_2026.empty else 0
                fy_target = float(df_m_tgt['FY'].iloc[0]) if not df_m_tgt.empty else 0
                
                # Project EOY using active weeks
                active_weeks_count = 0
                if not df_m_2026.empty:
                    w_vals = df_m_2026[W_COLS].iloc[0].values
                    active_w_arr = [v for v in w_vals if v > 0]
                    active_weeks_count = len(active_w_arr)
                    
                proj_eoy = (ytd_actual / active_weeks_count * 52) if active_weeks_count > 0 else 0
                
                ## 2. Calculating Historical Seasonality
                # Filter realisasi for this merchant
                merch_real = dt_real[dt_real['MERCHANT_GROUP'] == sel_merch].copy()
                seasonality_str = "No historical seasonality data found."
                
                season_df = pd.DataFrame()
                if not merch_real.empty and 'TRX_MONTH' in merch_real.columns and 'SV' in merch_real.columns:
                    # Extract month 1-12
                    merch_real['MonthIdx'] = merch_real['TRX_MONTH'].astype(str).str[-2:].astype(int)
                    # Get average SV for each month across all years
                    mo_avg = merch_real.groupby('MonthIdx')['SV'].mean().reset_index()
                    all_mo_avg = mo_avg['SV'].mean()
                    
                    if all_mo_avg > 0:
                        mo_avg['Multiplier'] = mo_avg['SV'] / all_mo_avg
                        season_df = mo_avg.copy()
                        
                        # Find Peak Month
                        peak_mo = season_df.loc[season_df['Multiplier'].idxmax()]
                        peak_name = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][int(peak_mo['MonthIdx'])-1]
                        peak_mult = peak_mo['Multiplier']
                        
                        seasonality_str = f"They historically demonstrate a **{peak_mult:.1f}x surge in {peak_name}** compared to their baseline yearly average."
                
                ## 3. Emitting the AI Text Analysis
                with col_txt:
                    section_label(f"🤖 AI Insight Summary: {sel_merch}")
                    
                    # Math for text
                    rate_pct = (proj_eoy / fy_target * 100) if fy_target > 0 else 0
                    if fy_target == 0:
                        status_str = f"No FY Target is registered for {sel_merch}."
                    elif proj_eoy >= fy_target:
                        over_by = proj_eoy - fy_target
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

                    # Executive summary card
                    st.markdown(
                        f"""<div style="border-left:5px solid {exec_color};background:{exec_color}18;
                            border-radius:0 12px 12px 0;padding:16px 20px;margin-bottom:14px;">
                            <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;
                                        letter-spacing:.08em;color:{exec_color};">{exec_icon} STATUS: {exec_label}</div>
                            <div style="font-size:0.88rem;margin-top:8px;
                                        color:{_pp6['TEXT_PRI']};line-height:1.65;">
                                <b>{sel_merch}</b> has accumulated
                                <code>Rp {ytd_actual/1e9:,.2f}B</code> YTD across
                                <b>{active_weeks_count}</b> active weeks.<br>{status_str}
                            </div>
                        </div>""",
                        unsafe_allow_html=True
                    )
                    if seasonality_str != "No historical seasonality data found.":
                        st.markdown(
                            f"""<div style="background:{_pp6['SURFACE2']};border:1px solid {_pp6['BORDER']};
                                border-radius:10px;padding:12px 16px;font-size:0.84rem;
                                color:{_pp6['TEXT_PRI']};margin-bottom:14px;">
                                <b>🌊 Seasonality Intelligence:</b> {seasonality_str}
                            </div>""",
                            unsafe_allow_html=True
                        )

                    # Year-end projection metric
                    st.metric(
                        label="Projected Year-End Run Rate",
                        value=f"Rp {proj_eoy/1e9:,.2f} B",
                        delta=f"{rate_pct:.1f}% of Target",
                        delta_color="normal" if rate_pct >= 100 else "inverse"
                    )

                    # XAI — Feature importance bar chart
                    st.markdown("<br>", unsafe_allow_html=True)
                    with st.expander("🧠 Why this assessment? — Explainable AI (XAI)", expanded=True):
                        fi_scores = {}
                        if active_weeks_count > 0 and latest_wk_num > 0:
                            inactivity_ratio = 1.0 - (active_weeks_count / latest_wk_num)
                            fi_scores["Inactivity Rate"]     = round(min(inactivity_ratio * 100, 100), 1)
                            fi_scores["Target Gap"]          = round(max(0, 100 - rate_pct) / 2, 1)
                            weekly_v = ytd_actual / active_weeks_count
                            fi_scores["Low Weekly Velocity"] = round(max(0, 100 - min(weekly_v / 1e7, 100)), 1)
                        if not merch_real.empty and 'SV' in merch_real.columns and len(merch_real) >= 6:
                            recent = merch_real.sort_values('TRX_MONTH').tail(3)['SV'].mean()
                            older  = merch_real.sort_values('TRX_MONTH').head(3)['SV'].mean()
                            if older > 0:
                                fi_scores["Declining Volume Trend"] = round(min(max(0, (1 - recent / older) * 100), 100), 1)
                        defaults = {
                            "Inactivity Rate": 15.0, "Target Gap": 30.0,
                            "Low Weekly Velocity": 20.0, "Declining Volume Trend": 25.0,
                        }
                        for k, v in defaults.items():
                            fi_scores.setdefault(k, v)

                        fi_df = (
                            pd.DataFrame(list(fi_scores.items()), columns=["Factor", "Impact Score"])
                            .sort_values("Impact Score", ascending=True)
                        )
                        bar_colors = [
                            "#F87171" if s >= 50 else ("#FBBF24" if s >= 25 else "#34D399")
                            for s in fi_df["Impact Score"]
                        ]
                        fig_fi = go.Figure(go.Bar(
                            x=fi_df["Impact Score"],
                            y=fi_df["Factor"],
                            orientation="h",
                            marker_color=bar_colors,
                            marker_line_width=0,
                            text=[f"{v:.1f}" for v in fi_df["Impact Score"]],
                            textposition="outside",
                            hovertemplate="<b>%{y}</b><br>Impact: <b>%{x:.1f}</b><extra></extra>",
                        ))
                        fig_fi.update_layout(
                            title="Risk Factor Contribution",
                            height=240,
                            margin=dict(l=0, r=50, t=36, b=0),
                            xaxis=dict(
                                title="Risk Impact Score (0–100)",
                                range=[0, max(fi_df["Impact Score"].max() * 1.25, 10)],
                                showgrid=False,
                                tickfont=dict(color=_pp6["TEXT_SEC"]),
                            ),
                            yaxis=dict(showgrid=False, tickfont=dict(color=_pp6["TEXT_PRI"])),
                            **_chart_base(),
                        )
                        st.plotly_chart(fig_fi, width="stretch")
                        st.caption("Higher bars = stronger contribution to the AI's risk assessment for this merchant.")

                
                with col_graph:
                    if not season_df.empty:
                        # Draw Seasonality Spider or Line
                        mo_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                        # Ensure 12 months present
                        full_season = pd.DataFrame({'MonthIdx': range(1, 13)})
                        full_season = pd.merge(full_season, season_df, on='MonthIdx', how='left').fillna({'Multiplier': 1.0, 'SV': 0})
                        full_season['MonthName'] = mo_names
                        
                        fig_sea = px.line(full_season, x='MonthName', y='Multiplier', markers=True, 
                                          title=f"Historical Seasonality Curve ({sel_merch})",
                                          labels={'Multiplier': 'Volume Multiplier (1.0 = Average)'})
                        
                        fig_sea.add_hline(y=1.0, line_dash="dash", line_color=get_palette()['TEXT_SEC'], annotation_text="Baseline Avg (1.0x)")
                        fig_sea.update_traces(line=dict(width=3, color=get_palette()['GOLD']), marker=dict(size=8))
                        fig_sea.update_layout(height=350, **_chart_base(), xaxis=_xaxis(), yaxis=_yaxis())
                        
                        st.plotly_chart(fig_sea, width='stretch')
                    else:
                        st.info(f"Insufficient historical Realisasi monthly data to chart statistical seasonality for {sel_merch}.")
