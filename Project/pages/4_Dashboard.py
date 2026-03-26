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

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="BTN Anchor Dashboard", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

* { font-family: 'Inter', sans-serif; }

.page-title {
    font-size: 1.9rem; font-weight: 700;
    color: #1F3864; border-bottom: 3px solid #F2C94C;
    padding-bottom: 10px; margin-bottom: 20px;
}
.tab-desc {
    background: #EEF2FF; border-left: 4px solid #1F3864;
    padding: 10px 16px; border-radius: 6px;
    font-size: 0.87rem; color: #333; margin-bottom: 20px;
}
.filter-box {
    background: #F8F9FC; border: 1px solid #D4D8E8;
    border-radius: 10px; padding: 16px 20px; margin-bottom: 20px;
}
.filter-active {
    background: #FFF8E1; border: 1.5px solid #F2C94C;
    border-radius: 10px; padding: 16px 20px; margin-bottom: 20px;
}
.kpi-row { display: flex; gap: 12px; margin-bottom: 20px; }
.kpi { background: linear-gradient(135deg,#1F3864,#2e4f91);
    border-radius: 10px; padding: 18px; color: white; flex: 1;
    box-shadow: 0 3px 12px rgba(31,56,100,.2); text-align: center; }
.kpi .val { font-size: 1.7rem; font-weight: 700; color: #F2C94C; }
.kpi .lbl { font-size: 0.78rem; opacity: .85; margin-top: 4px; }
.kpi-danger { background: linear-gradient(135deg,#7B0000,#C0392B); border-radius: 10px;
    padding: 18px; color: white; flex: 1; text-align: center; }
.kpi-danger .val { font-size: 1.7rem; font-weight: 700; }
.kpi-success { background: linear-gradient(135deg,#155724,#28a745); border-radius: 10px;
    padding: 18px; color: white; flex: 1; text-align: center; }
.kpi-success .val { font-size: 1.7rem; font-weight: 700; }
.section { font-size: 1.1rem; font-weight: 700; color: #1F3864;
    border-left: 4px solid #F2C94C; padding-left: 10px;
    margin: 22px 0 12px 0; }
</style>
""", unsafe_allow_html=True)

# ── PATHS ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_DB    = os.path.join(BASE_DIR, "database", "staging.db")
PATH_CARD  = os.path.join(BASE_DIR, "data", "master", "master_card_share.xlsx")
PATH_MON   = os.path.join(BASE_DIR, "data", "master", "master_monitoring.xlsx")

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
        
        # Calculate totals
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
    # Row 4 = headers (cols: SEGMEN/PM, KET, %, NaN, Periode, FY, YTD, Week-01..)
    hdr_idx = None
    for i, row in raw.iterrows():
        vals = [str(v) for v in row if pd.notna(v)]
        if 'KET' in vals and 'Periode' in vals:
            hdr_idx = i
            break
    if hdr_idx is None:
        return pd.DataFrame()
    headers = raw.iloc[hdr_idx].tolist()
    # Find col indices
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
    
    # Extract week labels directly from headers (e.g. "Week-01" -> "W01")
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
        
        rec = {'NAME': name_val, 'KET': ket_val, 'PERIODE': periode_val,
               'FY': fy_val, 'YTD': ytd_val}
        for lbl, idx in zip(week_labels, week_indices):
            val = row.iloc[idx]
            rec[lbl] = pd.to_numeric(val, errors='coerce') if pd.notna(val) else 0
        records.append(rec)
    df_out = pd.DataFrame(records)
    # Force all week columns to numeric
    for w in week_labels:
        df_out[w] = pd.to_numeric(df_out[w], errors='coerce').fillna(0)
    return df_out

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
def run_ml(card, mon, tgt):
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
    km = KMeans(n_clusters=3, init='k-means++', n_init=20, random_state=42)
    df['CLUSTER_RAW'] = km.fit_predict(X_s)
    sv_order = df.groupby('CLUSTER_RAW')['AVG_SV'].mean().sort_values(ascending=False)
    rank = {c: i for i, c in enumerate(sv_order.index)}
    lbl  = {0: 'PREMIUM', 1: 'REGULER', 2: 'PASIF'}
    df['CLUSTER'] = df['CLUSTER_RAW'].map(lambda c: lbl[rank[c]])
    df['ZSCORE_SV'] = stats.zscore(np.log1p(df['AVG_SV']))
    df['CHURN_RISK'] = (
        (df['WEEKS_ACTIVE'] <= 2) |
        ((df['SV_GROWTH_RATE'] <= -0.99) & (df['ACHIEVEMENT_PCT'] < 5)) |
        ((df['CLUSTER'] == 'PASIF') & (df['ACHIEVEMENT_PCT'] < 1)) |
        (df['ZSCORE_SV'] < -1.2)
    ).map({True: 'HIGH RISK ⚠️', False: 'STABLE ✅'})
    return df

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("<div class='page-title'>🏦 BTN Anchor Merchant — Decision Intelligence Platform</div>", unsafe_allow_html=True)

# Quick status indicators
s1, s2, s3, s4, s5 = st.columns(5)
s1.metric("Card Share Data",  "✅ Ready" if has_card else "❌ Missing")
s2.metric("Monitoring Data", "✅ Ready" if has_mon  else "❌ Missing")
s3.metric("Target Data",     "✅ Ready" if has_tgt  else "⚠️ Not uploaded")
s4.metric("Card Share File", "✅ Ready" if os.path.exists(PATH_CARD) else "❌ Upload in Settings")
s5.metric("Monitoring File", "✅ Ready" if os.path.exists(PATH_MON)  else "❌ Upload in Settings")

st.markdown("---")

CLAMP  = {'PREMIUM': '#27AE60', 'REGULER': '#2F80ED', 'PASIF': '#EB5757'}

# ── TABS ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "💰 Card Share Analytics",
    "📅 Weekly Monitoring",
    "🤖 ML Segmentation",
    "⚠️ Churn & Risk Alerts",
    "🔍 Merchant Explorer"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARD SHARE
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("<div class='tab-desc'>Monthly payment type breakdown — TRANSACTION / SALES VOLUME / FEE BASED INCOME. Use <b>Year Filter</b> to focus on one year. Numbers are formatted for quick reading; charts show composition and trend.</div>", unsafe_allow_html=True)

    # KPIs from DB
    if not df_card.empty:
        k1, k2, k3, k4 = st.columns(4)
        k1.markdown(f"<div class='kpi'><div class='val'>Rp {df_card['TOTAL_SV'].sum()/1e9:,.1f}M</div><div class='lbl'>💰 YTD Sales Volume</div></div>", unsafe_allow_html=True)
        k2.markdown(f"<div class='kpi'><div class='val'>Rp {df_card['TOTAL_FBI'].sum()/1e6:,.0f}Jt</div><div class='lbl'>📈 YTD Fee-Based Income</div></div>", unsafe_allow_html=True)

        k3.markdown(f"<div class='kpi'><div class='val'>{df_card['TOTAL_TRX'].sum()/1e6:,.2f}M</div><div class='lbl'>🔄 YTD Transactions</div></div>", unsafe_allow_html=True)
        avg_onus = df_card['RASIO_ONUS'].mean() if 'RASIO_ONUS' in df_card.columns else 0
        k4.markdown(f"<div class='kpi'><div class='val'>{avg_onus*100:.1f}%</div><div class='lbl'>🎯 Avg On-Us Ratio</div></div>", unsafe_allow_html=True)
        st.markdown("")

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

            TYPE_COLORS = {
                'DEBIT ON US':    '#1F3864',
                'DEBIT OFF US':   '#2F80ED',
                'CREDIT OFF US':  '#F2994A',
                'QRIS ON US':     '#27AE60',
                'QRIS OFF US':    '#6FCF97',
            }

            SECTIONS = {
                'TRANSACTION':     ('🔄', '#1F3864'),
                'SALES VOLUME':    ('💰', '#1a6b3c'),
                'FEE BASED INCOME':('📈', '#7B3F00'),
            }

            for sec, (icon, accent) in SECTIONS.items():
                sec_cols = [c for c in df_hl.columns if c.startswith(f'{sec}__') and '__col' not in c]
                if not sec_cols: continue

                st.markdown(f"<div class='section'>{icon} {sec}</div>", unsafe_allow_html=True)

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
                    base = f'background:{accent}15;' if not is_ytd else f'background:{accent};color:white;font-weight:bold;'
                    # Highlight TOTAL col more
                    styles = []
                    for col in disp_fmt.columns:
                        if is_ytd:
                            styles.append(base)
                        elif total_col and col == total_col:
                            styles.append(f'background:{accent}22;font-weight:600;')
                        else:
                            styles.append('')
                    return styles

                st.dataframe(
                    disp_fmt.style.apply(style_table, axis=1),
                    use_container_width=True, hide_index=True, height=min(38 * len(disp_fmt) + 40, 520)
                )

                # Charts
                chart_data = display.copy()  # exclude YTD row from charts

                if chart_type in ("Stacked Bar", "Both") and type_cols:
                    melted = chart_data.melt(id_vars='Bulan', value_vars=type_cols, var_name='Type', value_name='Value')
                    color_map = {t: TYPE_COLORS.get(t, '#999') for t in type_cols}
                    fig_s = px.bar(melted, x='Bulan', y='Value', color='Type',
                                   color_discrete_map=color_map,
                                   barmode='stack',
                                   title=f"{sec} — Monthly Payment Type Composition",
                                   text_auto=False)
                    fig_s.update_layout(
                        height=340, legend=dict(orientation='h', y=-0.3),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=True, gridcolor='#e8e8e8')
                    )
                    fig_s.update_traces(marker_line_width=0)
                    st.plotly_chart(fig_s, use_container_width=True)

                if chart_type in ("Line Trend", "Both") and total_col:
                    cht = chart_data[['Bulan', total_col]].copy()
                    
                    # Calculate MoM growth
                    cht['MoM'] = cht[total_col].pct_change() * 100
                    
                    # Build text labels with value and MoM%
                    text_labels = []
                    for i, row in cht.iterrows():
                        val = fmt_num(row[total_col], sec)
                        mom = row['MoM']
                        if pd.isna(mom):
                            text_labels.append(val)
                        else:
                            sign = "+" if mom > 0 else ""
                            text_labels.append(f"{val}<br>({sign}{mom:.1f}%)")
                            
                    fig_l = go.Figure()
                    fig_l.add_trace(go.Scatter(
                        x=cht['Bulan'], y=cht[total_col],
                        mode='lines+markers+text',
                        name=total_col,
                        line=dict(color=accent, width=2.5),
                        marker=dict(size=8, color=accent),
                        text=text_labels,
                        textposition='top center',
                        textfont=dict(size=10)
                    ))
                    fig_l.update_layout(
                        title=f"{sec} — {total_col} Monthly Trend & MoM Growth",
                        height=340,
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=True, gridcolor='#e8e8e8', title=''),
                        showlegend=False
                    )
                    st.plotly_chart(fig_l, use_container_width=True)

                # Payment type share donut (YTD) - Not hidden
                if type_cols:
                    ytd_type = {t: float(ytd_nums.get(t, 0)) for t in type_cols}
                    if sum(ytd_type.values()) > 0:
                        st.markdown(f"**🍩 Payment Type Composition — {sec} (YTD)**")
                        fig_pie = go.Figure(go.Pie(
                            labels=list(ytd_type.keys()),
                            values=list(ytd_type.values()),
                            hole=0.55,
                            marker_colors=[TYPE_COLORS.get(t, '#999') for t in ytd_type],
                            textinfo='label+percent',
                            textfont_size=12
                        ))
                        fig_pie.update_layout(height=300, showlegend=True,
                                              margin=dict(t=10, b=10, l=10, r=10),
                                              legend=dict(orientation='h', y=-0.2))
                        st.plotly_chart(fig_pie, use_container_width=True)

                st.markdown("---")

        # Top Merchants overview from DB
        if not df_card.empty:
            st.markdown("<div class='section'>🏆 Top Merchants Analytics (YTD)</div>", unsafe_allow_html=True)
            
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
            
            # Add formatted strings
            format_dict = {
                'TOTAL_SV': lambda x: f"Rp {x/1e9:,.2f} M",
                'TOTAL_FBI': lambda x: f"Rp {x/1e6:,.1f} Jt",
                'TOTAL_TRX': lambda x: f"{x:,.0f}",
                'AVG_TRX_VAL': lambda x: f"Rp {x:,.0f}",
                'FBI_YIELD': lambda x: f"{x:.4f}%",
                'RASIO_ONUS': lambda x: f"{x*100:.1f}%"
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
            
            st.dataframe(
                disp_top.rename(columns=col_names).style.format(format_dict).background_gradient(cmap='Blues', subset=['Sales Volume', 'Transactions']).background_gradient(cmap='Greens', subset=['Fee Based Income', 'FBI Yield']),
                use_container_width=True, height=min(38 * len(disp_top) + 40, 500)
            )

            with st.expander("📋 Raw Card Share Data"):
                st.dataframe(df_c.reset_index(drop=True), use_container_width=True)
                st.download_button("⬇️ Download CSV", df_c.to_csv(index=False, encoding='utf-8-sig'), "card_share_data.csv", "text/csv")


            # ── GROWTH ANALYTICS (Realisasi) ──────────────────────────────────
            st.markdown("<br><div class='section'>📈 Top & Bottom Merchant Growth (MoM YoY)</div>", unsafe_allow_html=True)
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
                    
                    # Target metric selection
                    metric_sel = st.radio("Select Metric to Analyze", ["SALES VOLUME", "TRANSACTION", "FEE BASED INCOME"], horizontal=True, key="t1_metric_growth")
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
                        
                        # Style Growth % (col 4) and Delta (col 5)
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
                        st.markdown(f"**🟢 Top 10 by {metric_sel} Growth**")
                        st.dataframe(top_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), use_container_width=True)
                    with c2:
                        st.markdown(f"**🔴 Bottom 10 by {metric_sel} Growth**")  
                        st.dataframe(bot_10.style.apply(style_growth, axis=1).format(formatters).hide(axis="index"), use_container_width=True)
                        
                except Exception as e:
                    st.error(f"Could not calculate growth from Realisasi dates: {e}")
            else:
                st.info("Realisasi data for growth analytics not available in Master file.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WEEKLY MONITORING
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("<div class='tab-desc'>Weekly monitoring — <b>PM View</b>: total per Account Manager | <b>Merchant Monitor</b>: per-merchant weekly matrix. Select periods and see achievement vs target, heatmaps, and weekly trends.</div>", unsafe_allow_html=True)

    has_mon_file = os.path.exists(PATH_MON)
    if not has_mon_file:
        st.warning("⚠️ Master Monitoring file not configured. Upload it in ⚙️ Master Configuration.")
    else:
        mon_view = st.radio("📋 View", ["👤 PM View (PerPM)", "🏪 Merchant Monitor (PerMerchant)"], horizontal=True, key="t2_monview")
        sheet    = "PerPM" if "PM View" in mon_view else "PerMerchant"
        # Pass file mtime to bust cache when master file is updated
        mon_mtime = os.path.getmtime(PATH_MON)
        df_raw   = parse_monitoring_sheet(PATH_MON, sheet, mon_mtime)

        if df_raw.empty:
            st.warning(f"⚠️ Could not parse {sheet} sheet.")
        else:
            W_COLS = sorted([c for c in df_raw.columns if c.startswith('W') and c[1:].isdigit()])
            
            # Ensure all week columns are numeric in the raw dataframe
            for w in W_COLS:
                df_raw[w] = pd.to_numeric(df_raw[w], errors='coerce').fillna(0)

            if sheet == "PerPM":
                df_anchor = df_raw[df_raw['NAME'].str.upper().str.contains('ANCHOR', na=False)].copy()
                df_pm     = df_raw[~df_raw['NAME'].str.upper().str.contains('ANCHOR', na=False)].copy()
                df_pm     = df_pm[df_pm['NAME'].notna()].copy()
            else:
                df_anchor = pd.DataFrame()
                df_pm     = df_raw[df_raw['NAME'].notna()].copy()

            # PM filter for Merchant view
            if sheet == "PerMerchant":
                pm_names    = sorted(df_raw[df_raw['KET'] == 'Ach (%)']['NAME'].dropna().unique())
                sel_pm_filt = st.multiselect("👤 Filter by PM", pm_names, default=pm_names, key="t2_pm_merch")
                df_pm = df_pm[df_pm['NAME'].isin(sel_pm_filt)] if sel_pm_filt else df_pm

            PERIODE_ORDER = ['Target', '2026', '2025', '2024']
            p_col1, p_col2 = st.columns([2, 3])
            with p_col1:
                sel_periode = st.multiselect("📅 Periods", PERIODE_ORDER, default=['2026', 'Target'], key="t2_periode")
            df_pm_filt = df_pm[df_pm['PERIODE'].isin(sel_periode)] if sel_periode else df_pm
            # Show ALL 53 weeks — user wants the full year visible, empty weeks show as 0
            active_weeks = W_COLS

            # ── ANCHOR AGGREGATE (PM view only) ──────────────────────────────
            if sheet == "PerPM" and not df_anchor.empty:
                st.markdown("<div class='section'>🏛️ ANCHOR — Portfolio Aggregate</div>", unsafe_allow_html=True)
                anc_2026 = df_anchor[df_anchor['PERIODE'].astype(str) == '2026'].copy()
                anc_tgt  = df_anchor[df_anchor['PERIODE'].astype(str) == 'Target'].copy()

                if not anc_2026.empty and not anc_tgt.empty:
                    ytd_2026 = pd.to_numeric(anc_2026['YTD'].iloc[0], errors='coerce') or 0
                    fy_tgt   = pd.to_numeric(anc_tgt['FY'].iloc[0],  errors='coerce') or 1
                    ach_pct  = min(ytd_2026 / fy_tgt * 100, 200) if fy_tgt else 0
                    ak1, ak2, ak3 = st.columns(3)
                    ak1.markdown(f"<div class='kpi'><div class='val'>Rp {ytd_2026/1e9:,.2f}M</div><div class='lbl'>📊 YTD 2026 Volume</div></div>", unsafe_allow_html=True)
                    ak2.markdown(f"<div class='kpi'><div class='val'>Rp {fy_tgt/1e9:,.2f}M</div><div class='lbl'>🎯 FY Target</div></div>", unsafe_allow_html=True)
                    color_ach = '#27AE60' if ach_pct >= 80 else ('#F2C94C' if ach_pct >= 50 else '#EB5757')
                    ak3.markdown(f"<div class='kpi' style='background:linear-gradient(135deg,{color_ach}cc,{color_ach});'><div class='val'>{ach_pct:.1f}%</div><div class='lbl'>✅ Achievement vs Target</div></div>", unsafe_allow_html=True)

                avail_anc_cols = [c for c in ['KET','PERIODE','FY','YTD'] + active_weeks if c in df_anchor.columns]
                anc_disp = df_anchor[df_anchor['PERIODE'].isin(sel_periode)][avail_anc_cols].fillna(0)
                st.dataframe(anc_disp, use_container_width=True, hide_index=True)
                st.markdown("")

            # ── MAIN TABLE ────────────────────────────────────────────────────
            sec_label = "👤 PM Summary" if sheet == "PerPM" else "🏪 Merchant Weekly Matrix"
            st.markdown(f"<div class='section'>{sec_label}</div>", unsafe_allow_html=True)

            if len(sel_periode) < 4:
                st.markdown(f"<div class='filter-active'>🔶 <b>Filter:</b> {', '.join(sel_periode)} · {df_pm_filt['NAME'].nunique()} entities</div>", unsafe_allow_html=True)

            disp_cols      = ['NAME', 'KET', 'PERIODE', 'FY', 'YTD'] + active_weeks
            available_disp = [c for c in disp_cols if c in df_pm_filt.columns]
            st.dataframe(df_pm_filt[available_disp].fillna(0).reset_index(drop=True),
                         use_container_width=True, height=430)

            # ── ACHIEVEMENT vs TARGET BAR ─────────────────────────────────────
            df_2026  = df_pm_filt[df_pm_filt['PERIODE'].astype(str) == '2026'].copy()
            df_target_mon = df_pm[df_pm['PERIODE'].astype(str) == 'Target'].copy()
            if not df_2026.empty and not df_target_mon.empty:
                df_2026['YTD']   = pd.to_numeric(df_2026['YTD'], errors='coerce').fillna(0)
                df_target_mon['FY'] = pd.to_numeric(df_target_mon['FY'], errors='coerce').fillna(0)
                df_ach = pd.merge(
                    df_2026[['NAME','YTD']].rename(columns={'YTD':'YTD_2026'}),
                    df_target_mon[['NAME','FY']].rename(columns={'FY':'TARGET_FY'}),
                    on='NAME', how='inner'
                )
                df_ach['ACH_PCT'] = (df_ach['YTD_2026'] / df_ach['TARGET_FY'].replace(0, np.nan) * 100).clip(0, 300).fillna(0)
                df_ach = df_ach.sort_values('ACH_PCT', ascending=False)

                st.markdown("<div class='section'>🏆 Achievement vs Target (YTD 2026 / FY Target)</div>", unsafe_allow_html=True)
                fig_ach = go.Figure()
                bar_colors = ['#27AE60' if v >= 80 else '#F2C94C' if v >= 50 else '#EB5757' for v in df_ach['ACH_PCT']]
                fig_ach.add_trace(go.Bar(
                    x=df_ach['NAME'], y=df_ach['ACH_PCT'],
                    marker_color=bar_colors,
                    text=[f"{v:.1f}%" for v in df_ach['ACH_PCT']],
                    textposition='outside',
                    name='Achievement %'
                ))
                fig_ach.add_hline(y=100, line_dash='dash', line_color='#1F3864',
                                   annotation_text='100% Target', annotation_position='top right')
                fig_ach.update_layout(
                    height=360, xaxis_tickangle=-40,
                    plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                    yaxis=dict(title='Achievement (%)', showgrid=True, gridcolor='#e8e8e8'),
                    showlegend=False, title=("PM" if sheet=="PerPM" else "Merchant") + " Achievement vs FY Target"
                )
                st.plotly_chart(fig_ach, use_container_width=True)

            # ── WEEKLY HEATMAP ────────────────────────────────────────────────
            if df_2026.empty:
                df_2026 = df_pm_filt[df_pm_filt['PERIODE'].astype(str) == '2026'].copy()
            # Only show heatmap for weeks that have at least some data
            data_weeks = [c for c in W_COLS if (df_2026[c].fillna(0) != 0).any()] if not df_2026.empty else []
            if not df_2026.empty and data_weeks:
                st.markdown("<div class='section'>🗓️ Weekly Activity Heatmap (2026)</div>", unsafe_allow_html=True)
                df_heat = df_2026.copy()
                df_heat[data_weeks] = df_heat[data_weeks].apply(pd.to_numeric, errors='coerce').fillna(0)
                heat_data = df_heat.set_index('NAME')[data_weeks]

                fig_heat = px.imshow(
                    heat_data,
                    color_continuous_scale=[[0,'#EEF2FF'],[0.3,'#6FA3EF'],[0.7,'#1F3864'],[1,'#F2C94C']],
                    aspect='auto',
                    title="Weekly Volume Heatmap — darker = higher value",
                    labels=dict(x="Week", y="", color="Value")
                )
                fig_heat.update_layout(
                    height=max(200, 35 * len(heat_data) + 80),
                    xaxis=dict(dtick=2),
                    coloraxis_showscale=True,
                    plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)'
                )
                fig_heat.update_traces(hovertemplate='<b>%{y}</b><br>%{x}: %{z:,.0f}<extra></extra>')
                st.plotly_chart(fig_heat, use_container_width=True)

            # ── WEEKLY TREND LINE ─────────────────────────────────────────────
            if not df_2026.empty and data_weeks:
                st.markdown("<div class='section'>📈 Weekly Trend & WoW Growth — 2026</div>", unsafe_allow_html=True)
                df_trend = df_2026.copy()
                df_trend[data_weeks] = df_trend[data_weeks].apply(pd.to_numeric, errors='coerce').fillna(0)
                
                # We need to calculate WoW pct change per entity
                # Melt first
                df_long = df_trend[['NAME'] + data_weeks].melt(id_vars='NAME', var_name='Week', value_name='Value')
                
                # Sort by NAME and Week to calculate properly
                # Week format is 'W01', 'W02'... sorting string is fine
                df_long = df_long.sort_values(['NAME', 'Week'])
                
                # Calculate WoW % change
                df_long['WoW'] = df_long.groupby('NAME')['Value'].pct_change() * 100
                
                # Format text labels
                df_long['Text'] = df_long.apply(
                    lambda row: f"{row['Value']/1e9:.1f}M<br>(+{row['WoW']:.1f}%)" if pd.notna(row['WoW']) and row['WoW'] > 0 
                                else f"{row['Value']/1e9:.1f}M<br>({row['WoW']:.1f}%)" if pd.notna(row['WoW']) and row['WoW'] < 0
                                else f"{row['Value']/1e9:.1f}M", axis=1
                )
                
                fig_line = px.line(
                    df_long, x='Week', y='Value', color='NAME', text='Text',
                    markers=True, title="Weekly Volume Trend by " + ("PM" if sheet=="PerPM" else "Merchant")
                )
                fig_line.update_traces(marker=dict(size=6), line=dict(width=2.5), textposition='top center', textfont_size=9)
                fig_line.update_layout(
                    height=450,
                    legend=dict(orientation='h', y=-0.35, title=None),
                    plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=False, dtick=2),
                    yaxis=dict(showgrid=True, gridcolor='#e8e8e8', title='')
                )
                st.plotly_chart(fig_line, use_container_width=True)

            st.download_button("⬇️ Export Table",
                df_pm_filt[available_disp].to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                f"{sheet}_export.csv", "text/csv")

    # KPI footer from DB
    if not df_mon.empty:
        st.markdown("---")
        k1, k2, k3 = st.columns(3)
        k1.markdown(f"<div class='kpi'><div class='val'>{len(df_mon):,}</div><div class='lbl'>🏪 Merchants in DB</div></div>", unsafe_allow_html=True)
        avg_wa = df_mon['WEEKS_ACTIVE'].mean() if 'WEEKS_ACTIVE' in df_mon.columns else 0
        k2.markdown(f"<div class='kpi'><div class='val'>{avg_wa:.1f}</div><div class='lbl'>📆 Avg Weeks Active</div></div>", unsafe_allow_html=True)
        ytd_v  = df_mon['YTD_VOL'].sum() if 'YTD_VOL' in df_mon.columns else 0
        k3.markdown(f"<div class='kpi'><div class='val'>Rp {ytd_v/1e9:,.2f}M</div><div class='lbl'>💰 YTD Volume Total</div></div>", unsafe_allow_html=True)


with tab3:
    st.markdown("<div class='tab-desc'>K-Means++ Clustering (K=3) segments every merchant into PREMIUM, REGULER, or PASIF based on SV, FBI, growth rate, achievement, and activity.</div>", unsafe_allow_html=True)

    if not (has_card and has_mon):
        st.warning("⚠️ ML analysis requires **both** Card Share and Monitoring data to be processed first.")
    else:
        with st.spinner("Running K-Means++ ML Pipeline..."):
            df_ml = run_ml(df_card, df_mon, df_target)

        all_pm_ml = sorted(df_ml['PM'].dropna().unique().tolist()) if 'PM' in df_ml.columns else []

        # Controls
        mc1, mc2 = st.columns(2)
        with mc1:
            sel_pm_ml = st.multiselect("👤 Filter by PM", all_pm_ml, default=all_pm_ml, key="t3_pm")
        with mc2:
            sel_clust = st.multiselect("🏷️ Show Clusters", ['PREMIUM','REGULER','PASIF'],
                                       default=['PREMIUM','REGULER','PASIF'], key="t3_clust")

        df_f = df_ml[df_ml['CLUSTER'].isin(sel_clust)]
        if sel_pm_ml and 'PM' in df_f.columns:
            df_f = df_f[df_f['PM'].isin(sel_pm_ml)]

        filtered = len(sel_pm_ml) < len(all_pm_ml) or len(sel_clust) < 3
        if filtered:
            st.markdown(f"<div class='filter-active'>🔶 <b>Filter Active:</b> {len(df_f)} of {len(df_ml)} merchants shown</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='tab-desc'>Showing all <b>{len(df_f)}</b> merchants across all clusters.</div>", unsafe_allow_html=True)

        # Cluster counts
        cc1, cc2, cc3 = st.columns(3)
        for seg, color, col in [('PREMIUM','#27AE60',cc1),('REGULER','#2F80ED',cc2),('PASIF','#EB5757',cc3)]:
            n = len(df_f[df_f['CLUSTER'] == seg])
            col.markdown(f"""<div style='background:{color};border-radius:10px;padding:18px;
                text-align:center;color:white;margin-bottom:8px;'>
                <div style='font-size:2rem;font-weight:700;'>{n}</div>
                <div style='font-size:0.82rem;'>Merchants — {seg}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")
        sc1, sc2 = st.columns(2)

        with sc1:
            counts = df_f['CLUSTER'].value_counts().reset_index()
            counts.columns = ['CLUSTER','COUNT']
            fig_pie = px.pie(counts, names='CLUSTER', values='COUNT', hole=0.45,
                             title='Merchant Segmentation (K=3)',
                             color='CLUSTER', color_discrete_map=CLAMP)
            fig_pie.update_layout(height=360)
            st.plotly_chart(fig_pie, use_container_width=True)

        with sc2:
            fig_sc = px.scatter(df_f, x='AVG_SV', y='AVG_FBI',
                                color='CLUSTER', hover_name='MERCHANT_GROUP',
                                hover_data=['PM','ACHIEVEMENT_PCT','WEEKS_ACTIVE'],
                                size='WEEKS_ACTIVE',
                                log_x=True, log_y=True,
                                title="SV vs FBI — Cluster View (hover for details)",
                                color_discrete_map=CLAMP)
            fig_sc.update_layout(height=360)
            st.plotly_chart(fig_sc, use_container_width=True)

        st.markdown("<div class='section'>Cluster Radar Profile</div>", unsafe_allow_html=True)
        radar_m = ['AVG_SV','AVG_FBI','RASIO_ONUS','ACHIEVEMENT_PCT','WEEKS_ACTIVE']
        cm = df_f.groupby('CLUSTER')[radar_m].mean()
        norm = (cm - cm.min()) / (cm.max() - cm.min() + 1e-9)
        fig_r = go.Figure()
        for clust in ['PREMIUM','REGULER','PASIF']:
            if clust in norm.index:
                vals = norm.loc[clust].tolist() + [norm.loc[clust].tolist()[0]]
                cats = radar_m + [radar_m[0]]
                fig_r.add_trace(go.Scatterpolar(r=vals, theta=cats, fill='toself',
                    name=clust, line_color=CLAMP[clust]))
        fig_r.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0,1])),
                             height=420, title="Each cluster's normalised characteristic profile")
        st.plotly_chart(fig_r, use_container_width=True)

        if 'PM' in df_f.columns:
            st.markdown("<div class='section'>PM × Cluster Breakdown</div>", unsafe_allow_html=True)
            pm_cl = df_f.groupby(['PM','CLUSTER']).size().reset_index(name='COUNT')
            fig_stk = px.bar(pm_cl, x='PM', y='COUNT', color='CLUSTER',
                             barmode='stack', title="Cluster Distribution per Account Manager",
                             color_discrete_map=CLAMP)
            fig_stk.update_layout(height=380)
            st.plotly_chart(fig_stk, use_container_width=True)

        with st.expander("📋 View ML Results Table"):
            show_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','AVG_SV','AVG_FBI',
                                     'ACHIEVEMENT_PCT','WEEKS_ACTIVE','ZSCORE_SV'] if c in df_f.columns]
            st.dataframe(df_f[show_cols].sort_values('AVG_SV', ascending=False).reset_index(drop=True), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CHURN & RISK
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("<div class='tab-desc'>Merchants flagged as <b>HIGH RISK ⚠️</b> meet at least one churn condition: low activity weeks, severe negative growth, PASIF cluster with near-zero achievement, or extreme negative Z-Score.</div>", unsafe_allow_html=True)

    if not (has_card and has_mon):
        st.warning("⚠️ Churn analysis requires both Card Share and Monitoring data.")
    else:
        df_churn_all = run_ml(df_card, df_mon, df_target)
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
            st.markdown(f"<div class='filter-active'>🔶 <b>Filter Active: High Risk Only</b> — {len(df_c4)} merchants shown</div>", unsafe_allow_html=True)
        elif risk_view == "Stable Only":
            df_c4 = df_c4[~churn_mask]
            st.markdown(f"<div class='filter-active'>🟢 <b>Filter Active: Stable Only</b> — {len(df_c4)} merchants shown</div>", unsafe_allow_html=True)

        df_high = df_c4[df_c4['CHURN_RISK'].str.contains("HIGH", na=False)]
        df_safe = df_c4[~df_c4['CHURN_RISK'].str.contains("HIGH", na=False)]
        total   = len(df_c4)

        # KPI
        ch_a, ch_b, ch_c = st.columns(3)
        ch_a.markdown(f"<div class='kpi-danger'><div style='font-size:2.2rem;font-weight:700;'>{len(df_high)}</div><div>⚠️ High Churn Risk</div></div>", unsafe_allow_html=True)
        ch_b.markdown(f"<div class='kpi-success'><div style='font-size:2.2rem;font-weight:700;'>{len(df_safe)}</div><div>✅ Stable</div></div>", unsafe_allow_html=True)
        rate = len(df_high)/total*100 if total > 0 else 0
        ch_c.markdown(f"<div class='kpi'><div class='val'>{rate:.1f}%</div><div class='lbl'>Churn Rate (filtered)</div></div>", unsafe_allow_html=True)

        st.markdown("")

        if total > 0:
            ch_x, ch_y = st.columns(2)
            with ch_x:
                fig_rc = px.pie(df_c4, names='CHURN_RISK',
                                color='CHURN_RISK',
                                color_discrete_map={'HIGH RISK ⚠️':'#C0392B','STABLE ✅':'#27AE60'},
                                hole=0.4, title="Churn Risk Breakdown")
                fig_rc.update_layout(height=340)
                st.plotly_chart(fig_rc, use_container_width=True)
            with ch_y:
                if 'PM' in df_high.columns and len(df_high) > 0:
                    pm_churn = df_high.groupby('PM').size().reset_index(name='HIGH_RISK_COUNT')
                    fig_pc = px.bar(pm_churn.sort_values('HIGH_RISK_COUNT', ascending=False),
                                    x='PM', y='HIGH_RISK_COUNT',
                                    color='HIGH_RISK_COUNT', color_continuous_scale='Reds',
                                    title="High-Risk Merchants per PM")
                    fig_pc.update_layout(height=340)
                    st.plotly_chart(fig_pc, use_container_width=True)

            if 'ZSCORE_SV' in df_c4.columns:
                st.markdown("<div class='section'>Z-Score Distribution — Churn vs Stable</div>", unsafe_allow_html=True)
                fig_z = px.histogram(df_c4, x='ZSCORE_SV', color='CHURN_RISK',
                                     nbins=25, barmode='overlay',
                                     color_discrete_map={'HIGH RISK ⚠️':'#EB5757','STABLE ✅':'#2F80ED'},
                                     title="Z-Score Distribution")
                fig_z.add_vline(x=-1.2, line_dash='dash', line_color='red',
                                annotation_text="Churn threshold (−1.2)")
                fig_z.update_layout(height=360)
                st.plotly_chart(fig_z, use_container_width=True)

        if len(df_high) > 0:
            st.markdown("<div class='section'>⚠️ High-Risk Merchant Details</div>", unsafe_allow_html=True)
            risk_cols = [c for c in ['MERCHANT_GROUP','PM','CLUSTER','CHURN_RISK',
                                      'WEEKS_ACTIVE','SV_GROWTH_RATE',
                                      'ACHIEVEMENT_PCT','ZSCORE_SV'] if c in df_high.columns]
            df_rd = df_high[risk_cols].copy()
            if 'SV_GROWTH_RATE' in df_rd.columns:
                df_rd['SV_GROWTH_RATE'] = (df_rd['SV_GROWTH_RATE']*100).round(1).astype(str)+'%'
            if 'ACHIEVEMENT_PCT' in df_rd.columns:
                df_rd['ACHIEVEMENT_PCT'] = df_rd['ACHIEVEMENT_PCT'].round(1).astype(str)+'%'
            if 'ZSCORE_SV' in df_rd.columns:
                df_rd['ZSCORE_SV'] = df_rd['ZSCORE_SV'].round(3)
            st.dataframe(df_rd.reset_index(drop=True), use_container_width=True)
            st.download_button("⬇️ Export High-Risk List", df_rd.to_csv(index=False, encoding='utf-8-sig'),
                               "churn_risk_merchants.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — MERCHANT EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown("<div class='tab-desc'>Fully interactive explorer. Apply any combination of filters, search, sort, and export to CSV. Your personal decision-making workspace.</div>", unsafe_allow_html=True)

    if has_card and has_mon:
        df_exp = run_ml(df_card, df_mon, df_target)
    elif has_card:
        df_exp = df_card.copy()
    else:
        df_exp = df_mon.copy()

    # ── All Controls Inline ──
    st.markdown("### 🎛️ Explorer Filters")
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
        st.markdown(f"<div class='filter-active'>🔶 <b>Filter Active:</b> Showing <b>{active_count:,}</b> of {all_count:,} merchants</div>", unsafe_allow_html=True)
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

    df_exp_s = df_exp[show_cols].sort_values(sort_e, ascending=(asc_e=='Asc')).reset_index(drop=True)
    st.dataframe(df_exp_s, use_container_width=True, height=480)

    st.download_button("⬇️ Export Filtered View as CSV",
                       df_exp_s.to_csv(index=False, encoding='utf-8-sig'),
                       "merchant_explorer_export.csv", "text/csv", type="primary")
