import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
import io
import re
import shutil
from datetime import datetime

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)
from utils.theme import apply_theme, page_header, section_label, GOLD, SURFACE, BORDER, TEXT_SEC

st.set_page_config(page_title="Monitoring Processor — BTN Anchor", page_icon="📅", layout="wide")
apply_theme()
page_header("📅", "Monitoring Weekly Processor", "Merge Master Excel with new weekly data, update Staging DB")

st.markdown(
    """<div class="tab-desc">Upload your existing <b>Master Monitoring Excel</b> and the <b>New Weekly Data</b>.
    The system updates weekly-specific columns, preserves formatting safely, and extracts database analytics.</div>""",
    unsafe_allow_html=True,
)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_DB    = os.path.join(BASE_DIR, "database", "staging.db")
PATH_MON   = os.path.join(BASE_DIR, "data", "master", "master_monitoring.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "data", "master", "backups_monitoring")
os.makedirs(os.path.dirname(PATH_DB), exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

def clean_val(v):
    if pd.isna(v) or v == "": return ""
    try: return float(str(v).replace(',', ''))
    except: return ""

if not os.path.exists(PATH_MON):
    st.error("❌ Master Monitoring File not found! Please upload it via **⚙️ Global Settings** first.")
    st.stop()

# ── Current master status ──
col_info, col_dl = st.columns([3, 1])
with col_info:
    mtime = datetime.fromtimestamp(os.path.getmtime(PATH_MON)).strftime("%d %b %Y %H:%M")
    msize = os.path.getsize(PATH_MON) // 1024
    st.success(f"✅ Master loaded — last updated **{mtime}** · {msize} KB")
with col_dl:
    with open(PATH_MON, "rb") as f:
        st.download_button("⬇️ Download Current Master", f,
                           file_name="master_monitoring_current.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── Rollback panel ──
mon_backups = sorted([b for b in os.listdir(BACKUP_DIR) if b.endswith(".xlsx")], reverse=True)
with st.expander(f"🔁 Rollback / Restore Previous Master ({len(mon_backups)} backups available)"):
    if not mon_backups:
        st.info("No backups yet. Backups are created automatically each time you process new data.")
    else:
        for bfile in mon_backups[:10]:
            bpath = os.path.join(BACKUP_DIR, bfile)
            bsize = os.path.getsize(bpath) // 1024
            btime = datetime.fromtimestamp(os.path.getmtime(bpath)).strftime("%d %b %Y %H:%M")
            rb1, rb2, rb3 = st.columns([4, 1, 1])
            rb1.markdown(f"📄 `{bfile}` — **{btime}** ({bsize} KB)")
            with open(bpath, "rb") as bf:
                rb2.download_button("⬇️ Download", bf, file_name=bfile,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"mdl_{bfile}")
            if rb3.button("♻️ Restore", key=f"mrestore_{bfile}"):
                shutil.copy2(bpath, PATH_MON)
                st.success(f"✅ Restored from backup: `{bfile}`")
                st.session_state.pop('mon_result', None)
                st.rerun()

# ── Show download result banner if processing just finished ──
if 'mon_result' in st.session_state:
    res = st.session_state['mon_result']
    st.markdown("---")
    st.success(f"🎉 Processing complete! Master updated at {res['timestamp']}. Download your files below:")
    rc1, rc2 = st.columns(2)
    rc1.download_button(
        label="📥 Download NEW Master (.xlsx)",
        data=res['excel_bytes'],
        file_name=res['excel_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary", key="mon_dl_new"
    )
    rc2.download_button(
        label="🔙 Download BACKUP",
        data=res['backup_bytes'],
        file_name=res['backup_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="mon_dl_bak"
    )
    if st.button("❌ Dismiss", key="mon_dismiss"):
        del st.session_state['mon_result']
        st.rerun()

st.markdown("---")
uploaded_csv = st.file_uploader("Upload NEW Weekly Data", type=['csv', 'xlsx', 'xls'])

if uploaded_csv is not None:
    if st.button("🚀 Process & Map Matrix", type="primary"):
        try:
            with st.spinner("Parsing Wide-Format Data..."):
                try:
                    if uploaded_csv.name.lower().endswith('.csv'):
                        df_csv = pd.read_csv(uploaded_csv, sep=',', on_bad_lines='skip')
                        if len(df_csv.columns) == 1:
                            uploaded_csv.seek(0)
                            df_csv = pd.read_csv(uploaded_csv, sep=';', on_bad_lines='skip')
                    else:
                        df_csv = pd.read_excel(uploaded_csv)
                except Exception as e:
                    st.error(f"Error reading file: {e}")
                    st.stop()
                    
                records = []
                week_cols = [c for c in df_csv.columns if 'Week' in str(c)]
                max_week = 0
                for c in week_cols:
                    match = re.search(r'Week\s+(\d+)', str(c))
                    if match:
                         week_num = int(match.group(1))
                         if week_num > max_week:
                             max_week = week_num

                for index, row in df_csv.iterrows():
                    merchant = str(row['MERCHANT_GROUP']).replace('nan', '').upper().strip()
                    if not merchant:
                         continue

                    trx_record = {'MERCHANT_GROUP': merchant, 'DIMENSI': 'TRX'}
                    vol_record = {'MERCHANT_GROUP': merchant, 'DIMENSI': 'VOL'}
                    fbi_record = {'MERCHANT_GROUP': merchant, 'DIMENSI': 'FBI'}
                    
                    for w in range(1, max_week + 1):
                        w_str = f"{w:02d}"
                        trx_col = next((c for c in df_csv.columns if 'TRX' in str(c) and f'Week {w_str}' in str(c)), None)
                        vol_col = next((c for c in df_csv.columns if 'VOL' in str(c) and f'Week {w_str}' in str(c)), None)
                        fbi_col = next((c for c in df_csv.columns if 'FBI' in str(c) and f'Week {w_str}' in str(c)), None)
                        
                        trx_record[w] = clean_val(row[trx_col] if trx_col else "")
                        vol_record[w] = clean_val(row[vol_col] if vol_col else "")
                        fbi_record[w] = clean_val(row[fbi_col] if fbi_col else "")
                    
                    records.extend([trx_record, vol_record, fbi_record])
                    
                st.info(f"Restructured CSV into {len(records)} matrix rows. Max Week Detected: {max_week}")
                
                lookup = {}
                for r in records:
                    key = (r['MERCHANT_GROUP'], r['DIMENSI'].upper().strip())
                    lookup[key] = r

            temp_excel_path = None
            with st.spinner("Injecting values into Master Excel Workbook (Preserving Formulas/Charts)..."):
                import win32com.client as win32
                import pythoncom
                import tempfile
                import shutil
                
                pythoncom.CoInitialize()
                
                temp_dir = tempfile.gettempdir()
                temp_excel_path = os.path.join(temp_dir, "temp_monitoring_master.xlsx")
                shutil.copy2(PATH_MON, temp_excel_path)
                
                excel_abs_path = os.path.abspath(temp_excel_path)
                
                excel = win32.Dispatch('Excel.Application')
                excel.Visible = False
                excel.DisplayAlerts = False
                updates_made = 0
                
                try:
                     wb = excel.Workbooks.Open(excel_abs_path)
                     sheet_name = '2026'
                     try:
                          ws = wb.Sheets(sheet_name)
                     except Exception as e:
                          st.error(f"Sheet '{sheet_name}' not found.")
                          wb.Close(SaveChanges=False)
                          excel.Quit()
                          st.stop()
                     
                     try:
                          ws_param = wb.Sheets('PARAMETER')
                          # X2 is row 2 column 24
                          ws_param.Cells(2, 24).Value = max_week
                     except Exception as e:
                          st.warning(f"Could not update PARAMETER!X2 max week constraint: {e}")
                          
                     last_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row # xlUp
                     start_row = 2
                     
                     for row_idx in range(start_row, last_row + 1):
                         cell_merchant = ws.Cells(row_idx, 1).Value
                         cell_dimensi = ws.Cells(row_idx, 3).Value
                         if not cell_merchant or not cell_dimensi: continue
                             
                         excel_merch = str(cell_merchant).strip().upper()
                         excel_dim = str(cell_dimensi).strip().upper()
                         key = (excel_merch, excel_dim)
                         
                         if key in lookup:
                             data_row = lookup[key]
                             week_data_array = []
                             for w in range(1, max_week + 1):
                                 week_data_array.append(data_row.get(w, ""))
                             if week_data_array:
                                 # Write correctly as 1D array row to cells
                                 ws.Range(ws.Cells(row_idx, 9), ws.Cells(row_idx, 9 + max_week - 1)).Value = [week_data_array]
                                 updates_made += 1
                                 
                     wb.Save()
                     wb.Close(SaveChanges=True)
                except Exception as e:
                     st.error(f"Excel COM error: {e}")
                     try:
                         wb.Close(SaveChanges=False)
                     except:
                         pass
                finally:
                     try:
                         excel.Quit()
                     except:
                         pass
                     pythoncom.CoUninitialize()
                     
                # BACKUP old master BEFORE overwriting
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"monitoring_backup_{timestamp}.xlsx"
                backup_path = os.path.join(BACKUP_DIR, backup_name)
                shutil.copy2(PATH_MON, backup_path)

                try:
                    shutil.copy2(temp_excel_path, PATH_MON)
                except Exception as e:
                    st.error(f"Failed to auto-update master file: {e}")
                     
                with open(temp_excel_path, "rb") as f:
                    excel_bytes = f.read()
                with open(backup_path, "rb") as bf:
                    backup_bytes = bf.read()
                
                st.session_state['mon_result'] = {
                    'timestamp': timestamp,
                    'excel_bytes': excel_bytes,
                    'excel_name': f"monitoring_master_{timestamp}.xlsx",
                    'backup_bytes': backup_bytes,
                    'backup_name': backup_name,
                }

            with st.spinner("Extracting fully to Staging Database..."):
                # Load strictly the correct sheets back into pandas
                df_mon = pd.read_excel(temp_excel_path, sheet_name="2026")
                df_mon['MERCHANT_GROUP'] = df_mon['MERCHANT_GROUP'].astype(str).str.strip().str.upper()
                df_mon['PM'] = df_mon['PM'].astype(str).str.strip().str.upper()
                
                week_cols = [c for c in df_mon.columns if isinstance(c, int)]
                weeks_with_data = [w for w in week_cols if df_mon[w].notna().any()]
                
                df_mon_long = df_mon.melt(
                    id_vars=['MERCHANT_GROUP', 'DIMENSI', 'PM', 'YTD'],
                    value_vars=weeks_with_data,
                    var_name='WEEK',
                    value_name='WEEKLY_VALUE'
                ).dropna(subset=['WEEKLY_VALUE'])
                
                df_mon_long['WEEK'] = df_mon_long['WEEK'].astype(int)
                
                df_mon_ytd = df_mon_long[df_mon_long['DIMENSI']=='VOL'].groupby('MERCHANT_GROUP').agg(
                    YTD_VOL         = ('YTD', 'first'),
                    VOL_WEEK_PERTAMA= ('WEEKLY_VALUE', 'first'),
                    VOL_WEEK_TERAKHIR= ('WEEKLY_VALUE', 'last'),
                    WEEKS_ACTIVE    = ('WEEKLY_VALUE', lambda x: (pd.to_numeric(x, errors='coerce').fillna(0) > 0).sum()),
                    PM              = ('PM', 'first')
                ).reset_index()
                
                df_mon_ytd['SV_GROWTH_RATE'] = (
                    (df_mon_ytd['VOL_WEEK_TERAKHIR'] - df_mon_ytd['VOL_WEEK_PERTAMA']) /
                    df_mon_ytd['VOL_WEEK_PERTAMA'].replace(0, pd.NA)
                ).fillna(0)
                
                df_target = pd.read_excel(temp_excel_path, sheet_name="Edit Target")
                df_target_clean = df_target[['MERCHANT GROUP', 'PM', 'VOL NEW', 'TRX NEW', 'FBI FIX']].copy()
                df_target_clean.columns = ['MERCHANT_GROUP', 'PM', 'TARGET_VOL_2026', 'TARGET_TRX_2026', 'TARGET_FBI_2026']
                df_target_clean = df_target_clean.dropna(subset=['MERCHANT_GROUP'])
                df_target_clean['MERCHANT_GROUP'] = df_target_clean['MERCHANT_GROUP'].astype(str).str.strip().str.upper()
                
                conn = sqlite3.connect(PATH_DB)
                df_mon_ytd.to_sql("raw_monitoring",  conn, if_exists="replace", index=False)
                df_target_clean.to_sql("raw_target", conn, if_exists="replace", index=False)
                
                # Store full weekly matrix for detailed dashboard views
                # Identify PERIODE column (e.g. Target/2026/2025/2024)
                all_cols = df_mon.columns.tolist()
                periode_col = next((c for c in all_cols if str(c).strip().upper() in ['PERIODE','PERIOD','TAHUN','YEAR']), None)
                if periode_col is None:
                    # Fallback: look for column that includes values like '2026','Target'
                    for c in all_cols:
                        if df_mon[c].astype(str).str.contains('2026|2025|Target', na=False).any():
                            periode_col = c
                            break
                
                keep_id_cols = [c for c in ['MERCHANT_GROUP','PM','DIMENSI', periode_col, 'FY','YTD'] if c and c in df_mon.columns]
                week_int_cols = [c for c in df_mon.columns if isinstance(c, int)]
                
                if week_int_cols and keep_id_cols:
                    df_weekly = df_mon[keep_id_cols + week_int_cols].copy()
                    # Rename integer week columns to W01, W02 etc
                    rename_map = {w: f"W{w:02d}" for w in week_int_cols}
                    df_weekly = df_weekly.rename(columns=rename_map)
                    if periode_col and periode_col != 'PERIODE':
                        df_weekly = df_weekly.rename(columns={periode_col: 'PERIODE'})
                    df_weekly.to_sql("raw_monitoring_weekly", conn, if_exists="replace", index=False)
                
                conn.close()

                st.success("✅ Analytics datasets successfully extracted to staging.db!")

            if os.path.exists(temp_excel_path):
                os.remove(temp_excel_path)

            # Rerun now that everything is saved — banner will appear at top
            st.rerun()

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
