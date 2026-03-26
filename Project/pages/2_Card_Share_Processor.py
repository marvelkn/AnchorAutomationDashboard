import streamlit as st
import sqlite3
import pandas as pd
import os
import io
import shutil
from datetime import datetime

st.title("💳 Card Share Processor (Master + New Data)")
st.markdown("Upload your existing **Master Excel** and the **New Data** fetch. The system will safely merge them, update the Staging DB, and provide the fully intact Microsoft Excel file for download.")

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_DB    = os.path.join(BASE_DIR, "database", "staging.db")
PATH_CARD  = os.path.join(BASE_DIR, "data", "master", "master_card_share.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "data", "master", "backups_card")
os.makedirs(os.path.dirname(PATH_DB), exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

if not os.path.exists(PATH_CARD):
    st.error("❌ Master Card Share File not found! Please upload it via **⚙️ Global Settings** first.")
    st.stop()

# ── Current master status ──
col_info, col_dl = st.columns([3, 1])
with col_info:
    mtime = datetime.fromtimestamp(os.path.getmtime(PATH_CARD)).strftime("%d %b %Y %H:%M")
    msize = os.path.getsize(PATH_CARD) // 1024
    st.success(f"✅ Master loaded — last updated **{mtime}** · {msize} KB")
with col_dl:
    with open(PATH_CARD, "rb") as f:
        st.download_button("⬇️ Download Current Master", f,
                           file_name="master_card_share_current.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── Rollback panel ──
card_backups = sorted([b for b in os.listdir(BACKUP_DIR) if b.endswith(".xlsx")], reverse=True)
with st.expander(f"🔁 Rollback / Restore Previous Master ({len(card_backups)} backups available)"):
    if not card_backups:
        st.info("No backups yet. Backups are created automatically each time you process new data.")
    else:
        for bfile in card_backups[:10]:
            bpath = os.path.join(BACKUP_DIR, bfile)
            bsize = os.path.getsize(bpath) // 1024
            rb1, rb2, rb3 = st.columns([4, 1, 1])
            rb1.markdown(f"📄 `{bfile}` — {bsize} KB")
            with open(bpath, "rb") as bf:
                rb2.download_button("⬇️ Download", bf, file_name=bfile,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"cdl_{bfile}")
            if rb3.button("♻️ Restore", key=f"crestore_{bfile}"):
                shutil.copy2(bpath, PATH_CARD)
                st.success(f"✅ Restored from backup: `{bfile}`")
                st.session_state.pop('card_result', None)
                st.rerun()

# ── Show download result banner if processing just finished ──
if 'card_result' in st.session_state:
    res = st.session_state['card_result']
    st.markdown("---")
    st.success(f"🎉 Processing complete! Master updated at {res['timestamp']}. Download your files below:")
    rc1, rc2 = st.columns(2)
    rc1.download_button(
        label="📥 Download NEW Master (.xlsx)",
        data=res['excel_bytes'],
        file_name=res['excel_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary", key="card_dl_new"
    )
    rc2.download_button(
        label="🔙 Download BACKUP",
        data=res['backup_bytes'],
        file_name=res['backup_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="card_dl_bak"
    )
    if st.button("❌ Dismiss", key="card_dismiss"):
        del st.session_state['card_result']
        st.rerun()

st.markdown("---")
uploaded_csv = st.file_uploader("Upload NEW Card Share Data", type=['csv', 'xlsx', 'xls'])

if uploaded_csv is not None:
    if st.button("🚀 Process & Merge Datasets", type="primary"):
        
        try:
            with st.spinner("Parsing Data..."):
                # Handle CSV (comma or semicolon) or Excel
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
                    
                numeric_cols = [c for c in df_csv.columns if 'TRX_' in c or 'VOL_' in c or 'FBI_' in c]
                for c in numeric_cols:
                     if c in df_csv.columns:
                          df_csv[c] = pd.to_numeric(df_csv[c], errors='coerce').fillna(0)
                          
                str_cols = ['MERCHANT_GROUP', 'MERCHANT_BRAND', 'MERCHANT_ANCHOR', 'TRANSACTION_MONTH', 'TRX_MONTH']
                for c in str_cols:
                     if c in df_csv.columns:
                          df_csv[c] = df_csv[c].astype(str).replace('nan', '')
                          if c == 'TRANSACTION_MONTH' or c == 'TRX_MONTH':
                              df_csv[c] = df_csv[c].apply(lambda x: x.replace('.0', '') if str(x).endswith('.0') else x)

                records = df_csv.to_dict('records')
                st.info(f"Loaded {len(records)} new records from new data file.")

            if len(records) > 0:
                temp_excel_path = None
                with st.spinner("Appending to Master Excel Workbook (Preserving Formatting & Charts)..."):
                    import win32com.client as win32
                    import pythoncom
                    import tempfile
                    import shutil
                    
                    pythoncom.CoInitialize()
                    
                    # Instead of saving from uploader, clone the Server Master to temp
                    temp_dir = tempfile.gettempdir()
                    temp_excel_path = os.path.join(temp_dir, "temp_card_share_master.xlsx")
                    shutil.copy2(PATH_CARD, temp_excel_path)
                    
                    excel_abs_path = os.path.abspath(temp_excel_path)
                    
                    # Safe COM Execution
                    excel = win32.Dispatch('Excel.Application')
                    excel.Visible = False
                    excel.DisplayAlerts = False
                    
                    try:
                         wb = excel.Workbooks.Open(excel_abs_path)
                         sheet_name = 'Realisasi'
                         try:
                              ws = wb.Sheets(sheet_name)
                         except Exception as e:
                              st.error(f"Error: Sheet '{sheet_name}' not found.")
                              wb.Close(SaveChanges=False)
                              excel.Quit()
                              st.stop()
                         
                         last_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row # xlUp
                         start_row = last_row + 1
                         num_rows = len(records)
                         end_row = start_row + num_rows - 1
                         
                         data_block1 = []
                         data_block2 = []
                         for record in records:
                             group = record.get('MERCHANT_GROUP', '')
                             brand = record.get('MERCHANT_BRAND', '')
                             if not brand: brand = record.get('MERCHANT_ANCHOR', '')
                             
                             trx_month = record.get('TRANSACTION_MONTH', '')
                             if not trx_month: trx_month = record.get('TRX_MONTH', '')
                             
                             data_block1.append([group, brand, trx_month])
                             data_block2.append([
                                 record.get('TRX_DEBIT_ONUS', 0), record.get('TRX_DEBIT_OFFUS', 0), record.get('TRX_CREDIT_OFFUS', 0),
                                 record.get('TRX_QRIS_ONUS', 0), record.get('TRX_QRIS_OFFUS', 0),
                                 record.get('VOL_DEBIT_ONUS', 0), record.get('VOL_DEBIT_OFFUS', 0), record.get('VOL_CREDIT_OFFUS', 0),
                                 record.get('VOL_QRIS_ONUS', 0), record.get('VOL_QRIS_OFFUS', 0),
                                 record.get('FBI_DEBIT_ONUS', 0), record.get('FBI_DEBIT_OFFUS', 0), record.get('FBI_CREDIT_OFFUS', 0),
                                 record.get('FBI_QRIS_ONUS', 0), record.get('FBI_QRIS_OFFUS', 0)
                             ])
                             
                         ws.Range(ws.Cells(start_row, 1), ws.Cells(end_row, 3)).Value = data_block1
                         ws.Range(ws.Cells(start_row, 6), ws.Cells(end_row, 20)).Value = data_block2
                         
                         ws.Range(ws.Cells(start_row, 4), ws.Cells(end_row, 4)).FormulaR1C1 = "=MID(RC[-1],1,4)"
                         ws.Range(ws.Cells(start_row, 5), ws.Cells(end_row, 5)).FormulaR1C1 = "=IF(Portfolio!R1C13=1,CONCATENATE(RC[-4],RC[-2]),CONCATENATE(RC[-3],RC[-2]))"
                         
                         wb.Save()
                         wb.Close(SaveChanges=True)
                    except Exception as e:
                        st.error(f"Excel COM error: {e}")
                        try:
                            wb.Close(SaveChanges=False)
                        except:
                            pass
                        st.stop()
                    finally:
                        try:
                            excel.Quit()
                        except:
                            pass
                        pythoncom.CoUninitialize()
                         
                    # BACKUP old master BEFORE overwriting
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_name = f"card_share_backup_{timestamp}.xlsx"
                    backup_path = os.path.join(BACKUP_DIR, backup_name)
                    shutil.copy2(PATH_CARD, backup_path)
                    
                    # OVERWRITE Master safely with the newly combined temporary file
                    try:
                        shutil.copy2(temp_excel_path, PATH_CARD)
                    except Exception as e:
                        st.error(f"Failed to auto-update master file: {e}")
                    
                    with open(temp_excel_path, "rb") as f:
                        excel_bytes = f.read()
                    with open(backup_path, "rb") as bf:
                        backup_bytes = bf.read()
                    
                    st.session_state['card_result'] = {
                        'timestamp': timestamp,
                        'excel_bytes': excel_bytes,
                        'excel_name': f"card_share_master_{timestamp}.xlsx",
                        'backup_bytes': backup_bytes,
                        'backup_name': backup_name,
                    }
                    
                with st.spinner("Extracting strictly Realisasi data to Staging Database..."):
                    # Load from python using pandas directly from the physical file we created for db loading
                    df_card = pd.read_excel(temp_excel_path, sheet_name="Realisasi")
                    
                    df_card['MERCHANT_GROUP']  = df_card['MERCHANT_GROUP'].astype(str).str.strip().str.upper()
                    
                    # Ensure YEAR is derived properly if formula wasn't evaluated cleanly in pandas load
                    month_col = 'TRANSACTION_MONTH' if 'TRANSACTION_MONTH' in df_card.columns else 'TRX_MONTH'
                    df_card['YEAR'] = df_card[month_col].astype(str).str[:4]
                    df_card['YEAR'] = pd.to_numeric(df_card['YEAR'], errors='coerce')
                    
                    df_card['TRX_QRIS_OFFUS'] = df_card.get('TRX_QRIS_OFFUS', pd.Series(0, index=df_card.index)).fillna(0)
                    
                    df_card['TOTAL_SV']  = (df_card.get('SV_DEBIT_ONUS', pd.Series(0)).fillna(0)  + df_card.get('SV_DEBIT_OFFUS', pd.Series(0)).fillna(0) +
                                            df_card.get('SV_CREDIT_OFFUS', pd.Series(0)).fillna(0) + df_card.get('SV_QRIS_ONUS', pd.Series(0)).fillna(0) +
                                            df_card.get('SV_QRIS_OFFUS', pd.Series(0)).fillna(0))
                    
                    df_card['TOTAL_TRX'] = (df_card.get('TRX_DEBIT_ONUS', pd.Series(0)).fillna(0)  + df_card.get('TRX_DEBIT_OFFUS', pd.Series(0)).fillna(0) +
                                            df_card.get('TRX_CREDIT_OFFUS', pd.Series(0)).fillna(0) + df_card.get('TRX_QRIS_ONUS', pd.Series(0)).fillna(0) +
                                            df_card.get('TRX_QRIS_OFFUS', pd.Series(0)).fillna(0))
                    
                    df_card['TOTAL_FBI'] = (df_card.get('FBI_DEBIT_ONUS', pd.Series(0)).fillna(0)  + df_card.get('FBI_DEBIT_OFFUS', pd.Series(0)).fillna(0) +
                                            df_card.get('FBI_CREDIT_OFFUS', pd.Series(0)).fillna(0) + df_card.get('FBI_QRIS_ONUS', pd.Series(0)).fillna(0) +
                                            df_card.get('FBI_QRIS_OFFUS', pd.Series(0)).fillna(0))
                                            
                    df_card['RASIO_ONUS'] = df_card.get('SV_DEBIT_ONUS', pd.Series(0)) / df_card['TOTAL_SV'].replace(0, pd.NA)
                    df_card['RASIO_ONUS'] = df_card['RASIO_ONUS'].fillna(0)
                    
                    df_card_2026 = df_card[df_card['YEAR'] == 2026].copy()
                    
                    if len(df_card_2026) == 0:
                        st.warning("No data found for year 2026 in the merged document.")
                    
                    # Prevent unique/max errors if column name is missing
                    trx_month_actual = 'TRX_MONTH' if 'TRX_MONTH' in df_card.columns else month_col
                    
                    df_card_agg = df_card_2026.groupby('MERCHANT_GROUP').agg(
                        TOTAL_SV      = ('TOTAL_SV',  'sum'),
                        TOTAL_TRX     = ('TOTAL_TRX', 'sum'),
                        TOTAL_FBI     = ('TOTAL_FBI', 'sum'),
                        SV_ONUS       = ('SV_DEBIT_ONUS', 'sum') if 'SV_DEBIT_ONUS' in df_card.columns else ('TOTAL_SV', lambda x: 0),
                        RASIO_ONUS    = ('RASIO_ONUS', 'mean'),
                        N_BULAN       = (trx_month_actual, 'nunique'),
                        BULAN_TERAKHIR= (trx_month_actual, 'max')
                    ).reset_index()
                    
                    conn = sqlite3.connect(PATH_DB)
                    df_card_agg.to_sql("raw_card_share", conn, if_exists="replace", index=False)
                    
                    df_hist = df_card.groupby(['MERCHANT_GROUP', trx_month_actual, 'YEAR']).agg(
                        TOTAL_SV=('TOTAL_SV','sum'), TOTAL_TRX=('TOTAL_TRX','sum'), TOTAL_FBI=('TOTAL_FBI','sum')
                    ).reset_index()
                    df_hist = df_hist.rename(columns={trx_month_actual: 'TRX_MONTH'})
                    df_hist.to_sql("raw_card_history", conn, if_exists="replace", index=False)
                    
                    # Store detailed per-payment-type monthly breakdown for dashboard
                    detail_grp_cols = ['MERCHANT_GROUP', trx_month_actual, 'YEAR']
                    detail_agg = {}
                    for prefix, types in [('TRX', ['TRX_DEBIT_ONUS','TRX_DEBIT_OFFUS','TRX_CREDIT_OFFUS','TRX_QRIS_ONUS','TRX_QRIS_OFFUS']),
                                          ('VOL', ['SV_DEBIT_ONUS','SV_DEBIT_OFFUS','SV_CREDIT_OFFUS','SV_QRIS_ONUS','SV_QRIS_OFFUS']),
                                          ('FBI', ['FBI_DEBIT_ONUS','FBI_DEBIT_OFFUS','FBI_CREDIT_OFFUS','FBI_QRIS_ONUS','FBI_QRIS_OFFUS'])]:
                        for col in types:
                            if col in df_card.columns:
                                detail_agg[col] = (col, 'sum')
                    if detail_agg:
                        df_monthly_detail = df_card.groupby(detail_grp_cols).agg(
                            TOTAL_TRX=('TOTAL_TRX','sum'),
                            TOTAL_SV=('TOTAL_SV','sum'),
                            TOTAL_FBI=('TOTAL_FBI','sum'),
                            **detail_agg
                        ).reset_index().rename(columns={trx_month_actual: 'TRX_MONTH'})
                        df_monthly_detail.to_sql("raw_card_monthly", conn, if_exists="replace", index=False)
                    conn.close()
                    
                st.success("✅ Analytics datasets successfully extracted to staging.db!")
                
                # Cleanup temp file
                if os.path.exists(temp_excel_path):
                    os.remove(temp_excel_path)
                
                # Rerun now that everything is saved — banner will appear at top
                st.rerun()

        except Exception as e:
            st.error(f"Error processing records: {str(e)}")
