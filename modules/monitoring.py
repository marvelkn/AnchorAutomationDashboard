import pandas as pd
import os
import shutil
import tempfile
import re
from datetime import datetime
import win32com.client as win32
import pythoncom
import sqlite3

def clean_val(v):
    if pd.isna(v) or v == "": return ""
    try: return float(str(v).replace(',', ''))
    except: return ""

def run_monitoring_merge(df_csv, db_path, path_mon, backup_dir):
    """
    Takes the natively queried weekly data and appends it to Master Excel using win32.
    Then, extracts all years (2024, 2025, 2026) and Targets to the dashboard staging DB.
    """
    # ── 1. Update 2026 Sheet with new data from Pipeline (df_csv) ───────────
    if len(df_csv) > 0:
        week_cols = [c for c in df_csv.columns if 'Week' in str(c)]
        max_week = 0
        for c in week_cols:
            match = re.search(r'Week\s+(\d+)', str(c))
            if match:
                 week_num = int(match.group(1))
                 if week_num > max_week:
                     max_week = week_num

        records = []
        for index, row in df_csv.iterrows():
            merchant = str(row['MERCHANT_GROUP']).replace('nan', '').upper().strip()
            if not merchant: continue
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

        lookup = {}
        for r in records:
            key = (r['MERCHANT_GROUP'], r['DIMENSI'].upper().strip())
            lookup[key] = r

        pythoncom.CoInitialize()
        temp_dir = tempfile.gettempdir()
        temp_excel_path = os.path.join(temp_dir, "temp_monitoring_master.xlsx")
        shutil.copy2(path_mon, temp_excel_path)
        excel_abs_path = os.path.abspath(temp_excel_path)
        excel = win32.Dispatch('Excel.Application')
        excel.Visible = False
        excel.DisplayAlerts = False
        try:
             wb = excel.Workbooks.Open(excel_abs_path)
             ws = wb.Sheets('2026')
             try:
                  ws_param = wb.Sheets('PARAMETER')
                  ws_param.Cells(2, 24).Value = max_week
             except: pass
             last_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row # xlUp
             for row_idx in range(2, last_row + 1):
                 cell_merchant = ws.Cells(row_idx, 1).Value
                 cell_dimensi = ws.Cells(row_idx, 3).Value
                 if not cell_merchant or not cell_dimensi: continue
                 excel_merch = str(cell_merchant).strip().upper()
                 excel_dim = str(cell_dimensi).strip().upper()
                 key = (excel_merch, excel_dim)
                 if key in lookup:
                     data_row = lookup[key]
                     week_data_array = [data_row.get(w, "") for w in range(1, max_week + 1)]
                     if week_data_array:
                         ws.Range(ws.Cells(row_idx, 9), ws.Cells(row_idx, 9 + max_week - 1)).Value = [week_data_array]
             wb.Save()
             wb.Close(SaveChanges=True)
        except Exception as e:
             try: wb.Close(SaveChanges=False)
             except: pass
             raise e
        finally:
             try: excel.Quit()
             except: pass
             pythoncom.CoUninitialize()
        
        if backup_dir and os.path.exists(path_mon):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(backup_dir, f"monitoring_backup_{timestamp}.xlsx")
            shutil.copy2(path_mon, backup_path)
        shutil.copy2(temp_excel_path, path_mon)
        if os.path.exists(temp_excel_path): os.remove(temp_excel_path)

    # ── 2. Unified Extraction of All Sheets ───────────
    xls = pd.ExcelFile(path_mon)
    
    # PM Mapping from PARAMETER
    pm_map = {}
    if 'PARAMETER' in xls.sheet_names:
        df_p = pd.read_excel(path_mon, sheet_name='PARAMETER', header=None)
        # Assuming PM in col 0, Merchant in col 3 (from user research)
        for _, r in df_p.iloc[1:].iterrows():
            pm_val = str(r.iloc[0]).strip()
            merch_val = str(r.iloc[3]).strip().upper()
            if pm_val and merch_val and pm_val != 'nan' and merch_val != 'nan':
                pm_map[merch_val] = pm_val

    all_weekly_dfs = []
    all_ytd_dfs    = []
    target_dfs     = []

    for sheet_name in ['2024', '2025', '2026', '2025 T', '2026 T']:
        if sheet_name not in xls.sheet_names: continue
        df_sheet = pd.read_excel(path_mon, sheet_name=sheet_name)
        # Standardize columns
        df_sheet.columns = [str(c).strip().upper() for c in df_sheet.columns]
        
        # Determine year
        yr = sheet_name.split()[0]
        is_target = 'T' in sheet_name
        
        # Identify merchant column
        m_col = 'MERCHANT_GROUP' if 'MERCHANT_GROUP' in df_sheet.columns else ('MERCHANT' if 'MERCHANT' in df_sheet.columns else None)
        if not m_col: continue

        df_sheet['MERCHANT_GROUP'] = df_sheet[m_col].astype(str).str.strip().str.upper()
        
        # Apply PM Mapping if not present or to ensure consistency
        if 'PM' not in df_sheet.columns:
            df_sheet['PM'] = df_sheet['MERCHANT_GROUP'].map(pm_map).fillna('UNASSIGNED')
        else:
            df_sheet['PM'] = df_sheet['PM'].fillna(df_sheet['MERCHANT_GROUP'].map(pm_map)).fillna('UNASSIGNED')

        if is_target:
            # For target, we just need the FY goals
            if 'FY' in df_sheet.columns:
                df_t = df_sheet[['MERCHANT_GROUP', 'DIMENSI', 'PM', 'FY']].copy()
                df_t['YEAR'] = yr
                target_dfs.append(df_t)
        else:
            # For Actuals, process weekly slots
            week_cols = [c for c in df_sheet.columns if str(c).isdigit()]
            keep_cols = ['MERCHANT_GROUP', 'DIMENSI', 'PM', 'FY', 'YTD']
            available_keep = [c for c in keep_cols if c in df_sheet.columns]
            
            df_w = df_sheet[available_keep + week_cols].copy()
            df_w['YEAR'] = yr
            
            # Melt for long-form if needed or just keep wide with renamed weeks
            rename_map = {w: f"W{int(w):02d}" for w in week_cols}
            df_w = df_w.rename(columns=rename_map)
            all_weekly_dfs.append(df_w)
            
            # Extract YTD summary for ML/KPIs
            if 'YTD' in df_sheet.columns and yr == '2026': # Only use latest for primary KPIs
                 df_y = df_sheet[df_sheet['DIMENSI']=='VOL'][['MERCHANT_GROUP', 'PM', 'YTD']].copy()
                 all_ytd_dfs.append(df_y)

    conn = sqlite3.connect(db_path)
    
    if all_weekly_dfs:
        df_full_weekly = pd.concat(all_weekly_dfs, ignore_index=True)
        df_full_weekly.to_sql("PROCESSED_MONITORING_WEEKLY", conn, if_exists="replace", index=False)
        
    if all_ytd_dfs:
        df_full_ytd = pd.concat(all_ytd_dfs, ignore_index=True)
        df_full_ytd.to_sql("PROCESSED_MONITORING", conn, if_exists="replace", index=False)
        
    if target_dfs:
        df_full_target = pd.concat(target_dfs, ignore_index=True)
        # Standardize for dashboard: MERCHANT_GROUP, PM, TARGET_VOL_2026, etc.
        # Pivot by YEAR if needed, or just keep long. 
        df_piv = df_full_target.pivot_table(index=['MERCHANT_GROUP', 'PM'], columns=['DIMENSI', 'YEAR'], values='FY').reset_index()
        # Flatten columns correctly
        new_cols = []
        for c in df_piv.columns:
            if c[1]:
                new_cols.append(f"{c[0]}_{c[1]}")
            else:
                new_cols.append(c[0])
        df_piv.columns = new_cols
        # Map to expected dashboard names
        rename_tgt = {
            'VOL_2026': 'TARGET_VOL_2026',
            'TRX_2026': 'TARGET_TRX_2026',
            'FBI_2026': 'TARGET_FBI_2026'
        }
        df_piv = df_piv.rename(columns=rename_tgt)
        df_piv.to_sql("TARGET", conn, if_exists="replace", index=False)

    conn.close()

    if os.path.exists(temp_excel_path):
        os.remove(temp_excel_path)
