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
    Then, extracts it to the dashboard staging DB.
    """
    if len(df_csv) == 0:
        return

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
    
    df_target = pd.read_excel(temp_excel_path, sheet_name="Edit Target")
    df_target_clean = df_target[['MERCHANT GROUP', 'PM', 'VOL NEW', 'TRX NEW', 'FBI FIX']].copy()
    df_target_clean.columns = ['MERCHANT_GROUP', 'PM', 'TARGET_VOL_2026', 'TARGET_TRX_2026', 'TARGET_FBI_2026']
    df_target_clean = df_target_clean.dropna(subset=['MERCHANT_GROUP'])
    df_target_clean['MERCHANT_GROUP'] = df_target_clean['MERCHANT_GROUP'].astype(str).str.strip().str.upper()
    
    conn = sqlite3.connect(db_path)
    df_mon_ytd.to_sql("raw_monitoring",  conn, if_exists="replace", index=False)
    df_target_clean.to_sql("raw_target", conn, if_exists="replace", index=False)
    
    all_cols = df_mon.columns.tolist()
    periode_col = next((c for c in all_cols if str(c).strip().upper() in ['PERIODE','PERIOD','TAHUN','YEAR']), None)
    if periode_col is None:
        for c in all_cols:
            if df_mon[c].astype(str).str.contains('2026|2025|Target', na=False).any():
                periode_col = c
                break
    
    keep_id_cols = [c for c in ['MERCHANT_GROUP','PM','DIMENSI', periode_col, 'FY','YTD'] if c and c in df_mon.columns]
    week_int_cols = [c for c in df_mon.columns if isinstance(c, int)]
    
    if week_int_cols and keep_id_cols:
        df_weekly = df_mon[keep_id_cols + week_int_cols].copy()
        rename_map = {w: f"W{w:02d}" for w in week_int_cols}
        df_weekly = df_weekly.rename(columns=rename_map)
        if periode_col and periode_col != 'PERIODE':
            df_weekly = df_weekly.rename(columns={periode_col: 'PERIODE'})
        df_weekly.to_sql("raw_monitoring_weekly", conn, if_exists="replace", index=False)
    
    conn.close()

    if os.path.exists(temp_excel_path):
        os.remove(temp_excel_path)
