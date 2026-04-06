import pandas as pd
import os
import shutil
import tempfile
from datetime import datetime
import win32com.client as win32
import pythoncom
import sqlite3

def run_card_share_merge(df_csv, db_path, path_card, backup_dir):
    """
    Takes the natively queried df_csv for Card Share and appends it to master Excel using win32.
    Then, it updates the database tables for the dashboard.
    """
    if len(df_csv) == 0:
        return

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
    
    pythoncom.CoInitialize()
    temp_dir = tempfile.gettempdir()
    temp_excel_path = os.path.join(temp_dir, "temp_card_share_master.xlsx")
    shutil.copy2(path_card, temp_excel_path)
    excel_abs_path = os.path.abspath(temp_excel_path)
    
    excel = win32.Dispatch('Excel.Application')
    excel.Visible = False
    excel.DisplayAlerts = False
    
    try:
        wb = excel.Workbooks.Open(excel_abs_path)
        ws = wb.Sheets('Realisasi')
        last_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row  # xlUp
        start_row = last_row + 1
        num_rows  = len(records)
        end_row   = start_row + num_rows - 1

        # ── Guard: Excel hard limit is 1,048,576 rows ──────────────────────
        MAX_XL_ROWS = 1_048_576
        if end_row > MAX_XL_ROWS:
            raise ValueError(
                f"Cannot write {num_rows:,} new rows starting at row {start_row:,}: "
                f"end row {end_row:,} exceeds Excel's limit of {MAX_XL_ROWS:,}. "
                "The SQL query returned too many rows — check filters / date range."
            )

        # ── Build data blocks ───────────────────────────────────────────────
        data_block1 = []
        data_block2 = []
        data_col4   = []  # YEAR  — computed in Python, no FormulaR1C1 needed
        data_col5   = []  # KEY   — computed in Python, no cross-sheet formula

        # Read Portfolio switch once (R1C13 = row 1, col 13 = M1)
        # Drives which key format is used (GROUP+MONTH vs BRAND+MONTH)
        portfolio_switch = 1  # default
        try:
            ws_portfolio = wb.Sheets('Portfolio')
            val = ws_portfolio.Cells(1, 13).Value
            if val is not None:
                portfolio_switch = int(val)
        except Exception:
            pass  # Portfolio sheet absent or unreadable — use default

        for record in records:
            group     = str(record.get('MERCHANT_GROUP', '') or '')
            brand     = str(record.get('MERCHANT_BRAND', '') or record.get('MERCHANT_ANCHOR', '') or '')
            trx_month = str(record.get('TRANSACTION_MONTH', '') or record.get('TRX_MONTH', '') or '')

            # Col 4: YEAR — first 4 chars of TRX_MONTH (e.g. '202601' → '2026')
            year_val  = trx_month[:4] if len(trx_month) >= 4 else ''

            # Col 5: KEY — mirrors the Portfolio-driven Excel formula logic
            key_val   = (group + trx_month) if portfolio_switch == 1 else (brand + trx_month)

            data_block1.append([group, brand, trx_month])
            data_col4.append([year_val])
            data_col5.append([key_val])
            data_block2.append([
                record.get('TRX_DEBIT_ONUS', 0),  record.get('TRX_DEBIT_OFFUS', 0),
                record.get('TRX_CREDIT_OFFUS', 0), record.get('TRX_QRIS_ONUS', 0),
                record.get('TRX_QRIS_OFFUS', 0),   record.get('VOL_DEBIT_ONUS', 0),
                record.get('VOL_DEBIT_OFFUS', 0),  record.get('VOL_CREDIT_OFFUS', 0),
                record.get('VOL_QRIS_ONUS', 0),    record.get('VOL_QRIS_OFFUS', 0),
                record.get('FBI_DEBIT_ONUS', 0),   record.get('FBI_DEBIT_OFFUS', 0),
                record.get('FBI_CREDIT_OFFUS', 0), record.get('FBI_QRIS_ONUS', 0),
                record.get('FBI_QRIS_OFFUS', 0)
            ])

        # ── Write to Excel ──────────────────────────────────────────────────
        # Cols 1-3: MERCHANT_GROUP, MERCHANT_ANCHOR/BRAND, TRX_MONTH
        ws.Range(ws.Cells(start_row, 1), ws.Cells(end_row, 3)).Value = data_block1
        # Col 4: YEAR (static value — no formula dependency)
        ws.Range(ws.Cells(start_row, 4), ws.Cells(end_row, 4)).Value = data_col4
        # Col 5: KEY (static value — no cross-sheet formula dependency)
        ws.Range(ws.Cells(start_row, 5), ws.Cells(end_row, 5)).Value = data_col5
        # Cols 6-20: all numeric metric columns
        ws.Range(ws.Cells(start_row, 6), ws.Cells(end_row, 20)).Value = data_block2

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
         
    # BACKUP old master BEFORE overwriting
    if backup_dir and os.path.exists(path_card):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"card_share_backup_{timestamp}.xlsx")
        shutil.copy2(path_card, backup_path)
    
    # OVERWRITE Master
    shutil.copy2(temp_excel_path, path_card)

    # Re-extract for Dashboard
    df_card = pd.read_excel(temp_excel_path, sheet_name="Realisasi")
    df_card['MERCHANT_GROUP']  = df_card['MERCHANT_GROUP'].astype(str).str.strip().str.upper()
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
    
    conn = sqlite3.connect(db_path)
    df_card_agg.to_sql("raw_card_share", conn, if_exists="replace", index=False)
    
    df_hist = df_card.groupby(['MERCHANT_GROUP', trx_month_actual, 'YEAR']).agg(
        TOTAL_SV=('TOTAL_SV','sum'), TOTAL_TRX=('TOTAL_TRX','sum'), TOTAL_FBI=('TOTAL_FBI','sum')
    ).reset_index()
    df_hist = df_hist.rename(columns={trx_month_actual: 'TRX_MONTH'})
    df_hist.to_sql("raw_card_history", conn, if_exists="replace", index=False)
    
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
    
    if os.path.exists(temp_excel_path):
        os.remove(temp_excel_path)
