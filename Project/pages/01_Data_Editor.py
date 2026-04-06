import streamlit as st
import pandas as pd
import sqlite3
import os
import sys
import shutil
from datetime import datetime

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

from utils.theme import apply_theme, page_header, section_label, GOLD

st.set_page_config(page_title="Data Editor — BTN Anchor", page_icon="✏️", layout="wide")
apply_theme()
page_header("✏️", "Master Records Editor", "Safely View, Edit, Add, or Delete Master Classifications & Data")

st.markdown(
    """<div class="tab-desc">
    Select a Master database below to activate the interactive <b>Streamlit Spreadsheet</b>. 
    You can double-click cells to edit texts, click the 'Add Row' button at the bottom to insert new data, or select rows and press your 'Delete' key to remove them. Make sure to click <b>Save & Overwrite</b> when done!
    </div>""",
    unsafe_allow_html=True,
)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.path.join(BASE_DIR, "database", "staging.db")
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

PATH_MID   = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD  = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON   = os.path.join(MASTER_DIR, "master_monitoring.xlsx")

# 1. Dataset Selection
section_label("1. Select Dataset")
dataset_choice = st.selectbox(
    "Choose a Master Data File to edit:",
    ["ALL_MID (Anchor Classifier)", "Card Share Analytics Matrix", "Monitoring Weekly Pivots"],
    index=0
)

# Configuration mapping
config_map = {
    "ALL_MID (Anchor Classifier)": {
        "path": PATH_MID,
        "sheet": 0,
        "editable_columns": ["MERCHANT_ID", "MERCHANT_NAME", "SEGMEN", "MERCHANT_BRAND", "MERCHANT_GROUP"],
        "backup_dir": "backups_editor_mid",
        "sync_to_db": True,
        "table_name": "master_mid"
    },
    "Card Share Analytics Matrix": {
        "path": PATH_CARD,
        "sheet": "Realisasi", # Default known sheet
        "editable_columns": ["MERCHANT_GROUP", "MERCHANT_BRAND", "TRANSACTION_MONTH", "TRX_MONTH", "YEAR"],
        "backup_dir": "backups_editor_card",
        "sync_to_db": False
    },
    "Monitoring Weekly Pivots": {
        "path": PATH_MON,
        "sheet": "2026", # Default known sheet
        "editable_columns": ["MERCHANT_GROUP", "DIMENSI", "PM", "PERIODE", "TAHUN", "YEAR"],
        "backup_dir": "backups_editor_mon",
        "sync_to_db": False
    }
}

conf = config_map[dataset_choice]

# Check if file exists
if not os.path.exists(conf["path"]):
    st.error(f"❌ Master file not found at: {conf['path']}")
    st.stop()

# Cache data loading so the editor doesn't completely reset violently on every keystroke
@st.cache_data(show_spinner="Loading Master File into memory...")
def load_data(path, sheet):
    try:
        # Some files might not have the specifically named sheet, fallback to 0
        df = pd.read_excel(path, sheet_name=sheet)
        return df
    except ValueError:
        return pd.read_excel(path, sheet_name=0)

df_master = load_data(conf["path"], conf["sheet"]).copy()

st.markdown("<br>", unsafe_allow_html=True)
section_label("2. Interactive Editor")

# Dynamically build column configurations to LOCK financial/formula columns
col_configs = {}
for col in df_master.columns:
    # If the column name isn't precisely in our explicitly "allowed to edit" whitelist, LOCK IT!
    is_editable = False
    col_str = str(col).strip().upper()
    for allowed in conf["editable_columns"]:
        if col_str == allowed:
            is_editable = True
            break
            
    if not is_editable:
        col_configs[col] = st.column_config.Column(
            disabled=True, 
            help="🔒 This column contains strictly formatted keys, financial vectors, or Excel formulas and cannot be manually edited."
        )

# Extra features specifically for ALL_MID auto-correction.
if dataset_choice == "ALL_MID (Anchor Classifier)":
    st.info("💡 **Pro-Tip for ALL_MID**: If you change a generic Segmen to `RETAIL`, you must click Save. Streamlit will automatically assign it to `MERCHANT RETAIL` for Brand & Group if left empty.")

st.markdown("""<div style="font-size: 0.9em; color:#64748B;">
<b>Grid Shortcuts:</b> 🔍 Click any column header to search/filter  |  🖱️ Click the checkbox column header to Select All  |  ❌ Press 'Delete' to remove rows
</div>""", unsafe_allow_html=True)

# ── BULK UPDATE UTILITY ──
with st.expander("⚡ **Bulk Operations Utility** (Search & Multi-Edit)", expanded=False):
    st.markdown("Use this tool to apply a massive change to hundreds of records instantly before saving.")
    
    col_filter, col_action = st.columns([1, 1], gap="large")
    with col_filter:
        search_col = st.selectbox("Search Column", conf["editable_columns"])
        search_kw = st.text_input(f"🔍 Type keyword to filter {search_col}:", help="Press Enter to apply")
        
        # Filter logic
        if search_kw:
            mask = df_master[search_col].astype(str).str.contains(search_kw, case=False, na=False)
            filtered_indices = df_master[mask].index.tolist()
            matched_count = len(filtered_indices)
            st.success(f"Found {matched_count} matching rows.")
        else:
            filtered_indices = []
            st.caption("Enter a keyword to unlock Bulk Select.")

    with col_action:
        if search_kw and len(filtered_indices) > 0:
            target_col = st.selectbox("Column to Override", conf["editable_columns"])
            new_val = st.text_input(f"New Value for {target_col}:")
            
            if st.button("Apply Bulk Update", type="primary"):
                # Apply changes directly to the active cached dataframe
                df_master.loc[filtered_indices, target_col] = new_val
                st.success(f"✅ Successfully staged bulk update on {len(filtered_indices)} rows! Check the grid below.")
                st.rerun() # Refresh to push cached changes down to the editor

# The Editor Grid
edited_df = st.data_editor(
    df_master,
    column_config=col_configs,
    num_rows="dynamic",
    width='stretch',
    height=600,
    key=f"editor_{dataset_choice}"
)

st.markdown("<br>", unsafe_allow_html=True)
section_label("3. Commit Changes")

# Build difference logic to prevent needless saves
if df_master.equals(edited_df):
    st.info("No modifications detected yet. Start typing in the table to propose updates.")
else:
    st.warning("⚠️ **Unsaved Changes Detected!** Review your data grid before committing.")
    
    if st.button("💾 OVERWRITE MASTER FILE & COMMIT CHANGES", type="primary", width='stretch'):
        
        with st.status("Committing updates to local storage...", expanded=True) as status_save:
            try:
                # 1. Backups
                st.write("Creating timestamped fallback backup...")
                bkp_dir_path = os.path.join(MASTER_DIR, conf["backup_dir"])
                os.makedirs(bkp_dir_path, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.basename(conf["path"]).replace('.xlsx', f'_backup_{timestamp}.xlsx')
                shutil.copy2(conf["path"], os.path.join(bkp_dir_path, filename))
                
                # 2. Applying Auto-Corrections for ALL_MID before final save
                if dataset_choice == "ALL_MID (Anchor Classifier)":
                    st.write("Running Auto-Correction Retail Rules...")
                    
                    empty_mask = (edited_df['SEGMEN'].astype(str).str.upper() == 'RETAIL') & \
                                 ((edited_df.get('MERCHANT_BRAND', pd.Series()).isna()) | (edited_df.get('MERCHANT_BRAND', pd.Series()).astype(str) == '') | (edited_df.get('MERCHANT_BRAND', pd.Series()).astype(str).str.lower() == 'nan')) & \
                                 ((edited_df.get('MERCHANT_GROUP', pd.Series()).isna()) | (edited_df.get('MERCHANT_GROUP', pd.Series()).astype(str) == '') | (edited_df.get('MERCHANT_GROUP', pd.Series()).astype(str).str.lower() == 'nan'))
                                 
                    if 'MERCHANT_BRAND' in edited_df.columns and 'MERCHANT_GROUP' in edited_df.columns:
                        edited_df.loc[empty_mask, 'MERCHANT_BRAND'] = 'MERCHANT RETAIL'
                        edited_df.loc[empty_mask, 'MERCHANT_GROUP'] = 'MERCHANT RETAIL'
                
                # 3. Overwriting `.xlsx`
                st.write("Injecting changes to Master Excel...")
                
                # Update underlying loaded master cache with the accepted edits so UI diff turns green
                df_master = edited_df.copy()
                load_data.clear() 
                
                if dataset_choice in ["Card Share Analytics Matrix", "Monitoring Weekly Pivots"]:
                    st.write("Surgically overwriting target sheet via Pandas...")
                    try:
                        with pd.ExcelWriter(conf["path"], engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                            edited_df.to_excel(writer, sheet_name=conf["sheet"] if isinstance(conf["sheet"], str) else "Master", index=False)
                    except Exception as append_err:
                        st.write(f"Fallback due to multi-sheet conflict. Writing cleanly. ({append_err})")
                        edited_df.to_excel(conf["path"], sheet_name=conf["sheet"] if isinstance(conf["sheet"], str) else "Sheet1", index=False)
                else:
                    edited_df.to_excel(conf["path"], index=False)
                
                # 4. Sync to DB if needed
                if conf["sync_to_db"] and conf.get("table_name"):
                    st.write(f"Synchronizing edits to `staging.db` -> `{conf['table_name']}`...")
                    conn = sqlite3.connect(DB_PATH)
                    edited_df.columns = [str(c).strip().upper() for c in edited_df.columns]
                    edited_df.to_sql(conf["table_name"], conn, if_exists='replace', index=False)
                    conn.close()

                status_save.update(label="✅ Commit Successful! Master updated safely.", state="complete")
                
            except Exception as e:
                status_save.update(label=f"❌ Save operation failed! Reason: {str(e)}", state="error")
                
        st.success("✅ Changes have been permanently verified & deployed! Please refresh or click away to reset the grid state.")
