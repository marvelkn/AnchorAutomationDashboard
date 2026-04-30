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

st.set_page_config(page_title="Data Editor — BTN Anchor", page_icon=os.path.join(_BASE, "static", "btn_logo.png"), layout="wide")
apply_theme()
page_header("", "Master Records Editor", "Safely View, Edit, Add, or Delete Master Classifications & Data")

st.markdown(
    """<div class="tab-desc">
    Select a dataset with the selector below, then use the interactive spreadsheet to edit records.
    Double-click cells to edit, use the row controls to add/delete, then click <b>Commit Changes</b> when done.
    </div>""",
    unsafe_allow_html=True,
)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.path.join(BASE_DIR, "database", "staging.db")
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

PATH_MID   = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD  = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON   = os.path.join(MASTER_DIR, "master_monitoring.xlsx")

# ── Dataset Selection — horizontal radio above tabs ───────────────────────────
section_label("Select Dataset")
dataset_choice = st.radio(
    "Choose a Master Data File to edit:",
    ["ALL_MID (Anchor Classifier)", "Card Share Analytics Matrix", "Monitoring Weekly Pivots"],
    horizontal=True,
    index=0,
    label_visibility="collapsed",
)

# Configuration mapping
config_map = {
    "ALL_MID (Anchor Classifier)": {
        "path": PATH_MID,
        "sheet": 0,
        "editable_columns": ["MERCHANT_ID", "MERCHANT_NAME", "SEGMENT", "MERCHANT_BRAND", "MERCHANT_GROUP"],
        "backup_dir": "backups_editor_mid",
        "sync_to_db": True,
        "table_name": "PROCESSED_MID"
    },
    "Card Share Analytics Matrix": {
        "path": PATH_CARD,
        "sheet": "Realisasi",
        "editable_columns": ["MERCHANT_GROUP", "MERCHANT_BRAND", "TRANSACTION_MONTH", "TRX_MONTH", "YEAR"],
        "backup_dir": "backups_editor_card",
        "sync_to_db": False
    },
    "Monitoring Weekly Pivots": {
        "path": PATH_MON,
        "sheet": "2026",
        "editable_columns": ["MERCHANT_GROUP", "DIMENSI", "PM", "PERIODE", "TAHUN", "YEAR"],
        "backup_dir": "backups_editor_mon",
        "sync_to_db": False
    }
}

conf = config_map[dataset_choice]

if not os.path.exists(conf["path"]):
    st.error(f"Master file not found at: {conf['path']}")
    st.stop()

# ── Cached data loader ────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Master File into memory...")
def load_data(path, sheet):
    try:
        return pd.read_excel(path, sheet_name=sheet)
    except ValueError:
        return pd.read_excel(path, sheet_name=0)

df_master = load_data(conf["path"], conf["sheet"]).copy()

# ── 2-TAB LAYOUT ─────────────────────────────────────────────────────────────
tab_edit, tab_bulk = st.tabs(["Edit Records", "Bulk Operations"])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — EDIT RECORDS
# ─────────────────────────────────────────────────────────────────────────────
with tab_edit:
    if dataset_choice == "ALL_MID (Anchor Classifier)":
        st.info("**Pro-Tip**: If you change a Segment to `RETAIL`, saving will auto-assign `MERCHANT RETAIL` for Brand & Group if left empty.")

    st.caption("Click any column header to search/filter  ·  Click checkbox header to Select All  ·  Press 'Delete' to remove selected rows")

    # Build column configs — lock non-editable columns
    col_configs = {}
    for col in df_master.columns:
        col_str = str(col).strip().upper()
        is_editable = any(col_str == allowed for allowed in conf["editable_columns"])
        if not is_editable:
            col_configs[col] = st.column_config.Column(
                disabled=True,
                help="Formula/financial column — read only."
            )

    # ── Editor + right-panel metrics ─────────────────────────────────────────
    editor_col, metrics_col = st.columns([3, 1])

    with editor_col:
        edited_df = st.data_editor(
            df_master,
            column_config=col_configs,
            num_rows="dynamic",
            width="stretch",
            height=min(400, max(200, len(df_master) * 35 + 60)),
            key=f"editor_{dataset_choice}",
        )

    # Compute diff metrics
    _orig_len = len(df_master)
    _edit_len = len(edited_df)
    _n_new    = max(0, _edit_len - _orig_len)

    # Modified rows: compare only rows that exist in both
    # Use fillna + astype(str) for NaN-safe comparison (NaN != NaN is True in plain pandas !=)
    _overlap = min(_orig_len, _edit_len)
    try:
        _a = edited_df.iloc[:_overlap].reset_index(drop=True).fillna("__NaN__").astype(str)
        _b = df_master.iloc[:_overlap].reset_index(drop=True).fillna("__NaN__").astype(str)
        _n_modified = int((_a != _b).any(axis=1).sum())
    except Exception:
        _n_modified = 0

    _has_changes = _n_new > 0 or len(edited_df) != len(df_master) or _n_modified > 0

    _mod_color = "amber" if _n_modified else "blue"
    _mod_meta  = "rows changed" if _n_modified else "no changes"
    with metrics_col:
        st.markdown(f"""
            <div class="stat-card amber" style="margin-bottom:8px;">
                <div class="stat-label">Total Rows</div>
                <div class="stat-value">{_edit_len:,}</div>
            </div>
            <div class="stat-card {_mod_color}" style="margin-bottom:8px;">
                <div class="stat-label">Modified</div>
                <div class="stat-value">{_n_modified}</div>
                <div class="stat-meta">{_mod_meta}</div>
            </div>
            <div class="stat-card green" style="margin-bottom:8px;">
                <div class="stat-label">New Rows</div>
                <div class="stat-value">{_n_new}</div>
                <div class="stat-meta">rows added</div>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        commit_clicked = st.button(
            "Commit Changes",
            type="primary",
            width="stretch",
            disabled=not _has_changes,
            key="btn_commit",
        )
        discard_clicked = st.button(
            "Discard",
            width="stretch",
            disabled=not _has_changes,
            key="btn_discard",
        )

    if discard_clicked:
        load_data.clear()
        st.rerun()

    if not _has_changes:
        st.info("No modifications detected yet. Start editing in the table to propose updates.")
    else:
        st.warning("**Unsaved Changes Detected!** Review your data grid before committing.")

    # ── Commit logic ──────────────────────────────────────────────────────────
    if commit_clicked and _has_changes:
        with st.status("Committing updates to local storage...", expanded=True) as status_save:
            try:
                st.write("Creating timestamped fallback backup...")
                bkp_dir_path = os.path.join(MASTER_DIR, conf["backup_dir"])
                os.makedirs(bkp_dir_path, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.basename(conf["path"]).replace('.xlsx', f'_backup_{timestamp}.xlsx')
                shutil.copy2(conf["path"], os.path.join(bkp_dir_path, filename))

                if dataset_choice == "ALL_MID (Anchor Classifier)":
                    st.write("Running Auto-Correction Retail Rules...")
                    empty_mask = (
                        (edited_df['SEGMENT'].astype(str).str.upper() == 'RETAIL') &
                        ((edited_df.get('MERCHANT_BRAND', pd.Series()).isna()) |
                         (edited_df.get('MERCHANT_BRAND', pd.Series()).astype(str) == '') |
                         (edited_df.get('MERCHANT_BRAND', pd.Series()).astype(str).str.lower() == 'nan')) &
                        ((edited_df.get('MERCHANT_GROUP', pd.Series()).isna()) |
                         (edited_df.get('MERCHANT_GROUP', pd.Series()).astype(str) == '') |
                         (edited_df.get('MERCHANT_GROUP', pd.Series()).astype(str).str.lower() == 'nan'))
                    )
                    if 'MERCHANT_BRAND' in edited_df.columns and 'MERCHANT_GROUP' in edited_df.columns:
                        edited_df.loc[empty_mask, 'MERCHANT_BRAND'] = 'MERCHANT RETAIL'
                        edited_df.loc[empty_mask, 'MERCHANT_GROUP'] = 'MERCHANT RETAIL'

                st.write("Injecting changes to Master Excel...")
                load_data.clear()

                if dataset_choice in ["Card Share Analytics Matrix", "Monitoring Weekly Pivots"]:
                    st.write("Surgically overwriting target sheet via Pandas...")
                    try:
                        with pd.ExcelWriter(conf["path"], engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                            edited_df.to_excel(writer, sheet_name=conf["sheet"] if isinstance(conf["sheet"], str) else "Master", index=False)
                    except Exception as append_err:
                        st.write(f"Fallback due to multi-sheet conflict. ({append_err})")
                        edited_df.to_excel(conf["path"], sheet_name=conf["sheet"] if isinstance(conf["sheet"], str) else "Sheet1", index=False)
                else:
                    edited_df.to_excel(conf["path"], index=False)

                if conf["sync_to_db"] and conf.get("table_name"):
                    st.write(f"Synchronizing edits to `staging.db` → `{conf['table_name']}`...")
                    conn = sqlite3.connect(DB_PATH)
                    sync_df = edited_df.copy()
                    sync_df.columns = [str(c).strip().upper() for c in sync_df.columns]
                    sync_df.to_sql(conf["table_name"], conn, if_exists='replace', index=False)
                    conn.close()

                status_save.update(label="Commit Successful! Master updated safely.", state="complete")

            except Exception as e:
                status_save.update(label=f"Save failed: {str(e)}", state="error")

        st.success("Changes permanently saved. The grid will reset on next load.")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — BULK OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
with tab_bulk:
    st.markdown(
        """<div class="tab-desc">
        Apply a mass change to hundreds of records instantly.
        Search for rows matching a keyword, then override a column value across all matches.
        Changes are staged — you still need to <b>Commit</b> in the Edit Records tab.
        </div>""",
        unsafe_allow_html=True,
    )

    with st.form("bulk_ops_form", clear_on_submit=False):
        bc1, bc2, bc3 = st.columns([2, 2, 1])
        with bc1:
            search_col = st.selectbox("Search Column", conf["editable_columns"])
            search_kw  = st.text_input("Find value (keyword):", help="Case-insensitive substring match")
        with bc2:
            target_col = st.selectbox("Column to Override", conf["editable_columns"])
            new_val    = st.text_input("Replace with:")
        with bc3:
            st.markdown("<div style='height:1.85rem'></div>", unsafe_allow_html=True)
            apply_bulk = st.form_submit_button("Apply", type="primary", width="stretch")

    if apply_bulk:
        if not search_kw:
            st.warning("Enter a keyword to search.")
        else:
            try:
                mask = df_master[search_col].astype(str).str.contains(
                    search_kw, case=False, na=False, regex=False
                )
                matched_count = int(mask.sum())
                if matched_count == 0:
                    st.warning(f"No rows matched `{search_kw}` in column `{search_col}`.")
                else:
                    df_master.loc[mask, target_col] = new_val
                    load_data.clear()
                    st.success(f"Staged bulk update on {matched_count} rows. Switch to **Edit Records** tab to review and commit.")
                    st.rerun()
            except Exception as e:
                st.error(f"Bulk operation failed: {e}")
