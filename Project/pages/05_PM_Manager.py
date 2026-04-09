import streamlit as st
import sqlite3
import pandas as pd
import openpyxl
import os
import sys

st.set_page_config(page_title="PM Manager", page_icon="👥", layout="wide")

# Use absolute paths resolved from this file's location so the page works
# both locally and on Streamlit Cloud (CWD is unpredictable in cloud runtimes).
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

DB_PATH    = os.path.join(_BASE, "database", "staging.db")
EXCEL_PATH = os.path.join(_BASE, "data", "master", "master_monitoring.xlsx")

st.title("👥 Project Manager (PM) Assignment Manager")
st.markdown("Manage PMs and their mapped Merchant Groups. Changes are saved directly to your database and synchronized with `master_monitoring.xlsx`.")

neon_url = os.getenv("DATABASE_URL")
neon_exists = neon_url is not None

# Guard: if DB is missing, show a clear message instead of crashing.
if not neon_exists and not os.path.exists(DB_PATH):
    st.error(
        "⚠️ **Database not found.**  "
        "Please upload `staging.db` via the **Automated Pipeline** page first."
    )
    st.stop()

def get_cloud_engine():
    from utils.cloud_db import build_engine
    return build_engine()

def fetch_target_data():
    if neon_exists:
        engine = get_cloud_engine()
        df = pd.read_sql_query("SELECT merchant_group, pm FROM target ORDER BY pm", engine)
        if len(df.columns) > 0:
            df.columns = [c.upper() for c in df.columns]
        return df
    else:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT MERCHANT_GROUP, PM FROM TARGET ORDER BY PM", conn)
        conn.close()
        return df

def fetch_all_pms():
    if neon_exists:
        from sqlalchemy import text
        engine = get_cloud_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT pm FROM target WHERE pm IS NOT NULL ORDER BY pm"))
            return [row[0] for row in result]
    else:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT PM FROM TARGET WHERE PM IS NOT NULL ORDER BY PM")
        pms = [row[0] for row in cursor.fetchall()]
        conn.close()
        return pms

def update_excel_assignment(merchant_group, new_pm, is_new=False, delete=False):
    """Update master_monitoring.xlsx directly using openpyxl to safely preserve formulas."""
    if not os.path.exists(EXCEL_PATH):
        st.warning(f"Excel file not found at {EXCEL_PATH}. Skipping update.")
        return
        
    try:
        wb = openpyxl.load_workbook(EXCEL_PATH)
        if "PARAMETER" not in wb.sheetnames:
            st.warning("PARAMETER sheet not found in Excel file.")
            return
            
        sheet = wb["PARAMETER"]
        
        # Find exact row
        target_row = None
        max_row = 1
        
        for row_idx in range(2, sheet.max_row + 1):
            merchant_val = sheet.cell(row=row_idx, column=4).value # Column D
            # Track the max row with actual data
            if sheet.cell(row=row_idx, column=1).value is not None:
                max_row = row_idx
                
            if str(merchant_val).upper() == str(merchant_group).upper():
                target_row = row_idx
                break
                
        if delete and target_row:
            # Just clear the row or mark it unassigned in the PM column
            sheet.cell(row=target_row, column=1).value = "UNASSIGNED"
            wb.save(EXCEL_PATH)
            return

        if target_row:
            # Update existing
            sheet.cell(row=target_row, column=1).value = new_pm
        elif is_new:
            # Insert new row at the bottom
            new_row_idx = max_row + 1
            sheet.cell(row=new_row_idx, column=1).value = new_pm
            
            # Replicate formulas for NO and KEY
            # NO formula: =IF(A{row}=A{row-1},B{row-1}+1,1)
            # KEY formula: =CONCATENATE(A{row},B{row})
            no_formula = f'=IF(A{new_row_idx}=A{new_row_idx-1},B{new_row_idx-1}+1,1)'
            key_formula = f'=CONCATENATE(A{new_row_idx},B{new_row_idx})'
            
            sheet.cell(row=new_row_idx, column=2).value = no_formula
            sheet.cell(row=new_row_idx, column=3).value = key_formula
            sheet.cell(row=new_row_idx, column=4).value = merchant_group

        wb.save(EXCEL_PATH)
    except Exception as e:
        st.error(f"Error updating Excel mapping: {e}")

# Load Initial Data
if "editor_key" not in st.session_state:
    st.session_state.editor_key = 0

try:
    current_data = fetch_target_data()
    all_pms = fetch_all_pms()
except Exception as e:
    st.error(f"Failed to fetch data: {e}")
    st.stop()

if "UNASSIGNED" not in all_pms:
    all_pms.append("UNASSIGNED")

# ==========================
# 1. READ & UPDATE (Data Editor)
# ==========================
st.subheader("📋 Current Assignments (Editable)")
st.info("Double click a PM's name in the table to reassign the Merchant Group. Changes are auto-saved.")

edited_df = st.data_editor(
    current_data.copy(),
    column_config={
        "MERCHANT_GROUP": st.column_config.TextColumn("Merchant Group / Brand", disabled=True),
        "PM": st.column_config.SelectboxColumn("Project Manager", options=all_pms, required=True),
    },
    hide_index=True,
    use_container_width=True,
    key=f"data_editor_{st.session_state.editor_key}"
)

# Detect Changes
if not edited_df.equals(current_data):
    # Find diffs
    diff_mask = edited_df["PM"] != current_data["PM"]
    changed_rows = edited_df[diff_mask]
    
    if len(changed_rows) > 0:
        if neon_exists:
            from sqlalchemy import text
            engine = get_cloud_engine()
            with engine.begin() as conn:
                for idx, row in changed_rows.iterrows():
                    new_pm = row["PM"]
                    merchant_group = row["MERCHANT_GROUP"]
                    
                    conn.execute(
                        text("UPDATE target SET pm = :pm WHERE merchant_group = :mg"), 
                        {"pm": new_pm, "mg": merchant_group}
                    )
                    update_excel_assignment(merchant_group, new_pm)
        else:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            for idx, row in changed_rows.iterrows():
                new_pm = row["PM"]
                merchant_group = row["MERCHANT_GROUP"]
                
                cursor.execute("UPDATE TARGET SET PM = ? WHERE MERCHANT_GROUP = ?", (new_pm, merchant_group))
                update_excel_assignment(merchant_group, new_pm)
            conn.commit()
            conn.close()
            
        st.success(f"Successfully updated {len(changed_rows)} assignments!")
        st.session_state.editor_key += 1
        st.rerun()

st.divider()

# ==========================
# 2. CREATE (New Mapping)
# ==========================
col1, col2 = st.columns(2)
with col1:
    st.subheader("➕ Add New Assignment")
    with st.form("add_assignment_form", clear_on_submit=True):
        new_merchant = st.text_input("New Merchant Group Name").strip().upper()
        # Allows typed input in case PM doesn't exist yet
        new_pm_name = st.text_input("PM Name (Will create new if it doesn't exist)").strip().upper()
        
        submit_new = st.form_submit_button("Submit New Mapping")
        
        if submit_new:
            if not new_merchant or not new_pm_name:
                st.warning("Both Merchant Group and PM Name are required.")
            elif new_merchant in current_data["MERCHANT_GROUP"].values:
                st.error("This Merchant Group already exists! Please use the table above to change its PM.")
            else:
                try:
                    if neon_exists:
                        from sqlalchemy import text
                        engine = get_cloud_engine()
                        with engine.begin() as conn:
                            conn.execute(
                                text("INSERT INTO target (merchant_group, pm) VALUES (:mg, :pm)"), 
                                {"mg": new_merchant, "pm": new_pm_name}
                            )
                    else:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute("INSERT INTO TARGET (MERCHANT_GROUP, PM) VALUES (?, ?)", (new_merchant, new_pm_name))
                        conn.commit()
                        conn.close()
                    
                    update_excel_assignment(new_merchant, new_pm_name, is_new=True)
                    st.success(f"Added {new_merchant} > {new_pm_name} successfully!")
                    st.session_state.editor_key += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding mapping: {e}")

# ==========================
# 3. DELETE (Remove PM)
# ==========================
with col2:
    st.subheader("🗑️ Remove a Project Manager")
    with st.form("delete_pm_form"):
        pm_to_delete = st.selectbox("Select PM to Remove", [pm for pm in all_pms if pm != "UNASSIGNED"])
        reassign_to = st.selectbox("Reassign their Merchants To:", all_pms)
        
        confirm_deletion = st.form_submit_button("Proceed with Deletion")
        
        if confirm_deletion:
            if pm_to_delete == reassign_to:
                st.error("Cannot reassign to the same PM being deleted!")
            else:
                try:
                    if neon_exists:
                        from sqlalchemy import text
                        engine = get_cloud_engine()
                        with engine.begin() as conn:
                            result = conn.execute(
                                text("SELECT merchant_group FROM target WHERE pm = :pm"), 
                                {"pm": pm_to_delete}
                            )
                            affected_merchants = [row[0] for row in result]
                            
                            conn.execute(
                                text("UPDATE target SET pm = :newpm WHERE pm = :oldpm"), 
                                {"newpm": reassign_to, "oldpm": pm_to_delete}
                            )
                    else:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        
                        cursor.execute("SELECT MERCHANT_GROUP FROM TARGET WHERE PM = ?", (pm_to_delete,))
                        affected_merchants = [row[0] for row in cursor.fetchall()]
                        
                        cursor.execute("UPDATE TARGET SET PM = ? WHERE PM = ?", (reassign_to, pm_to_delete))
                        conn.commit()
                        conn.close()
                    
                    # Update Excel
                    for merch in affected_merchants:
                        update_excel_assignment(merch, reassign_to)
                        
                    st.success(f"Removed '{pm_to_delete}' and reassigned {len(affected_merchants)} merchants to '{reassign_to}'.")
                    st.session_state.editor_key += 1
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error deleting PM: {e}")
