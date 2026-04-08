import streamlit as st
import os
import sys
import shutil
from datetime import datetime
import pandas as pd

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

from utils.theme import (
    apply_theme, page_header, section_label, pipeline_stepper,
    GOLD, SURFACE, BORDER, TEXT_SEC
)
from utils.db_connector import fetch_data_from_db, get_db_date_bounds
from modules.mid_cleaner import run_mid_cleaner
from modules.card_share import run_card_share_merge
from modules.monitoring import run_monitoring_merge
from utils.backup_manager import rotate_backups, get_available_backups, restore_backup

st.set_page_config(page_title="Automated Pipeline — BTN Anchor", page_icon="🚀", layout="wide")
apply_theme()
page_header("🚀", "Automated ETL Pipeline", "Upload Staging DB, execute Analytics, and update Master Files")

st.markdown(
    """<div class="tab-desc">
    This centralized automation module ingests your raw <b>staging.db</b>, executes the SQL models,
    dynamically classifies Anchors, and pushes the data to your Analytics Matrix automatically.
    </div>""",
    unsafe_allow_html=True,
)

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_UPLOAD_DIR = os.path.join(BASE_DIR, "database")
MASTER_DIR    = os.path.join(BASE_DIR, "data", "master")

PATH_LOCAL_DB = os.path.join(DB_UPLOAD_DIR, "staging.db")
PATH_MID      = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD     = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON      = os.path.join(MASTER_DIR, "master_monitoring.xlsx")
PATH_BACKUP_DIR = os.path.join(DB_UPLOAD_DIR, "backup")

# Create backup dir if missing
if not os.path.exists(PATH_BACKUP_DIR):
    os.makedirs(PATH_BACKUP_DIR)

# ── Pipeline Step Definitions ─────────────────────────────────────────────────
PIPELINE_STEPS = [
    ("📥", "Upload\nDatabase"),
    ("🛠️", "Process\nAnchor MIDs"),
    ("💳", "Card Share\nAnalytics"),
    ("📅", "Weekly\nMonitoring"),
    ("✅", "Complete"),
]

# current_step is driven by session state
pipe_step = st.session_state.get("pipeline_step", -1)
pipeline_stepper(PIPELINE_STEPS, pipe_step)

st.markdown("<br>", unsafe_allow_html=True)

# ── SECTION 1 & 2: Inputs ────────────────────────────────────────────────────
col1, col2 = st.columns([1, 1])

with col1:
    section_label("1. Database Source")
    st.info("Upload your latest `staging.db` containing the raw transaction tables.")
    
    # ── INGESTION STRATEGY ──
    ingest_strategy = st.radio(
        "Ingestion Strategy",
        ["Replace Full (Slower)", "Update Incremental (Faster)"],
        index=0, horizontal=True,
        help="Full: Overwrites DB. Incremental: Adds only new rows."
    )

    uploaded_db = st.file_uploader("Upload SQLite Database (.db)", type=['db', 'sqlite'])

    if uploaded_db and st.session_state.get("pipeline_step", -1) == -1:
        # 1. ROTATE BACKUPS BEFORE OVERWRITING/UPDATING
        if os.path.exists(PATH_LOCAL_DB):
            rotate_backups(PATH_LOCAL_DB, PATH_BACKUP_DIR)
            
        # 2. SAVE UPLOADED DB
        if ingest_strategy == "Replace Full (Slower)":
            with open(PATH_LOCAL_DB, "wb") as f:
                f.write(uploaded_db.getvalue())
            
            # Update metadata timestamp for full replace
            import sqlite3, datetime
            conn_m = sqlite3.connect(PATH_LOCAL_DB)
            cursor_m = conn_m.cursor()
            cursor_m.execute("CREATE TABLE IF NOT EXISTS APP_METADATA (key TEXT PRIMARY KEY, value TEXT)")
            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor_m.execute("INSERT OR REPLACE INTO APP_METADATA (key, value) VALUES ('LAST_DATA_UPDATE', ?)", (now_str,))
            cursor_m.execute("INSERT OR REPLACE INTO APP_METADATA (key, value) VALUES ('NEW_DATA_SIGNAL', '1')")
            conn_m.commit()
            conn_m.close()
            
            st.success(f"✅ `staging.db` replaced! Size: {len(uploaded_db.getvalue()) // (1024*1024)} MB")
        else:
            # Incremental ingestion logic will go here
            temp_path = os.path.join(DB_UPLOAD_DIR, "temp_upload.db")
            with open(temp_path, "wb") as f:
                f.write(uploaded_db.getvalue())
            
            # Placeholder for merge function
            st.info("🔄 Running Incremental Merge (Adding new rows only)...")
            from utils.db_merger import merge_incremental_data
            rows_added = merge_incremental_data(temp_path, PATH_LOCAL_DB)
            os.remove(temp_path)
            st.success(f"✅ Incremental update complete! Added {rows_added} new records.")

        # 3. ADVANCE STEPPER
        st.session_state["pipeline_step"] = 0
        st.rerun()
    elif os.path.exists(PATH_LOCAL_DB):
        sz = os.path.getsize(PATH_LOCAL_DB) // (1024 * 1024)
        st.success(f"✅ Existing `staging.db` found locally ({sz} MB).")
    
    else:
        st.warning("⚠️ No database found. Please upload `staging.db`.")
    
    # ── ROLLBACK SECTION ──
    st.markdown("<hr style='margin:1.5rem 0; opacity:0.3;'>", unsafe_allow_html=True)
    st.markdown("### 🔄 Rollback & Restore")
    backups = get_available_backups(PATH_BACKUP_DIR)
    
    if not backups:
        st.caption("No backups available yet.")
    else:
        for b in backups:
            col_b1, col_b2 = st.columns([3, 1])
            col_b1.write(f"**Version {b['version']}** ({b['timestamp']})")
            if col_b2.button(f"Restore", key=f"restore_{b['version']}"):
                if restore_backup(b['path'], PATH_LOCAL_DB):
                    st.success(f"✅ Restored Version {b['version']} successfully!")
                    st.rerun()
                else:
                    st.error("Failed to restore backup.")

    # ── RESET SECTION (New) ──
    st.markdown("<hr style='margin:1.5rem 0; opacity:0.3;'>", unsafe_allow_html=True)
    with st.expander("🗑️ Dangerous Zone: Reset Pipeline"):
        st.warning("This will completely delete the current `staging.db` and all versioned backups.")
        confirm_reset = st.checkbox("I confirm that I want to delete all transaction data.")
        if st.button("🔴 RESET & DELETE ALL DATABASE DATA", type="primary", disabled=not confirm_reset, use_container_width=True):
            try:
                # Delete main DB
                if os.path.exists(PATH_LOCAL_DB):
                    os.remove(PATH_LOCAL_DB)
                
                # Delete backups
                if os.path.exists(PATH_BACKUP_DIR):
                    import shutil
                    shutil.rmtree(PATH_BACKUP_DIR)
                    os.makedirs(PATH_BACKUP_DIR)
                
                st.success("🔥 Data wiped successfully! Redirecting...")
                st.session_state["pipeline_step"] = -1
                st.rerun()
            except Exception as e:
                st.error(f"Failed to reset: {e}")

    # ── MAINTENANCE SECTION (New) ──
    st.markdown("<hr style='margin:1.5rem 0; opacity:0.3;'>", unsafe_allow_html=True)
    with st.expander("🛠️ Maintenance & Data Integrity"):
        st.info("Use this if you notice data anomalies or duplicate entries in the dashboard.")
        if st.button("🧼 Scrub/De-duplicate Master Data", use_container_width=True):
            with st.spinner("Cleaning database and Excel files..."):
                try:
                    import sys
                    import os
                    # Add current dir to path to import repair_data
                    sys.path.append(os.getcwd())
                    from Project.repair_data import scrub_database, scrub_excel_card_share, scrub_excel_monitoring, PATH_DB, PATH_CARD, PATH_MON
                    
                    scrub_database(PATH_DB)
                    scrub_excel_card_share(PATH_CARD)
                    scrub_excel_monitoring(PATH_MON)
                    
                    st.success("✅ Data scrubbing complete! Check the dashboard.")
                except Exception as e:
                    st.error(f"Scrub failed: {e}")

with col2:
    section_label("2. Extraction Boundaries")

    # Auto-detect the actual date range available in staging.db so the
    # default values always match real data (prevents silent 0-row failures).
    _db_min, _db_max = get_db_date_bounds(PATH_LOCAL_DB)
    today = datetime.today()
    if _db_min and _db_max:
        _default_start = datetime.strptime(_db_min, "%Y-%m-%d").date()
        _default_end   = datetime.strptime(_db_max, "%Y-%m-%d").date()
        st.info(
            f"Auto-detected data window from staging.db: "
            f"**{_db_min}** to **{_db_max}**. Adjust if needed."
        )
    else:
        _default_start = today.replace(day=1).date()
        _default_end   = today.date()
        st.info("Set the SQL date range for the current reporting cycle.")

    start_date = st.date_input("Extract Start Date", value=_default_start)
    end_date   = st.date_input("Extract End Date",   value=_default_end)

st.markdown("<br>", unsafe_allow_html=True)

# ── SECTION 3: Prerequisite Checklist ────────────────────────────────────────
section_label("3. Pre-Flight Check")

prereqs = [
    ("staging.db (Database)",           os.path.exists(PATH_LOCAL_DB)),
    ("master_mid.xlsx (MID Master)",     os.path.exists(PATH_MID)),
    ("master_card_share.xlsx",           os.path.exists(PATH_CARD)),
    ("master_monitoring.xlsx",           os.path.exists(PATH_MON)),
]

all_ready = all(ok for _, ok in prereqs)

# Build the checklist HTML
rows_html = "".join([
    f'<div class="prereq-row">'
    f'<span class="prereq-icon">{"✅" if ok else "❌"}</span>'
    f'<span style="{"font-weight:600;" if ok else "opacity:0.7;"}">{label}</span>'
    f'</div>'
    for label, ok in prereqs
])
st.markdown(
    f'<div class="prereq-card">{rows_html}</div>',
    unsafe_allow_html=True,
)

if not all_ready:
    st.error(
        "❌ **Pipeline Locked**: One or more prerequisites are missing. "
        "Upload `staging.db` here and configure Master Excels in **⚙️ Global Settings**."
    )
else:
    st.success("✅ All prerequisites verified. Ready to execute.")

# ── SECTION 4: Execute Pipeline ───────────────────────────────────────────────
section_label("4. Execute Automation")

if all_ready:
    if st.button("▶️ RUN END-TO-END ANALYTICS PIPELINE", type="primary", width='stretch'):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str   = end_date.strftime("%Y-%m-%d")

        step_results = {}  # Store row counts for summary

        # ── Step 1: MID Cleaner ──────────────────────────────────────────────
        st.session_state["pipeline_step"] = 1
        with st.status("🛠️ Step 1: Processing Anchor MIDs...", expanded=True) as status_step1:
            try:
                st.write("Fetching unprocessed MIDs from staging.db...")
                df_mid = fetch_data_from_db(PATH_LOCAL_DB, '1_fetch_mid_null.sql', start_str, end_str)
                st.write(f"Fetched {len(df_mid)} new MIDs. Applying Regex classification...")
                bkp_dir = os.path.join(MASTER_DIR, "backups")
                os.makedirs(bkp_dir, exist_ok=True)
                new_n, tot_n = run_mid_cleaner(df_mid, PATH_MID, bkp_dir)

                st.write("Synchronizing updated PROCESSED_MID to Staging Database...")
                import sqlite3
                master_df = pd.read_excel(PATH_MID)
                master_df.columns = [str(c).strip().upper() for c in master_df.columns]
                conn = sqlite3.connect(PATH_LOCAL_DB)
                master_df.to_sql('PROCESSED_MID', conn, if_exists='replace', index=False)
                conn.close()

                step_results["mid"] = {"new": new_n, "total": tot_n}
                status_step1.update(label=f"✅ Step 1 Complete! {new_n} new MIDs classified. Total: {tot_n}", state="complete")
            except Exception as e:
                status_step1.update(label=f"❌ Mid Cleaner Failed: {e}", state="error")
                st.stop()

        # ── Step 2: Card Share Matrix ────────────────────────────────────────
        st.session_state["pipeline_step"] = 2
        with st.status("💳 Step 2: Extracting Card Share Analytics...", expanded=True) as status_step2:
            try:
                st.write("Executing parameterized SQL Card Share query...")
                df_card = fetch_data_from_db(PATH_LOCAL_DB, '2_fetch_card_share.sql', start_str, end_str)
                st.write(f"Fetched {len(df_card)} grouped records from staging DB.")

                # Guard: empty result means date range doesn't overlap with DB data.
                if len(df_card) == 0:
                    status_step2.update(
                        label="❌ Step 2 Aborted: SQL query returned 0 rows.",
                        state="error"
                    )
                    st.error(
                        f"**Card Share query returned 0 ANCHOR rows** for the date range "
                        f"`{start_str}` to `{end_str}`.\n\n"
                        "This usually means the `EDW_FETCH_DATE` values in `CARD_SHARE` "
                        "fall outside the selected range, or no rows have `IS_PROCESSED_BY_ETL = 0`. "
                        "Adjust the **Extract Start/End Date** above to match the actual data window shown."
                    )
                    st.stop()

                st.write("Formatting and dispatching to Excel via COM...")
                bkp_cdir = os.path.join(MASTER_DIR, "backups_card")
                os.makedirs(bkp_cdir, exist_ok=True)
                run_card_share_merge(df_card, PATH_LOCAL_DB, PATH_CARD, bkp_cdir)
                step_results["card"] = {"rows": len(df_card)}
                status_step2.update(label=f"✅ Step 2 Complete! {len(df_card)} records injected into Master Card Share.", state="complete")
            except Exception as e:
                status_step2.update(label=f"❌ Card Share Processor Failed: {e}", state="error")
                st.stop()

        # ── Step 3: Weekly Monitoring Pivot ──────────────────────────────────
        st.session_state["pipeline_step"] = 3
        with st.status("📅 Step 3: Pivoting Weekly Analytics Array...", expanded=True) as status_step3:
            try:
                st.write("Aggregating structured weekly monitoring facts...")
                df_mon = fetch_data_from_db(PATH_LOCAL_DB, '3_fetch_weekly_series.sql', start_str, end_str)
                st.write(f"Fetched {len(df_mon)} rows from staging DB.")

                # Guard: empty result means date range doesn't overlap with DB data.
                if len(df_mon) == 0:
                    status_step3.update(
                        label="❌ Step 3 Aborted: SQL query returned 0 rows.",
                        state="error"
                    )
                    st.error(
                        f"**Weekly Monitoring query returned 0 ANCHOR rows** for the date range "
                        f"`{start_str}` to `{end_str}`.\n\n"
                        "This usually means the `EDW_FETCH_DATE` values in `WEEKLY_MONITOR` "
                        "fall outside the selected range, or no rows have `IS_PROCESSED_BY_ETL = 0`. "
                        "Adjust the **Extract Start/End Date** above to match the actual data window shown."
                    )
                    st.stop()

                st.write("Traversing and writing array vectors to Excel...")
                bkp_mdir = os.path.join(MASTER_DIR, "backups_monitoring")
                os.makedirs(bkp_mdir, exist_ok=True)
                run_monitoring_merge(df_mon, PATH_LOCAL_DB, PATH_MON, bkp_mdir)
                step_results["monitoring"] = {"rows": len(df_mon)}
                status_step3.update(label=f"✅ Step 3 Complete! {len(df_mon)} rows pushed to analytics matrices.", state="complete")
            except Exception as e:
                status_step3.update(label=f"❌ Monitoring Processor Failed: {e}", state="error")
                st.stop()

        # ── All done: mark stepper as complete ───────────────────────────────
        st.session_state["pipeline_step"] = len(PIPELINE_STEPS) - 1

        # ── Post-Run Summary Card ─────────────────────────────────────────────
        st.success("🎉 **Pipeline Orchestration Complete!** Your analytics backend is fully synced.")

        p_bg    = "#1A2538"
        p_bdr   = "#2B4470"
        p_txt2  = "#A3B5CC"
        p_gold  = "#F0BE48"
        p_green = "#34D399"

        summary_rows = [
            ("🛠️ Anchor MIDs", f"{step_results.get('mid',{}).get('new', '—')} new classified"),
            ("💳 Card Share records", f"{step_results.get('card',{}).get('rows', '—')} rows"),
            ("📅 Monitoring rows", f"{step_results.get('monitoring',{}).get('rows', '—')} rows"),
            ("🕐 Completed at", datetime.now().strftime("%d %b %Y %H:%M:%S")),
        ]
        rows_html = "".join([
            f'<div style="display:flex;justify-content:space-between;padding:7px 0;'
            f'border-bottom:1px solid {p_bdr};font-size:0.86rem;">'
            f'<span style="color:{p_txt2};">{k}</span>'
            f'<span style="font-weight:700;color:{p_gold};">{v}</span>'
            f'</div>'
            for k, v in summary_rows
        ])
        st.markdown(
            f"""<div style="background:{p_bg};border:1px solid {p_bdr};
                           border-left:4px solid {p_green};border-radius:12px;
                           padding:18px 20px;margin:16px 0;">
              <div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:.07em;
                          color:{p_txt2};margin-bottom:10px;">📋 Pipeline Execution Summary</div>
              {rows_html}
            </div>""",
            unsafe_allow_html=True,
        )

        st.markdown("<br>", unsafe_allow_html=True)
        st.page_link("pages/4_Dashboard.py", label="**Go to Dashboard 📊**", icon="📈")
