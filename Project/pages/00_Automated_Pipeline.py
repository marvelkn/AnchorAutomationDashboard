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
    uploaded_db = st.file_uploader("Upload SQLite Database (.db)", type=['db', 'sqlite'])

    if uploaded_db:
        with open(PATH_LOCAL_DB, "wb") as f:
            f.write(uploaded_db.getvalue())
        st.success(f"✅ `staging.db` synchronized! Size: {len(uploaded_db.getvalue()) // (1024*1024)} MB")
        # Advance stepper to Step 1 (MID Cleaner) now that DB is ready
        st.session_state["pipeline_step"] = 0
        st.rerun()
    elif os.path.exists(PATH_LOCAL_DB):
        sz = os.path.getsize(PATH_LOCAL_DB) // (1024 * 1024)
        st.success(f"✅ Existing `staging.db` found locally ({sz} MB).")
    else:
        st.warning("⚠️ No database found. Please upload `staging.db`.")

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

                st.write("Synchronizing updated ALL_MID to Staging Database...")
                import sqlite3
                master_df = pd.read_excel(PATH_MID)
                master_df.columns = [str(c).strip().upper() for c in master_df.columns]
                conn = sqlite3.connect(PATH_LOCAL_DB)
                master_df.to_sql('master_mid', conn, if_exists='replace', index=False)
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
                        "This usually means the `EDW_FETCH_DATE` values in `raw_edw_card_share` "
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
                        "This usually means the `EDW_FETCH_DATE` values in `raw_edw_weekly` "
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
