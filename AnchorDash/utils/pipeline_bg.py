import os
import json
import threading
from datetime import datetime
import pandas as pd
import sqlite3

try:
    import pythoncom  # type: ignore
    HAS_PYTHONCOM = True
except Exception:
    pythoncom = None
    HAS_PYTHONCOM = False

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATUS_FILE = os.path.join(_BASE, "database", "pipeline_status.json")

def _init_status():
    return {
        "status": "idle",
        "step": -1,
        "message": "",
        "results": {},
        "error": "",
        "start_time": ""
    }

def get_pipeline_status():
    if not os.path.exists(STATUS_FILE):
        return _init_status()
    try:
        with open(STATUS_FILE, "r") as f:
            return json.load(f)
    except:
        return _init_status()

def set_pipeline_status(status_update):
    current = get_pipeline_status()
    current.update(status_update)
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump(current, f, indent=4)
    except:
        pass

def reset_pipeline_status():
    status = _init_status()
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=4)
    except:
        pass


def is_pipeline_supported() -> bool:
    return HAS_PYTHONCOM

def run_pipeline_thread(start_str, end_str, paths_config):
    """
    Executes the ETL pipeline in a background thread.
    paths_config is a dict containing PATH_LOCAL_DB, PATH_MID, PATH_CARD, PATH_MON, MASTER_DIR
    """
    if not HAS_PYTHONCOM:
        set_pipeline_status({
            "status": "error",
            "error": "Windows-only pipeline modules are unavailable in this cloud runtime."
        })
        return

    # Important: Initialize COM for the background thread
    pythoncom.CoInitialize()
    
    try:
        from utils.db_connector import fetch_data_from_db
        from modules.mid_cleaner import run_mid_cleaner
        from modules.card_share import run_card_share_merge
        from modules.monitoring import run_monitoring_merge
        
        PATH_LOCAL_DB = paths_config["PATH_LOCAL_DB"]
        PATH_MID      = paths_config["PATH_MID"]
        PATH_CARD     = paths_config["PATH_CARD"]
        PATH_MON      = paths_config["PATH_MON"]
        MASTER_DIR    = paths_config["MASTER_DIR"]

        step_results = {}
        
        # ── Step 1: MID Cleaner ──────────────────────────────────────────────
        set_pipeline_status({"step": 1, "message": "🛠️ Step 1: Processing Anchor MIDs (Querying staging.db...)", "status": "running"})
        
        df_mid = fetch_data_from_db(PATH_LOCAL_DB, '1_fetch_mid_null.sql', start_str, end_str)
        bkp_dir = os.path.join(MASTER_DIR, "backups")
        os.makedirs(bkp_dir, exist_ok=True)
        
        set_pipeline_status({"step": 1, "message": f"🛠️ Step 1: Acquired {len(df_mid)} new MIDs. Applying regex algorithms and merging with Master...", "status": "running"})
        new_n, tot_n = run_mid_cleaner(df_mid, PATH_MID, bkp_dir)

        set_pipeline_status({"step": 1, "message": "🛠️ Step 1: Classification complete. Updating SQLite staging.db with new PROCESSED_MID...", "status": "running"})
        master_df = pd.read_excel(PATH_MID)
        master_df.columns = [str(c).strip().upper() for c in master_df.columns]
        conn = sqlite3.connect(PATH_LOCAL_DB)
        master_df.to_sql('PROCESSED_MID', conn, if_exists='replace', index=False)
        conn.close()

        step_results["mid"] = {"new": new_n, "total": tot_n}
        
        # ── Step 2: Card Share Matrix ────────────────────────────────────────
        set_pipeline_status({"step": 2, "message": "💳 Step 2: Extracting Card Share Query from DB...", "results": step_results})
        
        df_card = fetch_data_from_db(PATH_LOCAL_DB, '2_fetch_card_share.sql', start_str, end_str)
        if len(df_card) == 0:
            set_pipeline_status({"status": "error", "error": "Card Share query returned 0 rows. Check date range."})
            return
            
        set_pipeline_status({"step": 2, "message": f"💳 Step 2: Dispatching {len(df_card)} records to Master Excel via COM...", "results": step_results})
        bkp_cdir = os.path.join(MASTER_DIR, "backups_card")
        os.makedirs(bkp_cdir, exist_ok=True)
        run_card_share_merge(df_card, PATH_LOCAL_DB, PATH_CARD, bkp_cdir)
        step_results["card"] = {"rows": len(df_card)}

        # ── Step 3: Weekly Monitoring Pivot ──────────────────────────────────
        set_pipeline_status({"step": 3, "message": "📅 Step 3: Extracting Weekly Monitoring Array from DB...", "results": step_results})
        
        df_mon = fetch_data_from_db(PATH_LOCAL_DB, '3_fetch_weekly_series.sql', start_str, end_str)
        if len(df_mon) == 0:
            set_pipeline_status({"status": "error", "error": "Weekly Monitoring query returned 0 rows. Check date range."})
            return
            
        set_pipeline_status({"step": 3, "message": f"📅 Step 3: Dispatching {len(df_mon)} records to Master Excel via COM...", "results": step_results})
        bkp_mdir = os.path.join(MASTER_DIR, "backups_monitoring")
        os.makedirs(bkp_mdir, exist_ok=True)
        run_monitoring_merge(df_mon, PATH_LOCAL_DB, PATH_MON, bkp_mdir)
        step_results["monitoring"] = {"rows": len(df_mon)}
        
        # ── Complete ────────────────────────────────────────────────────────
        set_pipeline_status({
            "status": "complete", 
            "step": 4, 
            "message": "🎉 Pipeline Orchestration Complete!", 
            "results": step_results
        })

    except Exception as e:
        set_pipeline_status({"status": "error", "error": str(e)})

    finally:
        pythoncom.CoUninitialize()

def start_pipeline_background(start_str, end_str, paths_config):
    if not HAS_PYTHONCOM:
        set_pipeline_status({
            "status": "error",
            "step": -1,
            "message": "",
            "error": "Pipeline execution is disabled on cloud Linux runtime (requires Windows COM).",
            "results": {},
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        return

    set_pipeline_status({
        "status": "running",
        "step": 0,
        "message": "Starting pipeline...",
        "error": "",
        "results": {},
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    
    t = threading.Thread(
        target=run_pipeline_thread, 
        args=(start_str, end_str, paths_config),
        daemon=True
    )
    t.start()
