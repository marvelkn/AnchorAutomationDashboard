# 🏦 BTN Anchor Intelligence Platform — Master Project Reference

> **Status:** Production-Ready / Active Development
> **Last Updated:** April 10, 2026
> **Stack:** Python 3.10+ · Streamlit ≥1.36 · PostgreSQL (Neon) · SQLite · Win32COM (local only)

---

## 🎯 PROJECT PURPOSE

The **Anchor Intelligence Platform** is a full-stack merchant analytics system built for Bank BTN's portfolio management team. It automates the entire data lifecycle for "Anchor" merchants (the bank's top-tier strategic partners) — from raw SQL/Excel ingestion → classification → weekly monitoring → ML clustering → churn prediction → interactive dashboard.

**Who uses it:** Portfolio Managers (PMs) at Bank BTN who need to monitor Anchor merchant KPIs, track weekly card-share and transaction data, detect churn risk early, and manage merchant-to-PM assignments.

---

## 🖥️ OPERATING MODES

The app has **two distinct modes** controlled by the `DATABASE_URL` environment variable:

| Mode | Condition | Database | ETL Pipeline |
|------|-----------|----------|-------------|
| **Local / Windows** | No `DATABASE_URL` | SQLite (`staging.db`) | Full (Excel COM, all 3 steps) |
| **Cloud** | `DATABASE_URL` is set (Neon PostgreSQL URL) | Neon PostgreSQL | Upload-only (SQLite → Neon ingestion) |

The `app.py` sidebar and every page checks `os.getenv("DATABASE_URL")` to branch logic. This flag is also used in `00_Automated_Pipeline.py` (variable: `cloud_mode_enabled`) and all utility modules.

> **Critical Rule:** Never assume local filesystem persistence in cloud mode. All files uploaded via Global Settings are stored in Neon (`master_files` table) and synced to disk on demand. The cloud ETL pipeline does **not** run Excel COM — it only ingests `.db` files into Neon.

---

## 🛠️ TECHNOLOGY STACK

| Layer | Technology |
|-------|-----------|
| UI Framework | Streamlit ≥1.36 (native multi-page navigation via `st.navigation()`) |
| Data Wrangling | Pandas ≥2.0, NumPy ≥1.24 |
| ML | Scikit-Learn ≥1.3 (K-Means++, StandardScaler), SciPy (Z-score) |
| Charting | Plotly ≥5.15 (interactive), Matplotlib (ETL diagnostics) |
| Local DB | SQLite3 (stdlib) — `staging.db` |
| Cloud DB | Neon PostgreSQL via SQLAlchemy ≥2.0 + psycopg2-binary |
| Excel I/O | openpyxl (read/write), pywin32/Win32COM (macro-compatible write — Windows only) |
| Scheduling | Python `threading` (background pipeline thread) |
| Config | `.streamlit/config.toml` (theme + upload limit) |

---

## 🗂️ COMPLETE FILE TREE

```
Project/
│
├── app.py                          # Streamlit entry point — sidebar, navigation, DB status indicator
├── repair_data.py                  # Maintenance scripts: scrub duplicates, Yoshinoya fix, Neon reset
│
├── 01_extract_and_clean.py         # ETL Step 1 — raw Excel load into SQLite (standalone script)
├── 02_transform_and_ml.py          # ETL Step 2 — ML clustering + anomaly detection (standalone script)
├── 03_load_to_datamart.py          # ETL Step 3 — datamart export + PM summaries (standalone script)
│
├── requirements.txt                # Python package dependencies
├── PROJECT_STRUCTURE.md            # This file
│
├── .streamlit/
│   └── config.toml                 # Theme (BTN Gold/Navy dark mode) + maxUploadSize=2000MB
│
├── pages/                          # Streamlit multi-page UI
│   ├── 4_Dashboard.py              # ★ Main analytics dashboard (Card Share, Monitoring, ML, Churn)
│   ├── 00_Automated_Pipeline.py    # ETL orchestrator + Neon cloud ingestion UI
│   ├── 0_Master_Configuration.py   # Global Settings — upload/manage master Excel files
│   ├── 01_Data_Editor.py           # CRUD editor for merchant MID records
│   └── 05_PM_Manager.py            # Portfolio Manager assignment interface
│
├── utils/                          # Shared utility modules
│   ├── theme.py                    # Design system — palette, CSS injection, component helpers
│   ├── db_connector.py             # SQLite query runner + date-bound auto-detection
│   ├── db_merger.py                # Incremental DB merge logic (new rows only)
│   ├── backup_manager.py           # File versioning — rotate 3 backups, restore
│   ├── pipeline_bg.py              # Background thread manager for ETL pipeline
│   ├── cloud_db.py                 # Neon/PostgreSQL engine builder + upsert helpers
│   ├── sqlite_to_neon.py           # Full SQLite → Neon ingestion with audit logging
│   ├── master_files_db.py          # ★ NEW — Neon persistence for master Excel blobs
│   └── __init__.py
│
├── modules/                        # Domain-specific ETL business logic
│   ├── mid_cleaner.py              # Regex classification engine for merchant MID → Anchor mapping
│   ├── card_share.py               # Monthly card-share matrix builder + Excel COM writer
│   └── monitoring.py              # Weekly monitoring series merger + Excel COM writer
│
├── Query/                          # Raw SQL scripts (run against staging.db)
│   ├── 1_fetch_mid_null.sql        # Fetches unclassified MIDs for classification
│   ├── 2_fetch_card_share.sql      # Fetches monthly card-share transaction data
│   └── 3_fetch_weekly_series.sql   # Fetches weekly monitoring time series
│
├── database/                       # Local database storage
│   ├── staging.db                  # ★ SQLite — primary local data store (all raw + processed tables)
│   ├── pipeline_status.json        # Background ETL status file (idle/running/complete/error)
│   └── backup/                     # staging.db version backups (v1, v2, v3)
│
├── data/
│   ├── master/                     # Master reference Excel files (source of truth)
│   │   ├── master_mid.xlsx         # ALL MID master — ~8 MB merchant classification map
│   │   ├── master_card_share.xlsx  # Card Share Master — monthly payment type matrix template
│   │   ├── master_monitoring.xlsx  # Monitoring Master — weekly series + PARAMETER sheet for governance
│   │   ├── governance_audit_log.csv# Audit trail of Anchor/PM governance decisions
│   │   ├── backups/                # master_mid version backups
│   │   ├── backups_card/           # master_card_share version backups
│   │   └── backups_monitoring/     # master_monitoring version backups
│   ├── raw/                        # Raw SQL exports (CSVs, Excel dumps)
│   └── testing/                    # Test data for development
│
└── static/
    └── btn_logo.png                # Bank BTN logo (used in sidebar brand header)
```

---

## 📄 PAGE-BY-PAGE REFERENCE

### `app.py` — Entry Point
- Registers all pages with `st.navigation()` (Streamlit ≥1.36 native routing)
- Renders the sidebar: BTN logo → env selector → DB status badge → theme toggle → custom nav links
- Nav is rebuilt manually with `st.page_link()` (the auto `stSidebarNav` is hidden via CSS)
- If no data exists (`staging.db` missing AND no `DATABASE_URL`), nav collapses to Setup mode

---

### `pages/4_Dashboard.py` — Analytics Dashboard
The most complex page (~1,560 lines). Cloud-aware: checks `DATABASE_URL` to select data source.

**Data Sources:**
- Cloud: `engine = build_engine()` → reads from Neon tables directly via SQL
- Local: `sqlite3.connect(PATH_DB)` → reads from `staging.db`

**Tables it reads:**
| Table | Purpose |
|-------|---------|
| `PROCESSED_CARD_SHARE` | YTD aggregate per merchant group |
| `PROCESSED_CARD_HISTORY` | Monthly historical for growth analytics |
| `PROCESSED_CARD_MONTHLY` | Monthly breakdown by payment type |
| `PROCESSED_MONITORING` | PM-level monitoring aggregate |
| `PROCESSED_MONITORING_WEEKLY` | Weekly series for trend charts |
| `TARGET` | Annual sales volume targets |

**Tabs:**
1. 💰 Card Share — Monthly TRX/SV/FBI with stacked bar + line charts
2. 📅 Weekly Monitoring — Weekly matrix per PM/merchant, trend lines
3. 🤖 ML Segmentation — K-Means++ cluster map (PREMIUM/REGULER/PASIF)
4. ⚠️ Churn & Risk — Z-score anomaly table, high-risk merchant list
5. 🔍 Merchant Explorer — Drill down per merchant
6. 🔮 AI Insights — Business narrative summaries
7. 📊 Batch Impact — Pipeline run impact reporting

**ML Engine (`run_ml()`):**
- Input: `df_card` + `df_mon` + optional `df_target`
- Features: `AVG_SV`, `AVG_FBI`, `RASIO_ONUS`, `SV_GROWTH_CLIPPED`, `ACHIEVEMENT_PCT`, `WEEKS_ACTIVE`
- Cluster labels assigned by average SV rank (highest SV → PREMIUM)
- Churn risk = multi-condition boolean (weeks active ≤2, growth ≤-95%, Z-score outlier)

---

### `pages/00_Automated_Pipeline.py` — ETL Orchestrator

**Cloud mode** (`DATABASE_URL` set): Shows Neon ingestion UI only.
- Upload `.db` file → `ingest_sqlite_bytes_to_neon()` copies all tables to Neon
- Single-table upsert via CSV/Excel (manual correction path)
- Cloud scrub + reset buttons (via `repair_data.py`)
- At startup: silently calls `sync_all_masters_to_disk()` from `utils/master_files_db.py` to ensure local master files are fresh for the governance check

**Local mode** (no `DATABASE_URL`): Full pipeline orchestrator.
- Upload `staging.db` → governance delta check → run 3-step pipeline
- Governance gate: detects new Anchors/PMs not in `master_monitoring.xlsx PARAMETER` sheet, blocks pipeline until resolved
- Pipeline runs in background thread (see `utils/pipeline_bg.py`)
- Rollback: restores previous `staging.db` versions from `database/backup/`
- Scrub: calls `repair_data.scrub_database()` for local de-duplication

**Governance flow:**
1. Upload `.db` → `_detect_governance_delta()` compares TARGET table vs PARAMETER sheet
2. If unknown Anchors/PMs found → `gov_status = "blocked"` → quarantine dialog opens
3. User approves/ignores each → `_append_to_parameter_sheet()` writes to local Excel → audit log
4. `gov_status = "resolved"` → pipeline can run

---

### `pages/0_Master_Configuration.py` — Global Settings

Manages three master reference Excel files. **Cloud-aware.**

**Cloud mode:**
- Files saved to **both** Neon (`public.master_files` → `master_files_db.py`) and local disk (cache)
- At page load: `sync_all_masters_to_disk()` runs **once per browser session** (gated by `st.session_state["_masters_synced"]`) to avoid unnecessary Neon calls on every button click
- Status badges show Neon sync status + upload timestamp (WIB/UTC+7)
- Download button fetches bytes from Neon

**Local mode:**
- Files saved only to `data/master/` on disk
- Behavior identical to pre-cloud version

**Version History / Rollback:**
- Keeps up to 3 local backup versions per file (via `backup_manager.rotate_backups()`)
- Restore also pushes restored bytes to Neon in cloud mode

**Session state keys used:**
- `_masters_synced` — prevents repeated Neon sync on reruns
- `_saved_master_mid` / `_saved_master_card` / `_saved_master_mon` — success banner flags

---

### `pages/01_Data_Editor.py` — Master Records Editor
CRUD interface for editing the merchant-to-anchor-group mapping stored in `PROCESSED_MID` table.

### `pages/05_PM_Manager.py` — PM Manager
Manages Portfolio Manager (PM) assignments. Reads from SQLite or Neon depending on mode.

---

## ⚙️ UTILITY MODULE REFERENCE

### `utils/theme.py`
Design system. All pages import from here.
- `apply_theme()` — injects global CSS (dark/light aware, BTN Gold/Navy palette)
- `page_header(icon, title, subtitle)` — standard page hero
- `section_label(text)` — gold-accented section divider
- `kpi_card(value, label, variant)` — metric card HTML
- `kpi_row(cards_list)` — renders a row of kpi_cards
- `pipeline_stepper(steps, current)` — progress stepper for ETL
- `stale_data_banner(db_path, threshold_hours)` — amber warning if DB is old
- Palette constants: `GOLD`, `NAVY`, `SURFACE`, `BORDER`, `TEXT_PRI`, `TEXT_SEC`, `GREEN`, `RED`, `AMBER`, `BLUE_ACC`
- `CLUSTER_COLORS`, `PAYMENT_COLORS` — chart color maps

**Timezone:** All timestamps displayed in **WIB (UTC+7)**. Use `_LOCAL_TZ = timezone(timedelta(hours=7))` when formatting `datetime.fromtimestamp()`.

---

### `utils/db_connector.py`
- `fetch_data_from_db(db_path, query_filename, start_date, end_date)` — reads a `.sql` file from `Query/`, injects date bounds via regex, executes against SQLite
- `get_db_date_bounds(db_path)` — auto-detects MIN/MAX `EDW_FETCH_DATE` from `CARD_SHARE` and `WEEKLY_MONITOR` tables

### `utils/db_merger.py`
- `merge_incremental_data(temp_path, target_path)` — appends only new rows from an uploaded DB to the existing one (incremental ingestion strategy)

### `utils/backup_manager.py`
- `rotate_backups(target_path, backup_dir, prefix, extension, max_versions=3)` — shifts v1→v2→v3, then copies current → v1
- `get_available_backups(backup_dir, prefix, extension)` — returns list of `{version, path, timestamp}` in **WIB time**
- `restore_backup(backup_path, target_path)` — copies backup → target

### `utils/pipeline_bg.py`
- `is_pipeline_supported()` — returns True only on Windows (checks for `pythoncom`)
- `start_pipeline_background(start_str, end_str, paths_config)` — spawns daemon thread, writes status to `database/pipeline_status.json`
- `get_pipeline_status()` / `set_pipeline_status()` / `reset_pipeline_status()` — JSON file-based IPC
- Pipeline thread calls: `run_mid_cleaner()` → `run_card_share_merge()` → `run_monitoring_merge()`

### `utils/cloud_db.py`
- `build_engine()` — creates pooled SQLAlchemy engine from `DATABASE_URL` env var
- `test_connection(engine)` — raises if unhealthy
- `read_uploaded_dataframe(uploaded_file)` — reads CSV/Excel from Streamlit uploader in-memory
- `upsert_dataframe(engine, dataframe, table_name, conflict_columns, schema)` — temp table → ON CONFLICT DO UPDATE

### `utils/sqlite_to_neon.py`
Full SQLite → Neon ingestion pipeline with audit logging.
- `ingest_sqlite_bytes_to_neon(engine, sqlite_bytes, schema, source_filename, progress_callback)` — reads all user tables from SQLite bytes, TRUNCATE + INSERT each into Neon, validates row counts, writes run record to `ingestion_runs` audit table
- `fetch_recent_ingestion_runs(engine, schema, limit)` — returns last N runs as DataFrame
- `ensure_ingestion_audit_table(engine, schema)` — creates `ingestion_runs` table if missing

### `utils/master_files_db.py` ★ NEW
Neon persistence for the three master Excel files. Stores binary content as `BYTEA` in `public.master_files`.

**Neon table schema:**
```sql
public.master_files (
    file_key   TEXT PRIMARY KEY,  -- 'master_mid' | 'master_card' | 'master_mon'
    filename   TEXT NOT NULL,     -- original filename
    content    BYTEA NOT NULL,    -- raw Excel bytes
    size_bytes BIGINT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
)
```

**API:**
- `ensure_master_files_table(engine)` — CREATE TABLE IF NOT EXISTS
- `save_master_to_db(engine, file_key, filename, content_bytes)` → bool
- `load_master_from_db(engine, file_key)` → `(filename, bytes)` or `(None, None)`
- `list_master_files(engine)` → dict keyed by file_key with size/updated_at metadata
- `sync_master_to_disk(engine, file_key, dest_path)` → bool — pull from Neon, write to local path
- `sync_all_masters_to_disk(engine, path_mid, path_card, path_mon)` → dict of results

---

## 🗄️ DATABASE SCHEMA (SQLite / Neon)

### Raw / Staging Tables (in `staging.db` / Neon)
| Table | Source | Key Columns |
|-------|--------|-------------|
| `CARD_SHARE` | SQL export | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, `MERCHANT_ANCHOR`, payment type cols |
| `WEEKLY_MONITOR` | SQL export | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, `PM`, weekly metric cols |
| `TARGET` | Manual upload | `MERCHANT_GROUP`, `PM`, `TARGET_VOL_2026` |
| `APP_METADATA` | Auto-created | `key`, `value` (`LAST_DATA_UPDATE`, `NEW_DATA_SIGNAL`) |

### Processed Tables (written by pipeline)
| Table | Written by | Purpose |
|-------|-----------|---------|
| `PROCESSED_MID` | `mid_cleaner.py` | Classified merchant → anchor group mapping |
| `PROCESSED_CARD_SHARE` | `card_share.py` | YTD totals per merchant group |
| `PROCESSED_CARD_HISTORY` | `card_share.py` | Monthly history for YoY growth |
| `PROCESSED_CARD_MONTHLY` | `card_share.py` | Monthly breakdown by payment type |
| `PROCESSED_MONITORING` | `monitoring.py` | PM-level annual aggregate |
| `PROCESSED_MONITORING_WEEKLY` | `monitoring.py` | Weekly series in long format |

### Neon-Only Tables
| Table | Purpose |
|-------|---------|
| `public.ingestion_runs` | Audit log of SQLite → Neon ingestion runs |
| `public.master_files` | Binary storage of master Excel files |

---

## 🔄 DATA FLOW

### Local Pipeline (Windows only)
```
staging.db (uploaded)
    │
    ├── Query/1_fetch_mid_null.sql ──► modules/mid_cleaner.py ──► master_mid.xlsx + PROCESSED_MID
    ├── Query/2_fetch_card_share.sql ─► modules/card_share.py ──► master_card_share.xlsx + PROCESSED_CARD_*
    └── Query/3_fetch_weekly_series.sql ► modules/monitoring.py ─► master_monitoring.xlsx + PROCESSED_MONITORING*
                                                                          │
                                                                          └── pages/4_Dashboard.py reads all tables
```

### Cloud Pipeline
```
User uploads staging.db (.db file)
    │
    └── utils/sqlite_to_neon.py ──► Neon PostgreSQL (all tables)
                                          │
                                          └── pages/4_Dashboard.py reads from Neon via SQLAlchemy
```

### Master File Flow (Cloud Mode)
```
User uploads Excel in Global Settings
    │
    ├── Local disk (data/master/*.xlsx)   ← always written as cache
    └── Neon (public.master_files BYTEA)  ← persistent cloud storage
          │
          └── On Pipeline page load: sync_all_masters_to_disk() ──► local disk (refreshed once/session)
```

---

## 🧠 ML ENGINE

### K-Means++ Clustering
- **Features (6):** `log(AVG_SV)`, `log(AVG_FBI)`, `RASIO_ONUS`, `SV_GROWTH_CLIPPED`, `ACHIEVEMENT_PCT`, `WEEKS_ACTIVE`
- **K=3 default labels:** PREMIUM (highest avg SV) → REGULER → PASIF
- **K=4:** ELITE → PREMIUM → REGULER → PASIF
- **K=5:** ELITE → PREMIUM → REGULER → PASIF → DORMANT
- Labels are rank-assigned by cluster average SV (not hardcoded to cluster ID)

### Churn Risk Detection
Merchant is flagged `HIGH RISK ⚠️` if **any** of:
- `WEEKS_ACTIVE ≤ 2` (nearly inactive)
- `SV_GROWTH_RATE ≤ -95%` AND `ACHIEVEMENT_PCT < 5%`
- Cluster is PASIF or DORMANT AND `ACHIEVEMENT_PCT < 1%`
- `ZSCORE_SV < -1.5` (Z-score on log SV)
- `ZSCORE_FBI < -1.5`
- `ZSCORE_GROWTH < -1.5`

---

## ⚡ KEY ENVIRONMENT VARIABLES

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Cloud only | Full Neon PostgreSQL connection string (activates cloud mode) |
| `DB_POOL_SIZE` | Optional | SQLAlchemy pool size (default: 5) |
| `DB_MAX_OVERFLOW` | Optional | Max overflow connections (default: 10) |
| `DB_POOL_TIMEOUT` | Optional | Pool timeout seconds (default: 30) |
| `DB_POOL_RECYCLE` | Optional | Connection recycle seconds (default: 1800) |

Set these in `.streamlit/secrets.toml` locally or in the cloud platform's environment config.

---

## ⚠️ KNOWN CONSTRAINTS & GOTCHAS

1. **Windows-only (local pipeline):** The ETL pipeline (`mid_cleaner`, `card_share`, `monitoring`) uses `pywin32`/`win32com` for Excel COM automation. This **only works on Windows**. `is_pipeline_supported()` returns False on Linux/Mac/cloud.

2. **Ephemeral filesystem (cloud):** Streamlit Cloud and similar platforms reset the filesystem on each deployment. Do NOT store important files only on disk in cloud mode — always use Neon. The `master_files_db.py` module handles this.

3. **Session state sync gate:** `st.session_state["_masters_synced"]` gates the expensive Neon→disk sync to once per browser session. If you need to force a re-sync mid-session, pop this key.

4. **Governance check reads local file:** `_read_master_parameter()` in `00_Automated_Pipeline.py` reads `master_monitoring.xlsx` from disk. In cloud mode, `sync_all_masters_to_disk()` runs at page startup to ensure the file is present.

5. **Timezone:** All UI timestamps display in **WIB (UTC+7)**. System clock may be UTC — always use `datetime.fromtimestamp(mtime, tz=timezone(timedelta(hours=7)))`, not the bare `datetime.fromtimestamp(mtime)`.

6. **Neon table names:** All tables ingested from SQLite are **lowercased** in Neon (e.g., `PROCESSED_CARD_SHARE` becomes `processed_card_share`). The Dashboard normalizes column names back to uppercase after reading.

7. **Upload size:** `.streamlit/config.toml` sets `maxUploadSize = 2000` (MB). Main `.db` file is ~68 MB; `master_mid.xlsx` is ~8 MB — both well within limits.

8. **Deduplication:** The scrub function uses row dropping (not summation) to handle historical data anomalies. Incremental merge uses composite keys to prevent duplicate ingestion.

---

## 🗓️ MAINTENANCE CHECKLIST

- **DB freshness:** Sidebar shows 🟢 Fresh (<24h), 🟡 Aging (<72h), 🔴 Stale (>72h)
- **After new monthly data:** Upload updated `staging.db` → run pipeline → verify Dashboard tab counts
- **Master file changes:** Upload via Global Settings; versioning is automatic (3 backups kept)
- **Neon audit:** Check `ingestion_runs` table for failed runs; use "Recent ingestion runs" expander in Pipeline page
- **Governance:** Review `data/master/governance_audit_log.csv` for all Anchor/PM approval decisions

---

*Built for Bank BTN Sidang Magang — Semester 6 · 2026*