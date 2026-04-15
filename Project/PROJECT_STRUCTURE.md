# BTN Anchor Intelligence Platform — Master Project Reference

> **Status:** Active Development — Dash migration in progress
> **Last Updated:** April 15, 2026
> **Legacy stack:** Python 3.10+ · Streamlit ≥1.36 · SQLite / Neon PostgreSQL · Win32COM (local only)
> **New stack:** Python 3.10+ · Plotly Dash ≥2.14 · Dash Bootstrap Components ≥1.5 · same DB layer

---

## Project Purpose

The **Anchor Intelligence Platform** is a full-stack merchant analytics system for Bank BTN's portfolio management team. It automates the complete data lifecycle for "Anchor" merchants — from raw SQL/Excel ingestion → classification → weekly monitoring → ML clustering → churn prediction → interactive dashboard.

**Who uses it:** Portfolio Managers (PMs) at Bank BTN who monitor Anchor merchant KPIs, track weekly card-share and transaction data, detect churn risk early, and manage merchant-to-PM assignments.

---

## Repository Layout

```
AnchorAutomationDashboard/
│
├── Project/                    ← Streamlit app (legacy, production)
│   └── ...                     (see "Streamlit Project" section below)
│
├── AnchorDash/                 ← Plotly Dash rewrite (active migration)
│   └── ...                     (see "Dash Project" section below)
│
├── .gitignore
└── README.md
```

Both projects share the **same database** (`Project/database/staging.db`) and the **same ETL modules** (`Project/modules/`, `Project/utils/`). The Dash project copies `modules/` and `utils/` verbatim — no ETL logic was changed.

---

## Operating Modes

Both apps support two distinct modes, controlled by the `DATABASE_URL` environment variable:

| Mode | Condition | Database | ETL |
|------|-----------|----------|-----|
| **Local / Windows** | No `DATABASE_URL` env var | SQLite `database/staging.db` | Full pipeline (Excel COM, 3 steps) |
| **Cloud** | `DATABASE_URL` set (Neon PostgreSQL URL) | Neon PostgreSQL | Upload-only (`.db` → Neon ingestion) |

> **Rule:** Never assume local filesystem persistence in cloud mode. All master Excel files are stored in Neon (`master_files` table) and synced to disk on demand.

---

## Streamlit Project (`Project/`)

### Technology Stack

| Layer | Technology |
|-------|-----------|
| UI | Streamlit ≥1.36 (native multi-page via `st.navigation()`) |
| Data | Pandas ≥2.0, NumPy ≥1.24 |
| ML | Scikit-Learn ≥1.3 (K-Means++, Isolation Forest), SciPy, statsmodels |
| Charts | Plotly ≥5.15 |
| Local DB | SQLite3 — `database/staging.db` |
| Cloud DB | Neon PostgreSQL via SQLAlchemy ≥2.0 + psycopg2-binary |
| Excel I/O | openpyxl (read), pywin32/Win32COM (write — Windows only) |
| Theming | CSS injection via `utils/theme.py` |

### File Tree

```
Project/
│
├── app.py                          # Entry point — sidebar, navigation, DB status
├── repair_data.py                  # Maintenance: scrub duplicates, Neon reset
├── requirements.txt
├── PROJECT_STRUCTURE.md            # This file
│
├── pages/
│   ├── 4_Dashboard.py              # ★ Main analytics dashboard (~1,800 lines)
│   ├── 00_Automated_Pipeline.py    # ETL orchestrator + Neon cloud ingestion
│   ├── 0_Master_Configuration.py   # Global Settings — upload master Excel files
│   ├── 01_Data_Editor.py           # CRUD editor for PROCESSED_MID
│   └── 05_PM_Manager.py            # Portfolio Manager assignment UI
│
├── utils/
│   ├── theme.py                    # Design system — CSS injection, palette, component helpers
│   ├── db_connector.py             # SQLite query runner + date-bound detection
│   ├── db_merger.py                # Incremental DB merge (new rows only)
│   ├── backup_manager.py           # File versioning — rotate 3 backups
│   ├── pipeline_bg.py              # Background thread manager for ETL
│   ├── cloud_db.py                 # Neon engine builder + upsert helpers
│   ├── sqlite_to_neon.py           # Full SQLite → Neon ingestion with audit log
│   ├── master_files_db.py          # Neon BYTEA persistence for master Excel files
│   └── __init__.py
│
├── modules/
│   ├── mid_cleaner.py              # Regex classification: MID → Anchor/Group mapping
│   ├── card_share.py               # Monthly card-share matrix builder + Excel COM writer
│   └── monitoring.py               # Weekly monitoring series merger + Excel COM writer
│
├── Query/
│   ├── 1_fetch_mid_null.sql        # Fetch unclassified MIDs
│   ├── 2_fetch_card_share.sql      # Fetch monthly card-share data
│   └── 3_fetch_weekly_series.sql   # Fetch weekly monitoring series
│
├── database/
│   ├── staging.db                  # ★ SQLite — primary local data store
│   ├── staging.sqbpro              # DB Browser project file
│   ├── IMPORT_HOWTO.txt            # Manual import instructions
│   ├── pipeline_status.json        # ETL background thread status (gitignored)
│   └── backup/                     # staging.db version backups (gitignored)
│
├── data/
│   ├── master/
│   │   ├── master_mid.xlsx         # ALL MID master (~8 MB merchant classification map)
│   │   ├── master_card_share.xlsx  # Card Share Master — monthly payment type matrix
│   │   └── master_monitoring.xlsx  # Monitoring Master — weekly series + PARAMETER sheet
│   ├── raw/                        # Raw SQL CSV exports
│   └── (backups gitignored)
│
└── static/
    └── btn_logo.png
```

### How to Run (Streamlit)

```bash
cd Project
pip install -r requirements.txt

# Local mode (Windows, full ETL pipeline)
streamlit run app.py

# Cloud mode (any OS, Neon DB)
export DATABASE_URL="postgresql://user:pass@host/db"
streamlit run app.py
```

### Page Reference

#### `app.py` — Entry Point
- Registers pages via `st.navigation()` (Streamlit ≥1.36)
- Sidebar: BTN logo → DB status badge → theme toggle → custom nav links
- If no database and no `DATABASE_URL`: collapses nav to Setup mode only

#### `pages/4_Dashboard.py` — Analytics Dashboard (~1,800 lines)
Cloud-aware: checks `DATABASE_URL` to select data source. Reads 6 processed tables.

**5 Tabs:**
1. **Card Share** — Monthly TRX/SV/FBI with stacked bar, line trend, donut charts
2. **Weekly Monitoring** — Weekly matrix per PM/merchant, trend lines, heatmap
3. **ML Segmentation** — K-Means++ cluster map (PREMIUM/REGULER/PASIF) + 3D scatter
4. **Risk & Churn** — Risk score table, gauge, per-merchant drill-down
5. **Overview** — Portfolio-wide KPIs + PM coverage summary

**AI Insights (no LLM — purely algorithmic):**
- Silent Churn Anomaly Scanner: compares latest activity to 4-week moving average
- Deep Dive & Projection: Holt-Winters forecast + domain-heuristic risk factor scoring

#### `pages/00_Automated_Pipeline.py` — ETL Orchestrator
- **Cloud mode:** SQLite `.db` upload → `ingest_sqlite_bytes_to_neon()` → all tables in Neon
- **Local mode:** governance gate → 3-step background pipeline → rollback support

**Governance flow:**
1. Upload `staging.db` → compare TARGET table vs PARAMETER sheet in `master_monitoring.xlsx`
2. If unknown Anchors/PMs found → quarantine dialog (approve / ignore each)
3. Approved entries appended to PARAMETER sheet + audit log
4. Pipeline unblocked → runs `mid_cleaner` → `card_share` → `monitoring`

---

## Dash Project (`AnchorDash/`)

### Why Migrate?
- Streamlit reruns the **entire script** on every user interaction — expensive for a 5-tab dashboard with 10+ Plotly charts
- Streamlit's CSS layer fights its own component styles, causing persistent light-mode rendering bugs
- Limited layout control: sidebar width, custom nav, modal dialogs all require hacky CSS injection
- Dash has a **proper callback graph** (only changed components update), a real HTML/CSS layout engine, and DBC themes that own all styling with zero injection

### Technology Stack

| Layer | Technology |
|-------|-----------|
| UI | Plotly Dash ≥2.14 + Dash Bootstrap Components ≥1.5 |
| Theme | DBC SPACELAB — single compiled Bootstrap CSS, no runtime injection |
| State | `dcc.Store` (replaces `st.session_state`) |
| Caching | `functools.lru_cache` / `flask-caching` (replaces `@st.cache_data`) |
| Data / ML | Same as Streamlit (Pandas, Scikit-Learn, statsmodels) |
| DB | Same as Streamlit (SQLite / Neon via SQLAlchemy) |

### File Tree

```
AnchorDash/
│
├── app.py                      # Dash instance, SPACELAB theme, top-level layout
├── wsgi.py                     # Production entry point (gunicorn wsgi:server)
├── requirements.txt            # dash, dash-bootstrap-components, flask-caching (no streamlit)
├── test_services.py            # Step-1 gate: smoke-test all data loaders
│
├── services/                   # ★ NEW — thin data-access layer
│   ├── data_service.py         # load_card_share(), load_monitoring_weekly(), db_status(), etc.
│   └── ml_service.py           # run_ml(), hw_forecast() — no UI imports
│
├── layouts/                    # Pure layout builders (no callbacks)
│   ├── sidebar.py              # dbc.Nav sidebar with brand header + DB status badge
│   └── kpi_cards.py            # kpi_card() / kpi_row() using dbc.Card
│
├── pages/                      # One file per page (Dash 2.x use_pages=True)
│   ├── dashboard.py            # 5-tab analytics dashboard (Overview, Card Share,
│   │                           #   Weekly Monitoring, Segmentation, Risk & Churn)
│   ├── pipeline.py             # Upload + ETL status
│   ├── data_editor.py          # DataTable CRUD for PROCESSED_MID
│   ├── pm_manager.py           # PM assignment editor
│   └── settings.py             # Master file upload + Neon sync
│
├── callbacks/                  # All @callback definitions, split by feature
│   ├── nav_callbacks.py        # DB status badge (dcc.Interval, 60 s refresh)
│   ├── filter_callbacks.py     # Group→Brand cascade, dcc.Store writers, global KPI strip
│   ├── card_share_callbacks.py # Year + filters → stacked bar / line / donut + top-N table
│   ├── monitoring_callbacks.py # Year + PM + DIMENSI → line + heatmap + matrix + CSV export
│   ├── ml_callbacks.py         # K slider + PM → 3D scatter, pie, box, PM×cluster stack
│   └── risk_callbacks.py       # Risk tier → pie, gauge, table, per-merchant drill-down
│
├── assets/                     # Dash auto-serves as static files
│   ├── btn_logo.png
│   └── custom.css              # ~90-line override (DBC owns base styles)
│
├── modules/                    # UNCHANGED — copied verbatim from Project/modules/
├── utils/                      # UNCHANGED — copied verbatim from Project/utils/
├── Query/                      # UNCHANGED — SQL scripts
└── database/                   # staging.sqbpro only (staging.db gitignored)
```

### How to Run (Dash)

```bash
cd AnchorDash
pip install -r requirements.txt

# Verify data loads (Step 1 gate)
python test_services.py

# Development server
python app.py
# → http://localhost:8050

# Production (gunicorn)
gunicorn wsgi:server --bind 0.0.0.0:8050 --workers 2
```

### Streamlit → Dash Component Map

| Streamlit | Dash / DBC Equivalent | Notes |
|-----------|----------------------|-------|
| `st.sidebar` | `dbc.Col(width=2)` + `dbc.Nav` | Fixed-width column, no built-in sidebar |
| `st.columns([1,1,1,1])` | `dbc.Row([dbc.Col(width=3), ...])` | Bootstrap 12-column grid |
| `st.expander()` | `dbc.Accordion` / `dbc.Collapse` | `dbc.AccordionItem` per section |
| `st.selectbox()` | `dcc.Dropdown(clearable=False)` | Single select by default |
| `st.multiselect()` | `dcc.Dropdown(multi=True)` | Same component, `multi=True` |
| `st.slider()` | `dcc.Slider()` | Direct equivalent |
| `st.plotly_chart()` | `dcc.Graph(id="...", figure=fig)` | `figure` prop updated by callbacks |
| `st.tabs()` | `dbc.Tabs([dbc.Tab(...)])` | |
| `st.metric()` | `dbc.Card([dbc.CardBody([...])])` | More layout control |
| `st.dataframe()` | `dash_table.DataTable(...)` | Native sort/filter/page |
| `st.download_button()` | `dcc.Download` + callback | |
| `@st.cache_data` | `functools.lru_cache` or `flask-caching` | |
| `st.session_state["k"]` | `dcc.Store(id="store-k", storage_type="session")` | Browser sessionStorage |
| `st.spinner()` | `dbc.Spinner(children=[dcc.Graph(...)])` | |

### State Management: Streamlit vs Dash

**Streamlit (reactive script):**
```
User changes dropdown
  → entire page.py reruns top to bottom
  → all DataFrames recomputed
  → all charts re-rendered
```

**Dash (callback graph):**
```
User changes dropdown
  → only @callback(s) that listed that dropdown as Input fire
  → only the Output components those callbacks return are updated
  → everything else stays frozen
```

**Cascading filter example (Group → Brand):**
```python
# callbacks/filter_callbacks.py
@callback(
    Output("dd-brand", "options"),
    Output("dd-brand", "value"),
    Input("dd-group", "value"),      # fires when group changes
)
def update_brand_options(sel_group):
    df = load_card_share()
    if sel_group != "ALL GROUPS":
        df = df[df["MERCHANT_GROUP"] == sel_group]
    brands = ["ALL BRANDS"] + sorted(df["MERCHANT_BRAND"].unique().tolist())
    return [{"label": b, "value": b} for b in brands], "ALL BRANDS"
```

---

## Database Schema

All tables exist in both SQLite (`staging.db`) and Neon PostgreSQL (column names lowercase in Neon). The Dashboard normalizes to uppercase after every read.

### Raw / Staging Tables

#### `ALL_MID` — 106,126 rows
Master MID registry. Source: EDW SQL export.

| Column | Type | Notes |
|--------|------|-------|
| `ID` | INTEGER | PK (autoincrement) |
| `MERCHANT_ID` | INTEGER | Bank MID identifier |
| `TERMINAL_ID` | TEXT | |
| `MERCHANT_NAME` | TEXT | |
| `EQUIP` | TEXT | Terminal equipment type |
| `MCC` | TEXT | Merchant Category Code |
| `CITY` | TEXT | |
| `BRANCH_CODE` | TEXT | |
| `INSTALLATION_DATE` | TEXT | ISO date string |
| `TERMINAL_STATUS` | TEXT | ACTIVE / INACTIVE |
| `EDW_FETCH_DATE` | TEXT | Source extract date |
| `IS_PROCESSED_BY_ETL` | INTEGER | 0/1 flag |
| `MAPPED_MERCHANT_GROUP` | TEXT | Anchor group mapping |
| `MERCHANT_CATEGORY_NAME` | TEXT | |
| `PROVINCE` | TEXT | |
| `MDR_RATE` | REAL | |
| `TERMINAL_TYPE` | TEXT | |
| `CONTRACT_START_DATE` | TEXT | |
| `LAST_TRX_DATE` | TEXT | |
| `EXTRACT_BATCH_ID` | TEXT | |
| `MERCHANT_TYPE` | TEXT | |
| `SEGMENT` | TEXT | ANCHOR / RETAIL |
| `SETTLEMENT_CYCLE` | TEXT | |
| `IS_KEY_MERCHANT` | INTEGER | 0/1 flag |
| `ONBOARDING_CHANNEL` | TEXT | |
| `LAST_SETTLEMENT_DATE` | TEXT | |
| `ANNUAL_VOL_ESTIMATE` | REAL | |
| `RISK_LEVEL` | TEXT | |
| `STAGING_INSERTED_AT` | TEXT | Insert timestamp |

---

#### `CARD_SHARE` — 3,391 rows
Monthly card-share transaction data per merchant brand. Source: EDW SQL export.

| Column | Type | Notes |
|--------|------|-------|
| `ID` | INTEGER | |
| `TRANSACTION_MONTH` | INTEGER | YYYYMM format |
| `MERCHANT_GROUP` | TEXT | Anchor merchant group name |
| `MERCHANT_BRAND` | TEXT | Anchor brand / sub-brand |
| `TRX_DEBIT_ONUS` | REAL | On-us debit transaction count |
| `TRX_DEBIT_OFFUS` | REAL | Off-us debit transaction count |
| `TRX_CREDIT_OFFUS` | REAL | Off-us credit transaction count |
| `TRX_CREDIT_ONUS` | REAL | On-us credit transaction count |
| `TRX_QRIS_ONUS` | REAL | On-us QRIS transaction count |
| `TRX_QRIS_OFFUS` | REAL | Off-us QRIS transaction count |
| `TOTAL_TRX` | REAL | Sum of all TRX columns |
| `VOL_DEBIT_ONUS` | REAL | On-us debit sales volume (IDR) |
| `VOL_DEBIT_OFFUS` | REAL | |
| `VOL_CREDIT_OFFUS` | REAL | |
| `VOL_CREDIT_ONUS` | REAL | |
| `VOL_QRIS_ONUS` | REAL | |
| `VOL_QRIS_OFFUS` | REAL | |
| `TOTAL_SV` | REAL | Total sales volume (IDR) |
| `FBI_DEBIT_ONUS` | REAL | Fee-based income, on-us debit |
| `FBI_DEBIT_OFFUS` | REAL | |
| `FBI_CREDIT_OFFUS` | REAL | |
| `FBI_CREDIT_ONUS` | REAL | |
| `FBI_QRIS_ONUS` | REAL | |
| `FBI_QRIS_OFFUS` | REAL | |
| `TOTAL_FBI` | REAL | Total fee-based income (IDR) |
| `EDW_FETCH_DATE` | TEXT | Source extract date |
| `IS_PROCESSED_BY_ETL` | INTEGER | 0/1 flag |
| `YTD_TRX` | REAL | Year-to-date transaction count |
| `YTD_VOL` | REAL | Year-to-date sales volume |
| `YTD_FBI` | REAL | Year-to-date fee income |
| `ACTIVE_MID_COUNT` | INTEGER | Active terminals this month |
| `MARKET_SHARE_TRX` | REAL | Market share by transactions |
| `MARKET_SHARE_VOL` | REAL | Market share by volume |
| `PREV_MONTH_TRX` | REAL | Previous month transaction count |
| `PREV_MONTH_VOL` | REAL | Previous month volume |
| `MOM_TRX_GROWTH` | REAL | Month-over-month TRX growth rate |
| `MOM_VOL_GROWTH` | REAL | Month-over-month VOL growth rate |
| `SEGMENT` | TEXT | |
| `REGION` | TEXT | |
| `CHANNEL` | TEXT | |
| `STAGING_INSERTED_AT` | TEXT | |

---

#### `WEEKLY_MONITOR` — 5,791 rows
Weekly monitoring aggregates per merchant group. Source: EDW SQL export.

| Column | Type | Notes |
|--------|------|-------|
| `ID` | INTEGER | PK |
| `MERCHANT_GROUP` | TEXT | Anchor merchant group |
| `PM_NAME` | TEXT | Portfolio Manager name |
| `YEAR` | INTEGER | Calendar year |
| `WEEK_NUM` | INTEGER | ISO week number (1–52) |
| `WEEKLY_TRX` | REAL | Weekly transaction count |
| `WEEKLY_VOL` | REAL | Weekly sales volume (IDR) |
| `WEEKLY_FBI` | REAL | Weekly fee-based income (IDR) |
| `WEEKLY_ACTIVE_MID` | INTEGER | Active terminals this week |
| `WEEKLY_AVG_TRX_PER_MID` | REAL | |
| `WEEK_START_DATE` | TEXT | |
| `WEEK_END_DATE` | TEXT | |
| `WOW_TRX_GROWTH` | REAL | Week-over-week TRX growth |
| `WOW_VOL_GROWTH` | REAL | Week-over-week VOL growth |
| `CUMULATIVE_YTD_TRX` | REAL | |
| `CUMULATIVE_YTD_VOL` | REAL | |
| `ACTIVE_TERMINAL_COUNT` | INTEGER | |
| `EDW_FETCH_DATE` | TEXT | |
| `IS_PROCESSED_BY_ETL` | INTEGER | 0/1 flag |
| `EXTRACT_BATCH_ID` | TEXT | |
| `SOURCE_SYSTEM` | TEXT | |
| `REGION` | TEXT | |
| `CHANNEL` | TEXT | |
| `SEGMENT` | TEXT | |
| `MERCHANT_TYPE` | TEXT | |
| `STAGING_INSERTED_AT` | TEXT | |

---

#### `TARGET` — 46 rows
Annual sales/TRX/FBI targets per merchant group. Source: manually extracted from `master_monitoring.xlsx`.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `PM` | TEXT | Portfolio Manager name |
| `VOL_2025` | REAL | Actual 2025 sales volume |
| `TARGET_VOL_2026` | REAL | 2026 annual volume target |
| `TRX_2025` | REAL | Actual 2025 transaction count |
| `TARGET_TRX_2026` | REAL | 2026 annual TRX target |
| `FBI_2025` | REAL | Actual 2025 fee income |
| `TARGET_FBI_2026` | REAL | 2026 annual FBI target |

---

#### `APP_METADATA` — 2 rows
Key-value store for app-level signals.

| Column | Type | Notes |
|--------|------|-------|
| `key` | TEXT | PK |
| `value` | TEXT | |

**Known keys:**
- `LAST_DATA_UPDATE` — ISO timestamp of last pipeline run
- `NEW_DATA_SIGNAL` — `"1"` if pipeline ran since last Dashboard view; cleared on read

---

### Processed Tables (written by ETL pipeline)

#### `PROCESSED_MID` — 196,236 rows
Classified merchant-to-anchor mapping. Written by `modules/mid_cleaner.py`.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_ID` | TEXT | Bank MID (may be string-padded) |
| `MERCHANT_NAME` | TEXT | |
| `EQUIP` | TEXT | Terminal equipment type |
| `SEGMEN` | TEXT | Raw segment label |
| `SEGMENT` | TEXT | Normalised: ANCHOR / RETAIL |
| `MERCHANT_GROUP` | TEXT | Anchor group (e.g., ALFAMART, INDOMARET) |
| `MERCHANT_BRAND` | TEXT | Sub-brand within group |
| `WILAYAH` | TEXT | Region |

---

#### `PROCESSED_CARD_SHARE` — 136 rows
YTD aggregated card-share per merchant group. Written by `modules/card_share.py`.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `MERCHANT_ANCHOR` | TEXT | Anchor brand |
| `TOTAL_SV` | REAL | YTD sales volume (IDR) |
| `TOTAL_TRX` | REAL | YTD transaction count |
| `TOTAL_FBI` | REAL | YTD fee-based income (IDR) |
| `SV_ONUS` | REAL | YTD on-us sales volume |
| `RASIO_ONUS` | REAL | On-us ratio (0.0–1.0) |
| `N_BULAN` | INTEGER | Number of months with data |
| `BULAN_TERAKHIR` | INTEGER | Last month with data (YYYYMM) |

---

#### `PROCESSED_CARD_HISTORY` — 2,056 rows
Monthly history for YoY growth analytics. Written by `modules/card_share.py`.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `MERCHANT_ANCHOR` | TEXT | |
| `TRX_MONTH` | INTEGER | YYYYMM |
| `YEAR` | INTEGER | Calendar year |
| `TOTAL_SV` | REAL | |
| `TOTAL_TRX` | REAL | |
| `TOTAL_FBI` | REAL | |

---

#### `PROCESSED_CARD_MONTHLY` — 2,056 rows
Monthly breakdown by payment type. Written by `modules/card_share.py`. Used by Card Share tab charts.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `MERCHANT_ANCHOR` | TEXT | |
| `TRX_MONTH` | INTEGER | YYYYMM |
| `YEAR` | INTEGER | |
| `TOTAL_TRX` | REAL | |
| `TOTAL_SV` | REAL | |
| `TOTAL_FBI` | REAL | |
| `TRX_DEBIT_ONUS` | REAL | On-us debit count |
| `TRX_DEBIT_OFFUS` | REAL | |
| `TRX_CREDIT_OFFUS` | REAL | |
| `TRX_QRIS_ONUS` | REAL | |
| `TRX_QRIS_OFFUS` | REAL | |
| `SV_DEBIT_ONUS` | REAL | On-us debit volume |
| `SV_DEBIT_OFFUS` | REAL | |
| `SV_CREDIT_OFFUS` | REAL | |
| `SV_QRIS_ONUS` | REAL | |
| `SV_QRIS_OFFUS` | REAL | |
| `FBI_DEBIT_ONUS` | REAL | On-us debit fee income |
| `FBI_DEBIT_OFFUS` | REAL | |
| `FBI_CREDIT_OFFUS` | REAL | |
| `FBI_QRIS_ONUS` | REAL | |
| `FBI_QRIS_OFFUS` | REAL | |

---

#### `PROCESSED_MONITORING` — 37 rows
PM-level annual monitoring aggregate. Written by `modules/monitoring.py`.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `PM` | TEXT | Portfolio Manager name |
| `YTD` | REAL | Year-to-date sales volume |

---

#### `PROCESSED_MONITORING_WEEKLY` — 345 rows
Weekly series in long format. Written by `modules/monitoring.py`. Used by Weekly Monitoring tab.

| Column | Type | Notes |
|--------|------|-------|
| `MERCHANT_GROUP` | TEXT | |
| `DIMENSI` | TEXT | Metric type: VOL / TRX / FBI |
| `PM` | TEXT | Portfolio Manager name |
| `FY` | REAL | Full-year target |
| `YTD` | REAL | Year-to-date cumulative |
| `W01`–`W53` | REAL | Weekly values, W01 = ISO week 1 |
| `YEAR` | TEXT | Calendar year (e.g., "2026") |

---

### Neon-Only Tables

#### `public.ingestion_runs`
Audit log of SQLite → Neon ingestion operations.

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | TEXT | PK (UUID) |
| `tables_ok` | INTEGER | Tables successfully ingested |
| `rows_loaded` | INTEGER | Total rows written |
| `source_filename` | TEXT | Original `.db` filename |
| `started_at` | TIMESTAMPTZ | |
| `completed_at` | TIMESTAMPTZ | |
| `status` | TEXT | `success` / `error` |
| `error_message` | TEXT | Null on success |

#### `public.master_files`
Binary storage for master Excel files.

| Column | Type | Notes |
|--------|------|-------|
| `file_key` | TEXT | PK: `master_mid` / `master_card` / `master_mon` |
| `filename` | TEXT | Original filename |
| `content` | BYTEA | Raw Excel bytes |
| `size_bytes` | BIGINT | |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() |

---

## ETL Pipeline (Local / Windows Only)

The 3-step ETL pipeline runs on Windows via `pages/00_Automated_Pipeline.py`.

```
staging.db (uploaded)
│
├── Query/1_fetch_mid_null.sql
│       ↓
│   modules/mid_cleaner.py
│       → Regex classify MID → Anchor/Group
│       → PROCESSED_MID table
│       → Updates master_mid.xlsx (via openpyxl)
│
├── Query/2_fetch_card_share.sql
│       ↓
│   modules/card_share.py
│       → Build monthly card-share matrix
│       → Excel COM write to master_card_share.xlsx
│       → PROCESSED_CARD_SHARE / _HISTORY / _MONTHLY tables
│
└── Query/3_fetch_weekly_series.sql
        ↓
    modules/monitoring.py
        → Merge weekly series into monitoring master
        → Excel COM write to master_monitoring.xlsx
        → PROCESSED_MONITORING / _WEEKLY tables
        → TARGET table (from T-sheets: 2025 T, 2026 T)
```

**Cloud pipeline:**
```
User uploads staging.db
    ↓
utils/sqlite_to_neon.py → ingest_sqlite_bytes_to_neon()
    → TRUNCATE + INSERT each table into Neon
    → Writes run record to public.ingestion_runs
```

---

## ML Engine

### K-Means++ Clustering (`services/ml_service.py` / `run_ml()`)

**Input:** `PROCESSED_CARD_SHARE` + `PROCESSED_MONITORING` + `TARGET`

**Features (6):**
| Feature | Computation |
|---------|-------------|
| `AVG_SV` | `log(1 + TOTAL_SV / months_active)` |
| `AVG_FBI` | `log(1 + TOTAL_FBI / months_active)` |
| `RASIO_ONUS` | On-us ratio clipped to [0, 1] |
| `SV_GROWTH_CLIPPED` | MoM growth rate, winsorised at 5th/95th percentile |
| `ACHIEVEMENT_PCT` | `(TOTAL_SV / TARGET_VOL_2026 × 100)`, clipped to [0, 200] |
| `WEEKS_ACTIVE` | Weeks with non-zero activity, clipped to [1, 52] |

All features standardised via `StandardScaler` before clustering.

**Cluster labels:** assigned by composite rank (not cluster ID), weighted:
- 60% average `AVG_SV`, 25% `ACHIEVEMENT_PCT`, 15% `SV_GROWTH_CLIPPED`

| K | Labels |
|---|--------|
| 3 | PREMIUM → REGULER → PASIF |
| 4 | ELITE → PREMIUM → REGULER → PASIF |
| 5 | ELITE → PREMIUM → REGULER → PASIF → DORMANT |

### Anomaly Detection

**Isolation Forest** (Liu et al. 2008):
- `n_estimators=100`, `contamination=0.10` (~10% of portfolio)
- LOFO (Leave-One-Feature-Out) contribution: ablate each feature → measure delta anomaly score

**Modified Z-Score (MAD):** `z = 0.6745 × (x − median) / MAD` — robust to small portfolios

**Composite Risk Score (0–100):**
```
RISK_SCORE = clip(-ZSCORE_GROWTH, 0, 3)/3 × 40   # Growth trend 40%
           + clip(-ZSCORE_SV,     0, 3)/3 × 30   # Volume anomaly 30%
           + clip(-ZSCORE_FBI,    0, 3)/3 × 20   # Fee anomaly 20%
           + clip(1 - ACH/100,    0, 1)  × 10   # Target gap 10%
```

**Churn risk tiers:**
- `≥ 60` → HIGH RISK
- `30–59` → MEDIUM RISK
- `< 30` → STABLE

### Holt-Winters Forecast (`hw_forecast()`)
- `< 24 months` data: Holt's Double Smoothing (trend only, no seasonal)
- `≥ 24 months` data: Holt-Winters (trend + additive seasonal, period=12)
- Falls back to linear extrapolation if model fails

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Cloud only | Neon PostgreSQL connection string — activates cloud mode |
| `DB_POOL_SIZE` | Optional | SQLAlchemy pool size (default: 5) |
| `DB_MAX_OVERFLOW` | Optional | Max overflow connections (default: 10) |
| `DB_POOL_TIMEOUT` | Optional | Pool timeout in seconds (default: 30) |
| `DB_POOL_RECYCLE` | Optional | Connection recycle interval in seconds (default: 1800) |

Set in `.env` (gitignored) locally or in the cloud platform's environment config.

---

## Known Constraints

1. **Windows-only ETL:** `mid_cleaner`, `card_share`, `monitoring` use `pywin32`/`win32com` for Excel COM. Only works on Windows. `is_pipeline_supported()` returns `False` on Linux/macOS/cloud.

2. **Ephemeral filesystem (cloud):** Cloud platforms reset the filesystem on redeploy. Never store important data only on disk in cloud mode — always persist to Neon. `utils/master_files_db.py` handles this automatically.

3. **Neon column casing:** All table names in Neon are lowercase (e.g., `processed_card_share`). The data service normalises column names to uppercase after every read.

4. **Timezone:** All UI timestamps display in **WIB (UTC+7)**. Use `datetime.fromtimestamp(mtime, tz=timezone(timedelta(hours=7)))` — never the bare `.fromtimestamp()`.

5. **PROCESSED_MONITORING_WEEKLY `YEAR` column:** stored as TEXT (e.g. `"2026"`), not INTEGER. Always cast with `.astype(str)` when filtering.

6. **DB freshness badges:** sidebar shows Fresh (<24 h), Aging (<72 h), Stale (>72 h) based on `APP_METADATA.LAST_DATA_UPDATE`.

---

*Built for Bank BTN Sidang Magang — Semester 6 · 2026*
