# 🏦 BTN Anchor Merchant — Decision Intelligence Dashboard

> **Modernizing Bank BTN's Merchant Portfolio Management via automated ETL, Machine Learning, and interactive analytics.**

A full-stack merchant intelligence platform that automates the entire data lifecycle for Bank BTN's Anchor merchant portfolio — from raw Excel/SQL ingestion → multi-layer ML classification → weekly KPI monitoring → churn detection → interactive dashboard. Supports both **local Windows** and **cloud (Neon PostgreSQL)** deployment modes from a single codebase.

---

## ✨ Key Features

### 🚀 Automated ETL Pipeline
- **3-Step Orchestration**: Extract & Clean → ML Transform → Load to Datamart, runnable via the UI with live progress tracking.
- **Regex-Driven Classification**: Automatically identifies and normalizes Anchor merchant groups from raw MID lists.
- **Legacy Excel Integration**: Uses `win32com.client` (COM interface) to safely write to corporate Master files without destroying built-in formulas, pivots, or formatting.
- **Governance Gating**: Pipeline auto-detects new Anchors/PMs via delta comparison against the master PARAMETER sheet and blocks execution until they are approved.
- **Incremental Merge**: Only new rows are inserted on repeat runs — no duplicates.

### 🧠 Machine Learning Engine
- **K-Means++ Clustering** (3–5 configurable clusters): Segments merchants into **PREMIUM**, **REGULER**, **PASIF** (or **ELITE** / **DORMANT** at higher K) tiers based on Sales Volume, FBI, card-share ratio, and YTD achievement.
- **Churn & Risk Detection**: Multi-condition flagging using Z-Score (MAD), IQR, and Holt-Winters activity thresholds. Merchants are marked `HIGH RISK` when multiple signals align.
- **Anomaly Detection**: Modified Z-Score (MAD) + Isolation Forest for outlier identification across the weekly time series.
- **Live Re-computation**: ML runs in real-time on the dashboard for instant "what-if" portfolio analysis.

### 📊 Analytics Dashboard (7 Tabs)
- **Card Share**: YTD card-share leaderboard with YoY growth overlays and payment type breakdown.
- **Weekly Monitoring**: Heatmaps and trend charts with WoW/MoM growth indicators.
- **ML Segmentation**: Cluster scatter plots, feature importance, and silhouette diagnostics.
- **Churn & Risk**: Risk register with multi-factor flag explanations per merchant.
- **Merchant Explorer**: Drill-down per merchant with full weekly history and forecasting.
- **AI Insights**: Auto-generated portfolio commentary.
- **Batch Impact**: Before/after comparison of bulk reassignments.

### 🎨 Professional UI/UX
- **Dual-Mode Theming**: Dark **Navy & Gold** (BTN brand) and high-contrast **Warm Cream** light mode, toggled from the sidebar.
- **Dual-Mode Database**: Seamlessly switches between local SQLite and cloud Neon PostgreSQL based on a single environment variable.
- **PM Manager**: Inline data-editor for merchant reassignments, add/remove PMs, and a Danger Zone for safe PM removal with auto-reassignment.

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| **Language** | Python 3.10+ |
| **UI Framework** | Streamlit ≥ 1.36 |
| **Data Processing** | Pandas ≥ 2.0, NumPy ≥ 1.24 |
| **Machine Learning** | Scikit-Learn (K-Means++, StandardScaler), SciPy (Z-Score, IQR), Statsmodels (Holt-Winters) |
| **Visualisation** | Plotly ≥ 5.15, Matplotlib ≥ 3.7 |
| **Local Database** | SQLite 3 (stdlib) |
| **Cloud Database** | Neon PostgreSQL via SQLAlchemy ≥ 2.0 + psycopg2-binary |
| **Excel I/O** | openpyxl ≥ 3.1, pywin32 / win32com (Windows only) |
| **Scheduling** | Python `threading` (background pipeline execution) |

---

## 🏗️ Architecture

```mermaid
graph TD
    subgraph Input
        A1[Master MID Excel]
        A2[Card Share Excel]
        A3[Monitoring Excel]
    end

    subgraph ETL["ETL Pipeline (3 Steps)"]
        B1["Step 1 — Extract & Clean\n(01_extract_and_clean.py)"]
        B2["Step 2 — ML Transform\n(02_transform_and_ml.py)"]
        B3["Step 3 — Load to Datamart\n(03_load_to_datamart.py)"]
    end

    subgraph DB["Database Layer"]
        C1[(SQLite staging.db\nLocal Mode)]
        C2[(Neon PostgreSQL\nCloud Mode)]
    end

    subgraph App["Streamlit App"]
        D1[📊 Dashboard\n7 Analytics Tabs]
        D2[🚀 Automated Pipeline]
        D3[⚙️ Master Configuration]
        D4[✏️ Data Editor]
        D5[👥 PM Manager]
    end

    A1 & A2 & A3 --> B1 --> B2 --> B3
    B3 --> C1
    B3 --> C2
    C1 & C2 --> D1 & D2 & D3 & D4 & D5
```

---

## 📄 Pages

| Page | Description |
|------|-------------|
| **📊 Dashboard** | Main analytics hub. Card Share leaderboard, weekly monitoring heatmaps, ML cluster visualisations, churn risk register, per-merchant drill-down, and AI-generated insights. |
| **🚀 Automated Pipeline** | ETL orchestrator. Runs the 3-step pipeline locally (full Excel COM mode) or handles SQLite → Neon ingestion in cloud mode. Includes governance delta detection and live step progress. |
| **⚙️ Master Configuration** | Upload and manage the three master Excel files (MID, Card Share, Monitoring). Files persist to Neon BYTEA storage in cloud mode and are synced to disk on session start. |
| **✏️ Data Editor** | CRUD interface for merchant classification data. Edit MID master, card-share matrix, or monitoring pivots directly in an in-page spreadsheet view. |
| **👥 PM Manager** | Portfolio Manager assignment interface. Inline data-editor for quick reassignments, form to add new PM–merchant pairs, and a collapsible Danger Zone to remove a PM and safely reassign their merchants. |

---

## 🗄️ Database Schema

### Staging Tables (written by ETL Step 1)
| Table | Key Columns |
|-------|-------------|
| `CARD_SHARE` | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, payment type columns |
| `WEEKLY_MONITOR` | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, `PM`, weekly metrics |
| `TARGET` | `MERCHANT_GROUP`, `PM`, `TARGET_VOL_2026` |
| `APP_METADATA` | `LAST_DATA_UPDATE`, `NEW_DATA_SIGNAL` |

### Processed Tables (written by ETL Step 2–3)
| Table | Description |
|-------|-------------|
| `PROCESSED_MID` | Regex-classified merchant → anchor group mapping |
| `PROCESSED_CARD_SHARE` | YTD totals per merchant group |
| `PROCESSED_CARD_HISTORY` | Monthly historical data for YoY growth |
| `PROCESSED_CARD_MONTHLY` | Monthly breakdown by payment type |
| `PROCESSED_MONITORING` | PM-level annual aggregate |
| `PROCESSED_MONITORING_WEEKLY` | Weekly series in long format |

### Cloud-Only Tables (Neon)
| Table | Description |
|-------|-------------|
| `public.master_files` | BYTEA blobs of the three master Excel files |
| `public.ingestion_runs` | Audit log of SQLite → Neon ingestion runs |

---

## 🚀 Getting Started

### Local Deployment (Windows)

**Prerequisites:**
- Windows OS (required for `win32com` Excel automation)
- Microsoft Excel installed
- Python 3.10+

```bash
# Clone the repository
git clone https://github.com/marvelkn/AnchorAutomationDashboard.git
cd AnchorAutomationDashboard/Project

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run Home.py
```

The app opens at `http://localhost:8501`. On first launch with no database, it will guide you through uploading the three master Excel files via the **Master Configuration** page to initialise `staging.db`.

---

### Cloud Deployment (Neon PostgreSQL)

1. Provision a [Neon](https://neon.tech) PostgreSQL database and copy the connection string.
2. Set the environment variable:

```bash
DATABASE_URL=postgresql://user:password@host/dbname
```

3. Deploy to Streamlit Cloud, Heroku, or any container platform.
4. On first launch, use **Master Configuration** to upload the master Excel files — they are persisted as BYTEA in Neon and synced to disk automatically on each session start.
5. Use **Automated Pipeline → Cloud Ingestion** to push a local `staging.db` to Neon.

> **Note:** In cloud mode the full 3-step Excel COM pipeline is disabled. Only SQLite → Neon ingestion is available. All dashboard and PM Manager features work identically.

---

## 🔑 Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Cloud only | — | Neon PostgreSQL connection string. Setting this activates cloud mode. |
| `DB_POOL_SIZE` | No | `5` | SQLAlchemy connection pool size |
| `DB_MAX_OVERFLOW` | No | `10` | Max overflow connections |
| `DB_POOL_TIMEOUT` | No | `30` | Pool acquire timeout (seconds) |
| `DB_POOL_RECYCLE` | No | `1800` | Connection recycle interval (seconds) |

For local development, place these in `.streamlit/secrets.toml`:
```toml
DATABASE_URL = "postgresql://..."
```

---

## 🤖 ML Engine Details

### K-Means++ Clustering
| Feature | Transformation |
|---------|---------------|
| Average Sales Volume | `log1p` |
| Average Fee-Based Income | `log1p` |
| On-Us Card Ratio | Raw |
| SV Growth Rate | Clipped at ±300% |
| YTD Achievement % | Raw |
| Weeks Active | Raw |

All features are normalised with `StandardScaler` before clustering. Cluster labels are rank-assigned by mean SV (not hardcoded to cluster ID), so labels stay stable as data changes.

### Churn Risk Flags
A merchant is marked **HIGH RISK** if **any** of the following are true:
- `WEEKS_ACTIVE ≤ 2` (near-zero activity)
- `SV_GROWTH_RATE ≤ −95%` AND `ACHIEVEMENT_PCT < 5%`
- Cluster is PASIF/DORMANT AND `ACHIEVEMENT_PCT < 1%`
- `Z-Score(SV) < −1.5`, `Z-Score(FBI) < −1.5`, or `Z-Score(Growth) < −1.5`

---

## 🗂️ Repository Structure

```
AnchorAutomationDashboard/
├── Project/
│   ├── Home.py                        # App entry point & navigation
│   ├── pages/
│   │   ├── 00_Automated_Pipeline.py   # ETL orchestrator
│   │   ├── 0_Master_Configuration.py  # Master file management
│   │   ├── 01_Data_Editor.py          # CRUD data editor
│   │   ├── 4_Dashboard.py             # Main analytics (7 tabs)
│   │   └── 05_PM_Manager.py           # PM assignment management
│   ├── utils/
│   │   ├── theme.py                   # Design system & CSS injection
│   │   ├── db_connector.py            # SQLite query helpers
│   │   ├── db_merger.py               # Incremental merge logic
│   │   ├── cloud_db.py                # Neon engine builder & upsert helpers
│   │   ├── sqlite_to_neon.py          # SQLite → Neon ingestion
│   │   ├── master_files_db.py         # Neon BYTEA file persistence
│   │   ├── governance.py              # Governance delta detection
│   │   ├── pipeline_bg.py             # Background thread manager
│   │   └── backup_manager.py          # File versioning & restore
│   ├── scripts/
│   │   ├── 01_extract_and_clean.py    # ETL Step 1
│   │   ├── 02_transform_and_ml.py     # ETL Step 2 (ML)
│   │   └── 03_load_to_datamart.py     # ETL Step 3
│   ├── database/
│   │   └── staging.db                 # Local SQLite (gitignored)
│   ├── data/master/                   # Master Excel files (gitignored)
│   └── requirements.txt
└── README.md
```

---

*Created as part of the UMN Semester 6 Internship Program (Magang Materi Sidang).*
