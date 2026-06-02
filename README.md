# 🏦 BTN Anchor Merchant — Decision Intelligence Dashboard

> **Modernizing Bank BTN's Merchant Portfolio Management via automated ETL, Machine Learning, and interactive analytics.**

A merchant intelligence platform for Bank BTN's Anchor merchant portfolio — from raw
Excel/SQL ingestion → multi-layer ML classification → weekly KPI monitoring → churn
detection → interactive dashboard. The interactive app runs on **Neon PostgreSQL (cloud)**;
an optional **Windows-only Excel-COM ETL** prepares a SQLite extract that is ingested into Neon.

---

## ✨ Key Features

### 🚀 Automated Pipeline
- **Cloud Ingestion**: Uploads a SQLite extract and upserts it into Neon (incremental — only new rows are inserted on repeat runs, no duplicates).
- **Master-File Pre-flight Gate**: Ingestion is blocked until the three required master files (MID, Card Share, Monitoring) are present in Neon.
- **Governance Gating**: Auto-detects new Anchors/PMs via delta comparison against the master `PARAMETER` sheet and blocks execution until they are approved.
- **Excel-COM ETL (Windows)**: Uses `win32com.client` to write to corporate Master files without destroying built-in formulas, pivots, or formatting.

### 🧠 Machine Learning Engine
- **K-Means++ Clustering** (fixed **K=3**, locked per academic review): segments merchants into **PREMIUM**, **REGULER**, **PASIF** tiers from Sales Volume, FBI, on-us card ratio, growth, YTD achievement, and weeks active. Tiers are rank-assigned by a composite score, so labels stay stable as data changes.
- **Composite Risk Score (0–100)**: weighted Growth 40% · Volume 30% · FBI 20% · Achievement 10%, bucketed into `HIGH RISK` (≥60) / `MEDIUM RISK` (30–59) / `STABLE` (<30).
- **Anomaly Detection**: Modified Z-Score (MAD, robust to small-portfolio outliers) plus Isolation Forest with leave-one-feature-out contributions; a MAD z-score breach upgrades a `STABLE` merchant to `MEDIUM RISK`.
- **Forecasting**: damped-trend Holt-Winters on monthly Settlement Volume with an 80% confidence band.
- **Cached Re-computation**: ML recomputes on the dashboard whenever the underlying data changes (keyed on `LAST_DATA_UPDATE`), and is cached between reruns.

### 📊 Analytics Dashboard (7 Tabs)
- **Card Share**: YTD card-share leaderboard with YoY growth overlays and payment type breakdown.
- **Weekly Monitoring**: Heatmaps and trend charts with WoW/MoM growth indicators.
- **ML Segmentation**: Cluster scatter (PCA 2-D), composite ranking, silhouette & Davies-Bouldin diagnostics.
- **Churn & Risk**: Risk register with multi-factor flag explanations per merchant.
- **Merchant Explorer**: Drill-down per merchant with full weekly history and Holt-Winters forecasting.
- **AI Insights**: Auto-generated portfolio commentary.
- **Batch Impact**: Before/after comparison of bulk reassignments.

### 🎨 Professional UI/UX
- **Dual-Mode Theming**: Dark **Navy & Gold** (BTN brand) and high-contrast light mode, toggled from the sidebar; responsive layout with a mobile bottom-nav.
- **Read-Only Snapshot Tier**: if Neon is briefly unreachable mid-session, the dashboard serves the last good load from a local pickle snapshot so it stays online.
- **PM Manager**: Inline data-editor for merchant reassignments, add/remove PMs, and a Danger Zone for safe PM removal with auto-reassignment.

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| **Language** | Python 3.10+ |
| **UI Framework** | Streamlit ≥ 1.36 |
| **Data Processing** | Pandas ≥ 2.0, NumPy ≥ 1.24 |
| **Machine Learning** | Scikit-Learn (K-Means++, StandardScaler, PCA, Isolation Forest), SciPy, Statsmodels (Holt-Winters) |
| **Visualisation** | Plotly ≥ 5.15, Matplotlib ≥ 3.7 |
| **Cloud Database** | Neon PostgreSQL via SQLAlchemy ≥ 2.0 + psycopg2-binary |
| **Ingestion / ETL staging** | SQLite 3 (stdlib) extract, ingested into Neon |
| **Excel I/O** | openpyxl ≥ 3.1, pywin32 / win32com (Windows-only ETL) |

---

## 🏗️ Architecture

```mermaid
graph TD
    subgraph Input
        A1[Master MID Excel]
        A2[Card Share Excel]
        A3[Monitoring Excel]
    end

    subgraph ETL["Excel-COM ETL (Windows, optional)"]
        B1["Clean & classify\n(modules/: mid_cleaner, card_share, monitoring)"]
        B2["SQLite extract\n(staging.db)"]
    end

    subgraph Ingest["Ingestion"]
        I1["sqlite_to_neon\n(upsert into Neon)"]
    end

    subgraph DB["Datamart"]
        C2[(Neon PostgreSQL)]
    end

    subgraph App["Streamlit App (Neon-only)"]
        D1[📊 Dashboard\n7 Analytics Tabs]
        D2[🚀 Automated Pipeline]
        D3[⚙️ Master Configuration]
        D4[✏️ Data Editor]
        D5[👥 PM Manager]
    end

    A1 & A2 & A3 --> B1 --> B2 --> I1 --> C2
    C2 --> D1 & D2 & D3 & D4 & D5
```

---

## 📄 Pages

| Page | Description |
|------|-------------|
| **📊 Dashboard** | Main analytics hub. Card Share leaderboard, weekly monitoring heatmaps, ML cluster visualisations, churn risk register, per-merchant drill-down, and AI-generated insights. |
| **🚀 Automated Pipeline** | Cloud ingest UI. Validates required master files in Neon, ingests a SQLite extract into Neon, and exposes maintenance (scrub / VACUUM / reset) plus an ingestion audit log. |
| **⚙️ Master Configuration** | Upload and manage the three master Excel files (MID, Card Share, Monitoring). Files persist to Neon BYTEA storage and are synced to disk on session start. |
| **✏️ Data Editor** | CRUD interface for merchant classification data. Edit MID master, card-share matrix, or monitoring pivots directly in an in-page spreadsheet view. |
| **👥 PM Manager** | Portfolio Manager assignment interface. Inline data-editor for quick reassignments, form to add new PM–merchant pairs, and a collapsible Danger Zone to remove a PM and safely reassign their merchants. |

---

## 🗄️ Database Schema

### Staging Tables (in the SQLite extract, ingested into Neon)
| Table | Key Columns |
|-------|-------------|
| `CARD_SHARE` | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, payment type columns |
| `WEEKLY_MONITOR` | `EDW_FETCH_DATE`, `MERCHANT_GROUP`, `PM`, weekly metrics |
| `TARGET` | `MERCHANT_GROUP`, `PM`, `TARGET_VOL_2026` |
| `APP_METADATA` | `LAST_DATA_UPDATE`, `NEW_DATA_SIGNAL` |

### Processed Tables (read by the dashboard)
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

The interactive app is **Neon-only** — it requires `DATABASE_URL` and will refuse to start
without it. Producing the SQLite extract is a separate, optional Windows step (below).

### Run the app (any OS)

```bash
# Clone the repository
git clone https://github.com/marvelkn/AnchorAutomationDashboard.git
cd AnchorAutomationDashboard

# Install dependencies
pip install -r requirements.txt

# Point at your Neon database (required)
export DATABASE_URL="postgresql://user:password@host/dbname"   # Windows: set DATABASE_URL=...

# Run the app
streamlit run app.py
```

The app opens at `http://localhost:8501`. On first launch, use **Master Configuration** to
upload the three master Excel files (persisted as BYTEA in Neon), then **Automated Pipeline**
to ingest a SQLite extract.

### (Optional) Windows Excel-COM ETL

To regenerate the SQLite extract from the corporate Master files you need a Windows host with
Microsoft Excel installed (required for `win32com` automation). The cleaning/classification
logic lives in `modules/` (`mid_cleaner`, `card_share`, `monitoring`).

> **Note:** The full Excel-COM ETL runs only on Windows. The dashboard, ingestion, and PM
> Manager features all run on any platform against Neon.

### Testing

```bash
pip install -r tests/requirements-dev.txt
pytest -q
```

Tests are hermetic (no live Neon required) — DB-backed suites use a temporary SQLite engine,
and the ML/forecast suites run on synthetic in-memory data.

---

## 🔑 Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | **Yes** | — | Neon PostgreSQL connection string. The app refuses to start without it. |
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

Implemented in `utils/ml_engine.py` (pure, unit-tested; the dashboard wraps it with caching).

### K-Means++ Clustering (fixed K=3)
| Feature | Transformation |
|---------|---------------|
| Average Sales Volume | `log1p` |
| Average Fee-Based Income | `log1p` |
| On-Us Card Ratio | Raw (clipped 0–1) |
| SV Growth Rate | Winsorized to the 5th–95th percentile |
| YTD Achievement % | Raw (clipped 0–200) |
| Weeks Active | Raw |

All features are normalised with `StandardScaler` before clustering. Cluster labels
(PREMIUM / REGULER / PASIF) are rank-assigned by a composite score (SV 60%, achievement 25%,
growth 15%), so labels stay stable as data changes.

### Composite Risk Score & Churn Tiers
A 0–100 risk score is computed from MAD-robust z-scores:

```
RISK = 40·clip(−z_growth) + 30·clip(−z_SV) + 20·clip(−z_FBI) + 10·(1 − achievement%)
```

Tiers: **HIGH RISK** ≥ 60 · **MEDIUM RISK** 30–59 · **STABLE** < 30. Any MAD z-score below
the breach threshold (`Z_THRESH = −1.2`) upgrades a `STABLE` merchant to `MEDIUM RISK`.
Isolation Forest provides an independent multivariate anomaly signal.

---

## 🗂️ Repository Structure

```
AnchorAutomationDashboard/
├── app.py                          # App entry point & navigation
├── pages/
│   ├── 00_Automated_Pipeline.py    # Cloud ingest UI + governance gate
│   ├── 0_Master_Configuration.py   # Master file management
│   ├── 01_Data_Editor.py           # CRUD data editor
│   ├── 4_Dashboard.py              # Main analytics (7 tabs)
│   └── 05_PM_Manager.py            # PM assignment management
├── modules/                        # Excel-COM ETL cleaning/transform logic
│   ├── mid_cleaner.py
│   ├── card_share.py
│   └── monitoring.py
├── utils/
│   ├── theme.py                    # Design system & CSS injection
│   ├── ml_engine.py                # run_ml + hw_forecast (pure ML core)
│   ├── cloud_db.py                 # Neon engine builder & upsert helpers
│   ├── sqlite_to_neon.py           # SQLite → Neon ingestion
│   ├── master_files_db.py          # Neon BYTEA file persistence
│   ├── governance.py               # Governance delta detection & write-back
│   ├── growth_analytics.py         # Growth metrics & action-inbox helpers
│   ├── formatting.py               # IDR / count / growth formatters
│   ├── app_state.py                # User-state side tables (triage, watchlist)
│   ├── rate_limiter.py             # Session rate limiting & pipeline cooldown
│   ├── i18n.py                     # Localisation helpers
│   └── backup_manager.py           # File versioning & restore
├── tests/                          # pytest suites (see Testing above)
├── scripts/
│   └── fix_data_quality_w21.py     # One-off data-quality maintenance script
├── data/master/                    # Master Excel files (gitignored)
├── data/snapshot/                  # Read-only fallback snapshots (gitignored)
└── requirements.txt
```

---

*Created as part of the UMN Semester 6 Internship Program (Magang Materi Sidang).*
