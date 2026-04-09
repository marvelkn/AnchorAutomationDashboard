# 🚀 MASTER PROJECT HANDOVER: BTN Anchor Intelligence Platform
> **State:** Production-Ready / Maintenance Mode  
> **Last Updated:** April 2026

## 🎯 PROJECT PURPOSE
The **Anchor Intelligence Platform** is a data-driven merchant management system built for Bank BTN. It automates the lifecycle of "Anchor" (top-tier) merchant data—from raw SQL/Excel ingestion to advanced ML clustering and churn prediction. The platform empowers Portfolio Managers (PMs) to identify high-performing merchants, detect anomalies, and prevent churn through a centralized Streamlit dashboard.

---

## 🛠️ CORE TECHNOLOGY STACK
- **Backend & Logic**: Python 3.10+
- **Frontend / UI**: [Streamlit](https://streamlit.io/) (Native Navigation & Theming)
- **Data Engineering**: [Pandas](https://pandas.pydata.org/), [NumPy](https://numpy.org/)
- **Machine Learning**: [Scikit-Learn](https://scikit-learn.org/) (K-Means++, StandardScaler)
- **Database**: SQLite (Staging Cache), MySQL (Production Source), & Neon PostgreSQL (Cloud Sync)
- **Office Automation**: [Win32COM](https://pypi.org/project/pywin32/) (Safe Excel manipulation for macro-enabled templates)
- **Visuals**: [Plotly](https://plotly.com/python/) (Interactive charts), Matplotlib (ETL Diagnostics)

---

## 🗂️ DIRECTORY ARCHITECTURE

### 1. Root Orchestration (ETL Pipeline)
The core logic is split into a 3-step sequential pipeline for modularity and error isolation:
*   `01_extract_and_clean.py`: **Ingestion Layer.** Loads raw Excel dumps into a structured SQLite staging database (`staging.db`). Handles MID standardization, removes duplicates, and pivots weekly wide-data into long-format.
*   `02_transform_and_ml.py`: **Intelligence Layer.** Conducts feature engineering (6 features), K-Means clustering (K=3), and Dual-Method Anomaly Detection (Z-Score + IQR).
*   `03_load_to_datamart.py`: **Reporting Layer.** Generates business-ready labels (TIER, RISK, GROWTH), produces the final `Data_Mart_Ready.csv`, and builds PM-specific summaries.
*   `app.py`: Entry point for the Streamlit UI. Implements high-end navigation, custom sidebar branding, and environmental mode selection (Production/Staging).

### 2. UI Modules (`pages/`)
Each page follows the project's premium design system (`utils/theme.py`):
*   `4_Dashboard.py`: **Mission Control.** Interactive hub for YoY/MoM KPIs, Plotly cluster maps, and Churn Risk alerts.
*   `00_Automated_Pipeline.py`: Orchestrator for triggering the full ETL pipeline with real-time background tracking.
*   `01_Data_Editor.py`: CRUD interface for correcting mapped merchant attributes and resolving MID naming conflicts.
*   `05_PM_Manager.py`: Specialized interface for managing Portfolio Manager assignments and merchant grouping.
*   `0_Master_Configuration.py`: Global settings for API endpoints, directory paths, and threshold tuning.

### 3. Specialized Logic Modules (`modules/`)
Encapsulates domain-specific complex operations:
*   `mid_cleaner.py`: **Classification Engine.** Uses a multi-step regex pipeline to automatically categorize merchants into ANCHOR or RETAIL groups and merge new data with master records.
*   `card_share.py`: **Transaction Aggregator.** Manages monthly metrics. Includes `win32com` integration to append data to macro-enabled Excel templates and enforces idempotency via KEY-based deduplication.
*   `monitoring.py`: **Weekly Tracker.** Merges weekly series data into master Excel files and extracts multi-year trends (2024-2026) for analysis.

### 4. Utilities (`utils/`)
*   `theme.py`: **Design System.** Defines the Blue/Gold/Grey palette, apply global CSS, and handles Light/Dark mode transitions.
*   `pipeline_bg.py`: **Async Manager.** Handles background thread execution for long-running ETL tasks, preventing UI freezes.
*   `db_connector.py / db_merger.py`: Abstractions for SQLite and MySQL operations.
*   `backup_manager.py`: Automated Excel versioning system.
*   `cloud_db.py / sqlite_to_neon.py`: **Cloud Sync Infrastructure.** Enables seamless syncing of the local SQLite staging database to a Neon PostgreSQL cloud instance for remote reporting or backup.

### 5. Data & Query Layer
*   `Query/`: SQL scripts (`1_fetch_mid_null.sql`, etc.) for metrics extraction.
*   `database/`: Contains `staging.db` (the heartbeat of the app).
*   `data/`: Organized into `master/` (Excel templates), `raw/` (SQL exports), and `backups/`.
*   `static/`: Graphic assets (e.g., `btn_logo.png`).
*   `output/`: Final CSV exports (`Data_Mart_Ready.csv`) and ML diagnostic plots.

---

## 🔄 DATA LIFE CYCLE (ETL FLOW)
1.  **Extract**: Raw transaction data is pulled from MySQL via scripts in `Query/` and saved as CSVs in `data/raw/`.
2.  **Clean**: `01_extract_and_clean.py` & `mid_cleaner.py` standardize MIDs and merge them into the master repository.
3.  **Engine**: `02_transform_and_ml.py` builds 6 features (Monthly SV/FBI, On-Us Ratio, Growth, Achievement %, and Active Weeks). It clusters merchants using K-Means++.
4.  **Label**: `03_load_to_datamart.py` assigns `PREMIUM`, `REGULER`, or `PASIF` tiers and flags `CHURN RISK` based on anomaly scoring.
5.  **Visualize**: The dashboard consumes the final Data Mart for end-user storytelling.

---

## 🧠 ML ENGINE & ANOMALY LOGIC
### K-Means Clustering (K=3)
- **PREMIUM**: Strategic partners showing high volume and steady growth.
- **REGULER**: Backbone merchants with scalability potential.
- **PASIF**: Low-activity merchants requiring PM intervention.

### Churn Risk Detection (Dual-Method)
- **Z-Score**: Detects statistical outliers in Growth Rate and Log-transformed SV.
- **IQR (Interquartile Range)**: Identifies performers falling below the lower fence of their peer group.
- **Business Rule Flags**: Automatic alerts for merchants with < 2 weeks of activity or -99% growth.

---

## ⚡ MAINTENANCE NOTES
- **DB Health**: Monitor the sidebar status; stale databases (>72h) should be re-processed via the `Automated Pipeline`.
- **Excel Sync**: Ensure `pywin32` is installed for `win32com` operations. The app automatically creates backups before any write operation.
- **Cloud Sync**: Use `sqlite_to_neon.py` to push local staging data to the cloud when database environment variables are configured.

---
*Created with ❤️ for Bank BTN Sidang Magang.*