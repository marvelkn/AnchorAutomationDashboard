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
- **Database**: SQLite (Staging Cache) & MySQL (Production Source)
- **Office Automation**: [Win32COM](https://pypi.org/project/pywin32/) (Safe Excel manipulation for macro-enabled templates)
- **Visuals**: [Plotly](https://plotly.com/python/) (Interactive charts), Matplotlib (ETL Diagnostics)

---

## 🗂️ DIRECTORY ARCHITECTURE

### 1. Root Orchestration (ETL Pipeline)
The core logic is split into a 3-step sequential pipeline for modularity and error isolation:
*   `01_extract_and_clean.py`: **Ingestion Layer.** Loads raw Excel dumps into a structured SQLite staging database (`staging.db`).
*   `02_transform_and_ml.py`: **Intelligence Layer.** Conducts feature engineering, K-Means clustering (K=3), and Z-Score/IQR anomaly detection.
*   `03_load_to_datamart.py`: **Reporting Layer.** Generates business-ready labels (TIER, RISK, GROWTH) and produces the final `Data_Mart_Ready.csv`.
*   `app.py`: The entry point for the Streamlit UI. Handles routing, authentication-lite via environment modes, and global theme injection.
*   `AnchorData.ipynb`: Shared research notebook for exploratory data analysis (EDA).

### 2. UI Modules (`pages/`)
*   `00_Automated_Pipeline.py`: Dashboard for triggering the full ETL pipeline with real-time status updates (via `pipeline_bg.py`).
*   `01_Data_Editor.py`: CRUD interface for correcting mapped merchant attributes and resolving MID naming conflicts.
*   `05_PM_Manager.py`: Specialized interface for managing Portfolio Manager assignments and merchant grouping.
*   `0_Master_Configuration.py`: Global environment settings (API endpoints, directory paths, and threshold tuning).
*   `4_Dashboard.py`: The "Mission Control" visualization hub. Features YoY/MoM KPIs, Plotly cluster maps, and Churn Risk alerts.

### 3. Logic & Utilities
*   `modules/`: Contains domain-specific logic (`card_share.py`, `monitoring.py`, `mid_cleaner.py`) used by the processor pages.
*   `utils/`: 
    *   `theme.py`: Custom CSS and dynamic color palettes (Dark/Light/Gold).
    *   `db_connector.py / db_merger.py`: Abstraction layers for SQLite and MySQL operations.
    *   `pipeline_bg.py`: Manages background thread execution for long-running ETL tasks.
    *   `backup_manager.py`: Automated Excel backup system that triggers before processing runs.

### 4. Data & Query Layer
*   `Query/`: Optimized SQL scripts (`1_fetch_mid_null.sql`, etc.) for extracting raw metrics from the production database.
*   `database/`: Contains `staging.db` (the heartbeat of the app).
*   `data/`: Organized into `master/` (Excel templates), `raw/` (SQL exports), and `backups/`.
*   `output/`: Automated exports from the ML engine, including `Data_Mart_Ready.csv` and diagnostic plots (Elbow method, etc.).

---

## 🔄 DATA LIFE CYCLE (ETL FLOW)

1.  **Extract**: Raw transaction data is pulled from MySQL via scripts in `Query/` and saved as CSVs in `data/raw/`.
2.  **Clean**: `01_extract_and_clean.py` handles MID standardization, removes duplicates, and pivots weekly wide-data into long-format.
3.  **Engine**: `02_transform_and_ml.py` builds 6 features (Monthly SV/FBI, On-Us Ratio, Growth, Achievement %, and Active Weeks). It clusters merchants using K-Means++.
4.  **Label**: `03_load_to_datamart.py` assigns `PREMIUM`, `REGULER`, or `PASIF` tiers and flags `CHURN RISK` based on multi-kriteria anomaly scoring.
5.  **Visualize**: The dashboard consumes the final Data Mart for end-user storytelling.

---

## 🧠 ML ENGINE & ANOMALY LOGIC

### K-Means Clustering (K=3)
Segments merchants into actionable tiers:
- **PREMIUM**: Top 10-15% by volume. Strategic partners for the bank.
- **REGULER**: The backbone. Merchants showing steady growth potential.
- **PASIF**: Potential churn or low-activity merchants requiring PM intervention.

### Churn Risk Detection
Uses a **Dual-Threshold** system:
- **Z-Score**: Detects statistical outliers in Growth Rate and Log-transformed SV.
- **IQR (Interquartile Range)**: Identifies performers falling below the lower fence of their peer group.
- **Business Rule**: Any merchant active for < 2 weeks or with -99% growth is automatically flagged for immediate review.

---

## ⚡ MAINTENANCE NOTES
- **Cleaning Data**: If MIDs are missing classifications, use `01_Data_Editor.py` to re-sync them.
- **Updating Templates**: Always update the files in `data/master/`. The app will auto-backup the old version before writing changes.
- **DB Health**: If the dashboard feels laggy, check the DB Status in the `app.py` sidebar. Stale databases (>72h) should be re-processed via the `Automated Pipeline`.

---
*Created with ❤️ for Bank BTN Sidang Magang.*