# 🚀 MASTER PROJECT HANDOVER: BTN Anchor ETL & Streamlit Pipeline
> **State:** Development/Validation
> **Last updated:** April 2026

## 🎯 PROJECT PURPOSE & ARCHITECTURE
An automated Streamlit application and ETL pipeline designed to ingest, classify, and visualize EDC and QRIS merchant transaction data. The pipeline aggregates raw weekly/monthly database dumps into clean Card Share and Weekly Monitoring trend metrics for top-tier "Anchor" merchants, running K-Means clustering for anomaly/churn analytics.

## 🗂️ CORE DIRECTORY (Inside `Project/`)

### 1. Data Extraction (SQL) - *WHERE WE LEFT OFF*
These queries extract raw metrics directly from the base database (`EDC_YYYYMM` & `QRIS_YYYYMM`). 
*Note: We recently optimized these by replacing massive static CASE statements with dynamic MySQL arithmetic intervals and manual date filters.*
* `1_fetch_mid_null.sql`: Scans monthly tables via `UNION ALL` for unclassified MIDs. Output feeds into `1_MID_Cleaner.py`.
* `2_fetch_card_share.sql`: Efficiently aggregates volumes and fees conditionally grouped by `PAYMENT_TYPE` (ONUS/OFFUS).
* `3_fetch_weekly_series.sql`: Pivots 7-day transaction intervals dynamically using `CEIL(DAYOFYEAR()/7.0)`.

### 2. Streamlit UI & Processing (`pages/`)
The UI heavily relies on `win32com.client` alongside pandas for safe, non-destructive Excel manipulation (retaining macros/formulas).
* `app.py`: Entry point emphasizing analytics and native theme toggling.
* `pages/0_Master_Configuration.py`: Global environment setup and master template provisioning.
* `pages/1_MID_Cleaner.py`: Regex-based categorization and duplicate resolution algorithm for unmapped MIDs.
* `pages/2_Card_Share_Processor.py`: Ingests the SQL Card Share output, merging it into the legacy Excel template securely.
* `pages/3_Monitoring_Processor.py`: Flattens the weekly SQL series against the master tracking template. Features a COM-level fix to override Excel formula bounds, preventing `#VALUE!` crashes.
* `pages/4_Dashboard.py`: Dynamic telemetry hub. Displays live Plotly clusters, unit-aware YoY/MoM KPIs, and Churn detection.
* `utils/theme.py`: Handles global CSS injection and dynamic UI palettes.

### 3. ML Analytics Engine (CLI Batch Scripts)
* `01_extract_and_clean.py`: Standalone CLI pipeline extractor.
* `02_transform_and_ml.py`: Scikit-Learn logic (K-Means++ K=3, Log-transform, Z-Score Anomaly & IQR churn detection).
* `03_load_to_datamart.py`: Finalizes reporting attributes (e.g., `TIER_LABEL`, `RISK_LABEL`).

### 4. Database & Storage (`database/` & `data/`)
* `database/`: Contains `staging.db` (intermediate SQLite cache) and structural scripts (`upgrade_*_table.py`).
* `data/master/`: The source-of-truth Excel templates (`master_mid.xlsx`, etc.). Features an automated `backups/` system that triggers before any destructive ETL writes.
* `data/raw/` & `data/testing/`: Dump folders for CSV outputs (from the SQL extracts) and historic test sheets.

## 🏁 NEXT ACTIONABLE STEPS
* **Current Checkpoint:** The base SQL extraction layer has been completely refactored. You now have 3 highly optimized `.sql` scripts capable of accurately summarizing raw base data into the exact schema the Streamlit app expects. 
* **To Do:** Execute these SQL scripts on your main database, export the resulting CSVs, and ingest them into the Streamlit pipeline (`1_MID_Cleaner.py` -> `2_Card_Share_Processor.py` -> `3_Monitoring_Processor.py`) to validate that the endpoints marry up perfectly.