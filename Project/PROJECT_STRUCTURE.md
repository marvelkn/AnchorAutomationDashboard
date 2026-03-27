# 🚀 MASTER PROJECT HANDOVER — BTN ETL, ML & Streamlit Pipeline
> **Focus:** Technical Architecture, Codebase, Database Structure, and Machine Learning
> **Last updated:** March 2026 (Reflects updated Streamlit structure)

## 🔧 TECH STACK
* **Data Processing:** Python, Pandas, Regular Expressions (Regex)
* **Machine Learning:** Scikit-Learn (K-Means++), SciPy, NumPy (Z-Score + IQR)
* **Database Layer:** SQLite (Built-in Python)
* **Visualization/UI:** Streamlit, Plotly
* **Excel Manipulation:** `win32com.client` (COM-interface for non-destructive Excel appending)

## 🗂️ PROJECT DIRECTORY STRUCTURE
**Root (`Materi Sidang/`)**
* `CARD_SHARE_MERCHANT_ANCHOR_2026.xlsx` & `Monitoring_Weekly_Anchor_2026.xlsx` (Raw data)
* `SQL.txt` / `SQL.xlsx` (Exploratory scripts)
* `Project/` (Main Application Directory)

**Inside `Project/`**
* `AnchorData.ipynb`: Active Jupyter Notebook for EDA and prototyping.
* `setup_database.py`: Foundational DDL script creating the star schema in `btn_anchor.db` and staging tables.
* `01_extract_and_clean.py`: ETL Step 1 (Regex matching, cleaning, long-format pivoting).
* `02_transform_and_ml.py`: ETL Step 2 (Log transform, scaling, K-Means++, Z-Score/IQR).
* `03_load_to_datamart.py`: ETL Step 3 (Schema validation, tier labeling, YoY trends, PM summaries).
* `app.py`: Main Streamlit application entry point.
* **`pages/`** (Streamlit multipage apps):
  * `1_MID_Cleaner.py`: Regex extraction and duplicate resolution.
  * `2_Card_Share_Processor.py`: Excel ingestion using `win32com.client` to retain formulas/charts.
  * `3_Monitoring_Processor.py`: Weekly matrix restructurer.
  * `4_Dashboard.py`: Dynamic telemetry hub, live K-Means clustering, and anomaly alerts.
* **`utils/`**: 
  * `theme.py`: Centralized UI/UX engine managing Dark/Light mode palettes, dynamic CSS injection, and Plotly chart theming.
* **`database/`**: `btn_anchor.db` (Target) and `staging.db` (Intermediate).
* **`data/`**: Subfolders `raw/` and `real/` for hard-coded source files. Subfolder `master/` for tracking Master Excel templates.
* **`output/`**: Generates `checkpoint_01_clean.csv`, `checkpoint_02_ml.csv`, `Data_Mart_Ready.csv`, `Summary_PM.csv`, and evaluation charts.

## 🏗️ ETL PIPELINE ARCHITECTURE (Python scripts)
1. **Extract & Clean (`01_extract_and_clean.py`):**
   * Applies regex to classify Anchor vs Retail merchants on the ALL MID List.
   * Extracts data using the Anchor list as the JOIN key (using `MERCHANT_GROUP`, not MID).
   * Standardizes text, pivots wide-to-long, aggregates totals (ONUS, OFFUS, CREDIT, QRIS).
2. **Transform & ML (`02_transform_and_ml.py`):**
   * Generates 6 features (AVG_SV, AVG_FBI, RASIO_ONUS, SV_GROWTH, ACHIEVEMENT_PCT, WEEKS_ACTIVE).
   * Applies `log1p` to AVG_SV and AVG_FBI, then `StandardScaler`.
   * Runs K-Means++ (K=3, n_init=50).
   * Runs anomaly detection (Z-Score < -1.2 + IQR criteria).
3. **Load (`03_load_to_datamart.py`):**
   * Enriches data with `TIER_LABEL`, `GROWTH_STATUS`, `RISK_LABEL`.
   * Outputs `Data_Mart_Ready.csv` (utf-8-sig) and `Summary_PM.csv`.

## 📊 MACHINE LEARNING METRICS & RESULTS
* **Algorithm:** K-Means++ (K=3)
* **Metrics:** Silhouette (0.3253), Davies-Bouldin (1.0969), Calinski-Harabasz (18.39).
* **Clusters:**
  * PREMIUM (13 merchants, Avg SV 9.2B, e.g., MAP GROUP, INDOMARET)
  * REGULER (20 merchants, Avg SV 264M)
  * PASIF (5 merchants, Avg SV 1M, e.g., SUSHI TEI, POPEYES)
* **Churn Logic (OR logic):** WEEKS_ACTIVE ≤ 2 OR (Growth ≤ -99% AND Achievement < 5%) OR (PASSIVE AND Achievement < 1%) OR ZSCORE_SV < -1.2.
* **Churn Detection Results:** 6 merchants flagged (Sushi Tei, Kimia Farma, Hokben, Banban Tea, Popeyes, Optik Melawai).

## 🖥️ STREAMLIT APPLICATION LOGIC (Updated Mar 2026)
*The architecture relies on headless execution of Excel COM objects to interact safely with legacy corporate files without destroying built-in formulas or pivot structures.*
* **`app.py`:** Main entry point with modern `st.navigation`. Prioritizes **ANALYTICS** as the landing page. Integrates a sidebar-top branding header and a native theme toggle switch.
* **Page 1 (MID Cleaner):** Ingests Master Reference, processes regex/dictionary mapping.
* **Page 2 (Card Share Processor):** Uses `win32com.client` to parse and safely merge new transaction data natively via temporary files. 
* **Page 3 (Monitoring Processor):** Flattens and structures new wide-format CSV files against the Master Monitoring `.xlsx` target.
  * **Critical:** Overrides `PARAMETER!X2` ceiling natively via COM to adapt to the dataset length, preventing `=U14+18` equations from returning `#VALUE!` past Week 11.
* **Page 4 (Dashboard):** Decision Intelligence hub with real-time telemetry.
  * *Theme Engine:* Dynamic palette switching (Dark Navy/Gold vs Warm Cream) handled via `utils/theme.py`.
  * *Card Share:* Extracts `Realisasi` sheets for MoM and YoY growth. Features unit-aware formatting (`Rp X.XM/Jt`).
  * *Monitoring:* Implements `NAME` forward-fill to capture all 2026/2025/2024 trends. Includes a "Chart Entity Filter" to selectively visualize PMs or Merchants, preventing Plotly clutter.
  * *KPIs:* Uses smart Juta-level scaling for consistent reporting (e.g. `Rp 351.7M` vs `Rp 0.00M` errors).

## 🐛 RECENT BUG FIXES & ARCHITECTURE UPGRADES
* **Missing Heatmap Rows:** Fixed a parsing bug where rows with empty names (inheriting from the block header) were dropped. Implemented `NAME.ffill()` in `parse_monitoring_sheet`.
* **Plotly Chart Clutter:** Added entity-level multiselect filters for the Weekly Trend and Heatmap charts to allow focused analysis of specific PMs/Merchants.
* **Unit Mismatch (Juta vs Rupiah):** Monitoring Excel values are in Millions (Juta). Updated KPIs and Trend charts to use `_fmt_juta()` for correct Triliun/Milyar/Juta scaling.
* **Streamlit Table Rendering:** Resolved a CSS conflict that caused `st.dataframe` to appear blank. Narrowed global CSS selectors to exempt the Glide data grid canvas.
* **Navigation Restructure:** Reordered pages to lead with Visualizations, moved settings to the final tab, and replaced the button toggle with a modern switch.