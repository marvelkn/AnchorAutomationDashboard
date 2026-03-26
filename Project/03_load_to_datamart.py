"""
=============================================================
 PROJECT SIDANG MAGANG - BANK BTN
 Script : 03_load_to_datamart.py
 Tahap  : Load to Data Mart (ETL Step 3)
 Deskripsi:
   - Load hasil ML dari SQLite
   - Validasi schema (kolom wajib)
   - Enrich kolom bisnis (TIER_LABEL, GROWTH_STATUS, RANK, dll)
   - Hitung historis SV 2024 vs 2025 vs 2026 (YoY)
   - Build Summary per PM
   - Export Data_Mart_Ready.csv + Summary_PM.csv
=============================================================
"""

import os
import sqlite3
import warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_DB  = os.path.join(BASE_DIR, "database", "staging.db")
OUT_DIR  = os.path.join(BASE_DIR, "output")

print("=" * 60)
print("  TAHAP 3: LOAD TO DATA MART")
print("=" * 60)


# ─────────────────────────────────────────────
# STEP 1: LOAD DATA DARI SQLITE
# ─────────────────────────────────────────────
print("\n[1/5] Load data dari SQLite...")

conn = sqlite3.connect(PATH_DB)

# Load hasil ML
df = pd.read_sql_query("SELECT * FROM mart_merchant_cluster ORDER BY AVG_SV_MONTHLY DESC", conn)

# Load historis card share (semua tahun) untuk perbandingan YoY
df_hist = pd.read_sql_query("""
    SELECT MERCHANT_GROUP, YEAR, 
           SUM(TOTAL_SV) as SV_TAHUN, 
           SUM(TOTAL_TRX) as TRX_TAHUN,
           SUM(TOTAL_FBI) as FBI_TAHUN
    FROM raw_card_history
    GROUP BY MERCHANT_GROUP, YEAR
""", conn)

conn.close()

print(f"  ✓ mart_merchant_cluster : {len(df)} baris, {len(df.columns)} kolom")
print(f"  ✓ raw_card_history      : {len(df_hist)} baris (data historis)")


# ─────────────────────────────────────────────
# STEP 2: VALIDASI SCHEMA
# ─────────────────────────────────────────────
print("\n[2/5] Validasi Schema...")

REQUIRED_COLS = [
    'MERCHANT_GROUP', 'PM', 'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI',
    'AVG_SV_MONTHLY', 'AVG_FBI_MONTHLY', 'RASIO_ONUS',
    'SV_GROWTH_RATE', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE',
    'CLUSTER', 'CHURN_RISK', 'ZSCORE_GROWTH', 'ZSCORE_SV'
]

missing = [c for c in REQUIRED_COLS if c not in df.columns]
if missing:
    print(f"  ✗ KOLOM HILANG: {missing}")
    print("  Pastikan script 01 dan 02 sudah dijalankan!")
    exit(1)
else:
    print(f"  ✓ Semua {len(REQUIRED_COLS)} kolom wajib tersedia")

# Cek null di kolom kritis
for col in ['PM', 'CLUSTER', 'CHURN_RISK']:
    n_null = df[col].isna().sum()
    if n_null > 0:
        print(f"  ⚠ NULL di {col}: {n_null} baris → diisi 'UNKNOWN'")
        df[col] = df[col].fillna('UNKNOWN')
    else:
        print(f"  ✓ {col}: tidak ada null")


# ─────────────────────────────────────────────
# STEP 3: ENRICH KOLOM BISNIS
# ─────────────────────────────────────────────
print("\n[3/5] Enrichment kolom bisnis...")

# ── TIER_LABEL: deskripsi panjang cluster ────
tier_map = {
    'PREMIUM': 'Merchant Strategis — Pertahankan & Maksimalkan',
    'REGULER': 'Merchant Berkembang — Dorong Naik ke Premium',
    'PASIF'  : 'Merchant Tidak Aktif — Butuh Intervensi PM'
}
df['TIER_LABEL'] = df['CLUSTER'].map(tier_map)

# ── RISK_LABEL: gabungan cluster + churn ─────
def risk_label(row):
    if row['CHURN_RISK'] == 'YA':
        return f"⚠ CHURN RISK ({row['CLUSTER']})"
    return row['CLUSTER']
df['RISK_LABEL'] = df.apply(risk_label, axis=1)

# ── GROWTH_STATUS: badge tren SV ─────────────
def growth_status(rate):
    if rate > 0.1:
        return 'TUMBUH'
    elif rate < -0.5:
        return 'TURUN'
    else:
        return 'STABIL'
df['GROWTH_STATUS'] = df['SV_GROWTH_RATE'].apply(growth_status)

# ── RANK_SV_IN_CLUSTER: ranking dalam cluster ─
df['RANK_SV_IN_CLUSTER'] = df.groupby('CLUSTER')['AVG_SV_MONTHLY'] \
                             .rank(method='dense', ascending=False).astype(int)

# ── RANK_SV_OVERALL: ranking global ──────────
df['RANK_SV_OVERALL'] = df['AVG_SV_MONTHLY'] \
                          .rank(method='dense', ascending=False).astype(int)

# ── RASIO_ONUS_PCT: persentase (0-100) ───────
df['RASIO_ONUS_PCT'] = (df['RASIO_ONUS'] * 100).round(2)

# ── DATA HISTORIS: pivot YoY ──────────────────
df_2024 = df_hist[df_hist['YEAR']==2024][['MERCHANT_GROUP','SV_TAHUN']].rename(columns={'SV_TAHUN':'SV_2024'})
df_2025 = df_hist[df_hist['YEAR']==2025][['MERCHANT_GROUP','SV_TAHUN']].rename(columns={'SV_TAHUN':'SV_2025'})
df_2026 = df_hist[df_hist['YEAR']==2026][['MERCHANT_GROUP','SV_TAHUN']].rename(columns={'SV_TAHUN':'SV_2026_YTD'})

df = df.merge(df_2024, on='MERCHANT_GROUP', how='left')
df = df.merge(df_2025, on='MERCHANT_GROUP', how='left')
df = df.merge(df_2026, on='MERCHANT_GROUP', how='left')

# YoY Growth: (2025 - 2024) / 2024
df['YOY_GROWTH_2024_2025'] = np.where(
    df['SV_2024'].fillna(0) > 0,
    ((df['SV_2025'].fillna(0) - df['SV_2024'].fillna(0)) / df['SV_2024']) * 100,
    0
).round(2)

# ── IS_BELOW_IQR ke string ───────────────────
df['IS_BELOW_IQR'] = df['IS_BELOW_IQR'].map({1: 'YA', 0: 'TIDAK', True: 'YA', False: 'TIDAK'}).fillna('TIDAK')

# ── Round kolom float ─────────────────────────
for col in ['AVG_SV_MONTHLY', 'AVG_FBI_MONTHLY']:
    df[col] = df[col].round(0)
for col in ['RASIO_ONUS', 'SV_GROWTH_RATE', 'ACHIEVEMENT_PCT', 'ZSCORE_GROWTH', 'ZSCORE_SV']:
    df[col] = df[col].round(4)

print(f"  ✓ TIER_LABEL      : 3 kategori")
print(f"  ✓ RISK_LABEL      : gabungan cluster + churn flag")
print(f"  ✓ GROWTH_STATUS   : {df['GROWTH_STATUS'].value_counts().to_dict()}")
print(f"  ✓ RANK_SV_OVERALL : 1-{len(df)}")
print(f"  ✓ YoY Growth 2024→2025: avg {df['YOY_GROWTH_2024_2025'].mean():.1f}%")


# ─────────────────────────────────────────────
# STEP 4: BUILD SUMMARY PM
# ─────────────────────────────────────────────
print("\n[4/5] Build Summary per PM...")

df_pm = df.groupby('PM').agg(
    TOTAL_SV            = ('TOTAL_SV', 'sum'),
    TOTAL_FBI           = ('TOTAL_FBI', 'sum'),
    TOTAL_TRX           = ('TOTAL_TRX', 'sum'),
    JUMLAH_MERCHANT     = ('MERCHANT_GROUP', 'count'),
    AVG_ACHIEVEMENT_PCT = ('ACHIEVEMENT_PCT', 'mean'),
    MERCHANT_PREMIUM    = ('CLUSTER', lambda x: (x=='PREMIUM').sum()),
    MERCHANT_REGULER    = ('CLUSTER', lambda x: (x=='REGULER').sum()),
    MERCHANT_PASIF      = ('CLUSTER', lambda x: (x=='PASIF').sum()),
    MERCHANT_CHURN      = ('CHURN_RISK', lambda x: (x=='YA').sum()),
    MERCHANT_TUMBUH     = ('GROWTH_STATUS', lambda x: (x=='TUMBUH').sum()),
    SV_2024             = ('SV_2024', 'sum'),
    SV_2025             = ('SV_2025', 'sum'),
    SV_2026_YTD         = ('SV_2026_YTD', 'sum'),
    TARGET_VOL_2026     = ('TARGET_VOL_2026', 'sum'),
).reset_index()

df_pm['RANK_SV']          = df_pm['TOTAL_SV'].rank(method='dense', ascending=False).astype(int)
df_pm['FBI_PER_MERCHANT'] = (df_pm['TOTAL_FBI'] / df_pm['JUMLAH_MERCHANT']).round(0)
df_pm['AVG_ACHIEVEMENT_PCT'] = df_pm['AVG_ACHIEVEMENT_PCT'].round(2)
df_pm['YOY_GROWTH_PM']    = np.where(
    df_pm['SV_2024'] > 0,
    ((df_pm['SV_2025'] - df_pm['SV_2024']) / df_pm['SV_2024'] * 100).round(2),
    0
)
df_pm = df_pm.sort_values('RANK_SV')

print(f"\n  {'PM':<12} {'Merchant':>8} {'Total SV':>16} {'Premium':>8} {'Reguler':>8} {'Pasif':>6} {'Churn':>6} {'Rank':>5}")
print(f"  {'-'*75}")
for _, row in df_pm.iterrows():
    print(f"  {row['PM']:<12} {row['JUMLAH_MERCHANT']:>8} "
          f"  Rp {row['TOTAL_SV']/1e9:>8.2f} M "
          f"{row['MERCHANT_PREMIUM']:>8} {row['MERCHANT_REGULER']:>8} "
          f"{row['MERCHANT_PASIF']:>6} {row['MERCHANT_CHURN']:>6} "
          f"{'#'+str(int(row['RANK_SV'])):>5}")


# ─────────────────────────────────────────────
# STEP 5: EXPORT FINAL CSV
# ─────────────────────────────────────────────
print("\n[5/5] Export Data Mart...")

# Kolom final Data_Mart_Ready (urutan untuk Power BI)
COLS_FINAL = [
    # Dimensi
    'MERCHANT_GROUP', 'PM', 'SEGMEN', 'TOTAL_MID', 'EQUIP_TYPES',
    # KPI Utama
    'AVG_SV_MONTHLY', 'AVG_FBI_MONTHLY', 'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI',
    # Performa
    'RASIO_ONUS', 'RASIO_ONUS_PCT', 'SV_GROWTH_RATE', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE',
    # Target
    'TARGET_VOL_2026', 'TARGET_TRX_2026', 'TARGET_FBI_2026', 'YTD_VOL',
    # Historis YoY
    'SV_2024', 'SV_2025', 'SV_2026_YTD', 'YOY_GROWTH_2024_2025',
    # ML Output
    'CLUSTER', 'CLUSTER_RAW', 'CHURN_RISK', 'ZSCORE_GROWTH', 'ZSCORE_SV', 'IS_BELOW_IQR',
    # Label Bisnis
    'TIER_LABEL', 'RISK_LABEL', 'GROWTH_STATUS',
    # Ranking
    'RANK_SV_OVERALL', 'RANK_SV_IN_CLUSTER',
    # Meta
    'N_BULAN', 'BULAN_TERAKHIR'
]

# Pastikan semua kolom ada
cols_exist = [c for c in COLS_FINAL if c in df.columns]
df_out = df[cols_exist].copy()

# Export
path_mart   = os.path.join(OUT_DIR, 'Data_Mart_Ready.csv')
path_pm     = os.path.join(OUT_DIR, 'Summary_PM.csv')

df_out.to_csv(path_mart, index=False, encoding='utf-8-sig')
df_pm.to_csv(path_pm,   index=False, encoding='utf-8-sig')

print(f"  ✓ Data_Mart_Ready.csv : {len(df_out)} baris, {len(df_out.columns)} kolom")
print(f"  ✓ Summary_PM.csv      : {len(df_pm)} baris, {len(df_pm.columns)} kolom")
print(f"  ✓ Encoding: utf-8-sig (siap Power BI)")

# ── RINGKASAN AKHIR ───────────────────────────
print(f"\n{'='*60}")
print(f"  DATA MART FINAL — RINGKASAN EKSEKUTIF")
print(f"{'='*60}")
print(f"  Total Merchant Anchor : {len(df_out)}")
print(f"  Total SV 2026 YTD     : Rp {df_out['TOTAL_SV'].sum()/1e9:.2f} Miliar")
print(f"  Total FBI 2026 YTD    : Rp {df_out['TOTAL_FBI'].sum()/1e6:.2f} Juta")
print(f"  Total TRX 2026 YTD    : {df_out['TOTAL_TRX'].sum():,.0f}")

print(f"\n  Distribusi Cluster:")
for label, count in df_out['CLUSTER'].value_counts().items():
    pct = count / len(df_out) * 100
    bar = '█' * int(pct / 3)
    print(f"  {label:<10} {bar:<20} {count:>3} merchant ({pct:.0f}%)")

print(f"\n  Merchant CHURN RISK   : {(df_out['CHURN_RISK']=='YA').sum()} merchant")
print(f"  Merchant TUMBUH       : {(df_out['GROWTH_STATUS']=='TUMBUH').sum()} merchant")
print(f"  Merchant TURUN        : {(df_out['GROWTH_STATUS']=='TURUN').sum()} merchant")

print(f"\n  PM Ranking:")
for _, row in df_pm.iterrows():
    print(f"  #{int(row['RANK_SV'])} {row['PM']:<12} — "
          f"Rp {row['TOTAL_SV']/1e9:.2f} M SV | "
          f"{row['JUMLAH_MERCHANT']} merchant | "
          f"{row['MERCHANT_CHURN']} churn risk")

print(f"\n{'='*60}")
print(f"  TAHAP 3 SELESAI ✓  — PIPELINE ETL LENGKAP ✓")
print(f"  Output siap untuk Power BI:")
print(f"    → output/Data_Mart_Ready.csv")
print(f"    → output/Summary_PM.csv")
print(f"{'='*60}\n")