"""
=============================================================
 PROJECT SIDANG MAGANG - BANK BTN
 Script : 01_extract_and_clean.py
 Tahap  : Extract & Clean (ETL Step 1)
 Deskripsi:
   - Load 3 file Excel ke SQLite (staging database)
   - Extract data via SQL query
   - Data cleaning & standardisasi
   - Output: tabel bersih di SQLite, siap untuk Transform
=============================================================
"""

import os
import sqlite3
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# KONFIGURASI PATH
# ─────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
PATH_MID    = os.path.join(BASE_DIR, "data", "raw", "ALL_MID_202601_MASTER.xlsx")
PATH_CARD   = os.path.join(BASE_DIR, "data", "raw", "CARD_SHARE_MERCHANT_ANCHOR_2026.xlsx")
PATH_MON    = os.path.join(BASE_DIR, "data", "raw", "Monitoring_Weekly_Anchor_2026.xlsx")
PATH_DB     = os.path.join(BASE_DIR, "database", "staging.db")

os.makedirs(os.path.join(BASE_DIR, "database"), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "output"), exist_ok=True)

print("=" * 60)
print("  TAHAP 1: EXTRACT & CLEAN")
print("=" * 60)


# ─────────────────────────────────────────────
# STEP 1: LOAD EXCEL → PANDAS
# ─────────────────────────────────────────────
print("\n[1/5] Membaca file Excel...")

# FILE 1: ALL_MID — buku induk merchant (dimensi statis)
df_mid = pd.read_excel(PATH_MID, sheet_name="Sheet1", dtype=str)
print(f"  ✓ ALL_MID     : {df_mid.shape[0]:,} baris, {df_mid.shape[1]} kolom")

# FILE 2: CARD_SHARE — data transaksi bulanan (Jan 2024 - Feb 2026)
df_card = pd.read_excel(PATH_CARD, sheet_name="Realisasi")
print(f"  ✓ CARD_SHARE  : {df_card.shape[0]:,} baris, {df_card.shape[1]} kolom")

# FILE 3: MONITORING — data mingguan 2026 (Week 1-7, format wide)
df_mon = pd.read_excel(PATH_MON, sheet_name="2026")
print(f"  ✓ MONITORING  : {df_mon.shape[0]:,} baris, {df_mon.shape[1]} kolom")

# FILE 3b: EDIT TARGET — target tahunan 2026 per merchant
df_target = pd.read_excel(PATH_MON, sheet_name="Edit Target")
print(f"  ✓ EDIT TARGET : {df_target.shape[0]:,} baris, {df_target.shape[1]} kolom")


# ─────────────────────────────────────────────
# STEP 2: CLEANING MASING-MASING DATAFRAME
# ─────────────────────────────────────────────
print("\n[2/5] Data Cleaning...")

# ── ALL_MID ─────────────────────────────────
# Ambil kolom yang relevan saja
df_mid = df_mid[['MERCHANT_ID', 'SEGMEN', 'MERCHANT_GROUP', 'MERCHANT_BRAND', 'EQUIP', 'MERCHANT_NAME']].copy()

# Standardisasi: uppercase + strip whitespace
for col in ['SEGMEN', 'MERCHANT_GROUP', 'MERCHANT_BRAND', 'EQUIP', 'MERCHANT_NAME']:
    df_mid[col] = df_mid[col].str.strip().str.upper()

# Deduplikasi: ambil unique MERCHANT_GROUP saja untuk tabel dimensi
df_mid_dim = df_mid.groupby('MERCHANT_GROUP').agg(
    SEGMEN        = ('SEGMEN', 'first'),
    MERCHANT_BRAND= ('MERCHANT_BRAND', 'first'),
    TOTAL_MID     = ('MERCHANT_ID', 'count'),   # jumlah MID per group
    EQUIP_TYPES   = ('EQUIP', lambda x: ', '.join(sorted(x.unique())))
).reset_index()

print(f"  ✓ ALL_MID cleaned: {len(df_mid_dim)} merchant group unik")

# ── CARD_SHARE ───────────────────────────────
# Standardisasi nama merchant
df_card['MERCHANT_GROUP']  = df_card['MERCHANT_GROUP'].str.strip().str.upper()
df_card['MERCHANT_ANCHOR'] = df_card['MERCHANT_ANCHOR'].str.strip().str.upper()

# Handle null di TRX_QRIS_OFFUS (10 baris null) → fill 0
df_card['TRX_QRIS_OFFUS'] = df_card['TRX_QRIS_OFFUS'].fillna(0)

# Buat kolom agregat: total SV, TRX, FBI per record
df_card['TOTAL_SV']  = (df_card['SV_DEBIT_ONUS']  + df_card['SV_DEBIT_OFFUS'] +
                        df_card['SV_CREDIT_OFFUS'] + df_card['SV_QRIS_ONUS']   +
                        df_card['SV_QRIS_OFFUS'])

df_card['TOTAL_TRX'] = (df_card['TRX_DEBIT_ONUS']  + df_card['TRX_DEBIT_OFFUS'] +
                        df_card['TRX_CREDIT_OFFUS'] + df_card['TRX_QRIS_ONUS']   +
                        df_card['TRX_QRIS_OFFUS'])

df_card['TOTAL_FBI'] = (df_card['FBI_DEBIT_ONUS']  + df_card['FBI_DEBIT_OFFUS'] +
                        df_card['FBI_CREDIT_OFFUS'] + df_card['FBI_QRIS_ONUS']   +
                        df_card['FBI_QRIS_OFFUS'])

# Rasio On-Us (transaksi pakai kartu BTN sendiri / total)
df_card['RASIO_ONUS'] = df_card['SV_DEBIT_ONUS'] / df_card['TOTAL_SV'].replace(0, pd.NA)
df_card['RASIO_ONUS'] = df_card['RASIO_ONUS'].fillna(0)

# Filter: hanya data 2026 (data terbaru untuk analisis)
df_card_2026 = df_card[df_card['YEAR'] == 2026].copy()

# Agregat per MERCHANT_GROUP (karena 1 group bisa punya banyak anchor)
df_card_agg = df_card_2026.groupby('MERCHANT_GROUP').agg(
    TOTAL_SV      = ('TOTAL_SV',  'sum'),
    TOTAL_TRX     = ('TOTAL_TRX', 'sum'),
    TOTAL_FBI     = ('TOTAL_FBI', 'sum'),
    SV_ONUS       = ('SV_DEBIT_ONUS', 'sum'),
    RASIO_ONUS    = ('RASIO_ONUS', 'mean'),
    N_BULAN       = ('TRX_MONTH', 'nunique'),
    BULAN_TERAKHIR= ('TRX_MONTH', 'max')
).reset_index()

print(f"  ✓ CARD_SHARE cleaned: {len(df_card_agg)} merchant group (data 2026)")
print(f"    Periode: {df_card_2026['TRX_MONTH'].min()} - {df_card_2026['TRX_MONTH'].max()}")

# ── MONITORING 2026 (Pivot Wide → Long) ──────
df_mon['MERCHANT_GROUP'] = df_mon['MERCHANT_GROUP'].str.strip().str.upper()
df_mon['PM']             = df_mon['PM'].str.strip().str.upper()

# Kolom minggu yang tersedia (1-7, sisanya NaN)
week_cols = [c for c in df_mon.columns if isinstance(c, int)]
weeks_with_data = [w for w in week_cols if df_mon[w].notna().any()]
print(f"  ✓ MONITORING: week tersedia = Week {min(weeks_with_data)} - {max(weeks_with_data)} ({len(weeks_with_data)} minggu)")

# Pivot: wide → long (melt week columns)
df_mon_long = df_mon.melt(
    id_vars=['MERCHANT_GROUP', 'DIMENSI', 'PM', 'YTD'],
    value_vars=weeks_with_data,
    var_name='WEEK',
    value_name='WEEKLY_VALUE'
).dropna(subset=['WEEKLY_VALUE'])

df_mon_long['WEEK'] = df_mon_long['WEEK'].astype(int)

# Pisah per DIMENSI → 3 kolom (TRX, VOL, FBI)
df_trx = df_mon_long[df_mon_long['DIMENSI']=='TRX'][['MERCHANT_GROUP','PM','WEEK','WEEKLY_VALUE']].rename(columns={'WEEKLY_VALUE':'WEEKLY_TRX'})
df_vol = df_mon_long[df_mon_long['DIMENSI']=='VOL'][['MERCHANT_GROUP','WEEK','WEEKLY_VALUE']].rename(columns={'WEEKLY_VALUE':'WEEKLY_VOL'})
df_fbi = df_mon_long[df_mon_long['DIMENSI']=='FBI'][['MERCHANT_GROUP','WEEK','WEEKLY_VALUE']].rename(columns={'WEEKLY_VALUE':'WEEKLY_FBI'})

df_weekly = df_trx.merge(df_vol, on=['MERCHANT_GROUP','WEEK'], how='outer')
df_weekly = df_weekly.merge(df_fbi, on=['MERCHANT_GROUP','WEEK'], how='outer')

# Hitung growth: (nilai week terakhir - week pertama) / week pertama
df_mon_ytd = df_mon_long[df_mon_long['DIMENSI']=='VOL'].groupby('MERCHANT_GROUP').agg(
    YTD_VOL         = ('YTD', 'first'),
    VOL_WEEK_PERTAMA= ('WEEKLY_VALUE', 'first'),
    VOL_WEEK_TERAKHIR= ('WEEKLY_VALUE', 'last'),
    WEEKS_ACTIVE    = ('WEEKLY_VALUE', lambda x: (x > 0).sum()),
    PM              = ('PM', 'first')
).reset_index()

df_mon_ytd['SV_GROWTH_RATE'] = (
    (df_mon_ytd['VOL_WEEK_TERAKHIR'] - df_mon_ytd['VOL_WEEK_PERTAMA']) /
    df_mon_ytd['VOL_WEEK_PERTAMA'].replace(0, pd.NA)
).fillna(0)

print(f"  ✓ MONITORING cleaned: {len(df_mon_ytd)} merchant, {len(df_weekly)} baris weekly")

# ── EDIT TARGET ───────────────────────────────
# Ambil kolom yang relevan: target VOL, TRX, FBI 2026
df_target_clean = df_target[['MERCHANT GROUP', 'PM', 'VOL NEW', 'TRX NEW', 'FBI FIX']].copy()
df_target_clean = df_target_clean.dropna(subset=['MERCHANT GROUP'])
df_target_clean.columns = ['MERCHANT_GROUP', 'PM', 'TARGET_VOL_2026', 'TARGET_TRX_2026', 'TARGET_FBI_2026']
df_target_clean['MERCHANT_GROUP'] = df_target_clean['MERCHANT_GROUP'].str.strip().str.upper()
df_target_clean['PM'] = df_target_clean['PM'].str.strip().str.upper()

print(f"  ✓ EDIT TARGET cleaned: {len(df_target_clean)} merchant")


# ─────────────────────────────────────────────
# STEP 3: SIMPAN KE SQLITE (STAGING DATABASE)
# ─────────────────────────────────────────────
print(f"\n[3/5] Menyimpan ke SQLite: {PATH_DB}")

conn = sqlite3.connect(PATH_DB)

df_mid_dim.to_sql("raw_master",      conn, if_exists="replace", index=False)
df_card_agg.to_sql("raw_card_share", conn, if_exists="replace", index=False)
df_mon_ytd.to_sql("raw_monitoring",  conn, if_exists="replace", index=False)
df_weekly.to_sql("raw_weekly",       conn, if_exists="replace", index=False)
df_target_clean.to_sql("raw_target", conn, if_exists="replace", index=False)
# Simpan juga card_share full (semua tahun) untuk analisis historis
df_card.groupby(['MERCHANT_GROUP','TRX_MONTH','YEAR']).agg(
    TOTAL_SV=('TOTAL_SV','sum'), TOTAL_TRX=('TOTAL_TRX','sum'), TOTAL_FBI=('TOTAL_FBI','sum')
).reset_index().to_sql("raw_card_history", conn, if_exists="replace", index=False)

print("  ✓ Tabel tersimpan di SQLite:")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
for (tbl,) in cursor.fetchall():
    cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
    count = cursor.fetchone()[0]
    print(f"    - {tbl:<20} : {count:,} baris")


# ─────────────────────────────────────────────
# STEP 4: EXTRACT VIA SQL QUERY
# ─────────────────────────────────────────────
print("\n[4/5] Extract & JOIN via SQL Query...")

query = """
    SELECT
        m.MERCHANT_GROUP,
        m.SEGMEN,
        m.TOTAL_MID,
        m.EQUIP_TYPES,

        -- Data transaksi 2026 dari CARD_SHARE
        c.TOTAL_SV,
        c.TOTAL_TRX,
        c.TOTAL_FBI,
        c.RASIO_ONUS,
        c.N_BULAN,
        c.BULAN_TERAKHIR,

        -- Data monitoring mingguan
        mon.PM,
        mon.YTD_VOL,
        mon.SV_GROWTH_RATE,
        mon.WEEKS_ACTIVE,
        mon.VOL_WEEK_PERTAMA,
        mon.VOL_WEEK_TERAKHIR,

        -- Target 2026
        t.TARGET_VOL_2026,
        t.TARGET_TRX_2026,
        t.TARGET_FBI_2026

    FROM raw_card_share c
    LEFT JOIN raw_master m
        ON c.MERCHANT_GROUP = m.MERCHANT_GROUP
    LEFT JOIN raw_monitoring mon
        ON c.MERCHANT_GROUP = mon.MERCHANT_GROUP
    LEFT JOIN raw_target t
        ON c.MERCHANT_GROUP = t.MERCHANT_GROUP

    ORDER BY c.TOTAL_SV DESC
"""

df_clean = pd.read_sql_query(query, conn)
conn.close()

print(f"  ✓ Dataset hasil JOIN: {df_clean.shape[0]} merchant, {df_clean.shape[1]} kolom")
print(f"\n  Merchant yang berhasil di-join:")
print(f"    - Punya data MONITORING : {df_clean['PM'].notna().sum()} merchant")
print(f"    - Punya data TARGET 2026: {df_clean['TARGET_VOL_2026'].notna().sum()} merchant")
print(f"    - Tidak ada monitoring  : {df_clean['PM'].isna().sum()} merchant")

# Tampilkan merchant tanpa monitoring
no_mon = df_clean[df_clean['PM'].isna()]['MERCHANT_GROUP'].tolist()
if no_mon:
    print(f"    → Merchant tanpa monitoring: {no_mon}")


# ─────────────────────────────────────────────
# STEP 5: VALIDASI DATA & OUTPUT
# ─────────────────────────────────────────────
print("\n[5/5] Validasi & Summary...")

print(f"\n  {'='*50}")
print(f"  RINGKASAN DATA BERSIH")
print(f"  {'='*50}")
print(f"  Total merchant     : {len(df_clean)}")
print(f"  Total SV 2026      : Rp {df_clean['TOTAL_SV'].sum()/1e9:,.2f} Miliar")
print(f"  Total TRX 2026     : {df_clean['TOTAL_TRX'].sum():,}")
print(f"  Total FBI 2026     : Rp {df_clean['TOTAL_FBI'].sum()/1e6:,.2f} Juta")
print(f"  Periode data       : {df_clean['N_BULAN'].max()} bulan")
print(f"\n  PM Distribution:")
pm_counts = df_clean.groupby('PM').size().sort_values(ascending=False)
for pm, count in pm_counts.items():
    print(f"    {pm:<12}: {count} merchant")

print(f"\n  Top 5 Merchant by SV:")
top5 = df_clean[['MERCHANT_GROUP','TOTAL_SV','PM']].head(5)
for _, row in top5.iterrows():
    print(f"    {row['MERCHANT_GROUP']:<25} : Rp {row['TOTAL_SV']/1e9:,.2f} M  (PM: {row['PM']})")

# Simpan checkpoint untuk Step 2
df_clean.to_csv(os.path.join(BASE_DIR, "output", "checkpoint_01_clean.csv"),
                index=False, encoding='utf-8-sig')
print(f"\n  ✓ Checkpoint disimpan: output/checkpoint_01_clean.csv")
print(f"\n{'='*60}")
print("  TAHAP 1 SELESAI ✓")
print("  Jalankan berikutnya: 02_transform_and_ml.py")
print(f"{'='*60}\n")