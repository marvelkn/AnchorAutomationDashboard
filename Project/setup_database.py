"""
=============================================================================
SETUP DATABASE — Project Magang BTN
=============================================================================
Script ini mensimulasikan proses ingest data dari sumber (Excel/sistem bank)
ke dalam database relasional SQLite.

Di lingkungan production bank, proses ini setara dengan:
  • Data warehouse ingest dari core banking system
  • ETL dari berbagai source system (EDC, QRIS, monitoring)
  • Load ke PostgreSQL / Oracle / SQL Server

Untuk keperluan demo/magang: kita gunakan SQLite sebagai database lokal.
Struktur SQL (DDL & DML) identik dengan production-grade RDBMS.

Struktur database:
  ┌─────────────────────┐     ┌─────────────────────────┐
  │   dim_merchant      │────▶│   fact_card_share       │
  │  (master MID)       │     │  (transaksi bulanan)    │
  └─────────────────────┘     └─────────────────────────┘
           │                             │
           ▼                             ▼
  ┌─────────────────────┐     ┌─────────────────────────┐
  │   dim_target        │     │   fact_monitoring_weekly│
  │  (target 2026)      │     │  (monitoring mingguan)  │
  └─────────────────────┘     └─────────────────────────┘

Author  : Marvel Kevin Nathanael
Magang  : Bank BTN — Divisi Digital Banking Sales, Dept. Anchor
Project : Pipeline ETL + K-Means Clustering untuk Segmentasi Merchant
=============================================================================
"""

import sqlite3
import pandas as pd
import numpy as np
import os

# ─────────────────────────────────────────────
# KONFIGURASI PATH
# ─────────────────────────────────────────────
DB_PATH      = "database/btn_anchor.db"
PATH_MASTER  = "data/raw/ALL_MID_202601_MASTER.xlsx"
PATH_CARD    = "data/raw/CARD_SHARE_MERCHANT_ANCHOR_2026.xlsx"
PATH_MONITOR = "data/raw/Monitoring_Weekly_Anchor_2026.xlsx"

os.makedirs("database", exist_ok=True)
os.makedirs("output", exist_ok=True)

# Hapus DB lama jika ada (fresh start untuk demo)
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("⚠️  Database lama dihapus — membuat ulang...")

print("=" * 65)
print("   SETUP DATABASE — BTN Anchor Merchant Analytics")
print("=" * 65)


# ═════════════════════════════════════════════════════════════════
# TAHAP 1 — BUAT SCHEMA (DDL)
# ═════════════════════════════════════════════════════════════════
print("\n[1/5] Membuat skema database (DDL)...")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ── Tabel dimensi: master merchant (satu baris per MID) ──────────
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id     TEXT        NOT NULL,
    segmen          TEXT,                   -- RETAIL / ANCHOR
    merchant_group  TEXT        NOT NULL,   -- key join utama
    merchant_brand  TEXT,
    equip           TEXT,                   -- QRIS / EDC
    merchant_name   TEXT,
    -- Kolom enrichment (tambahan analitik)
    merchant_category   TEXT,              -- F&B, RETAIL, HEALTHCARE, dll
    merchant_city       TEXT,
    merchant_province   TEXT,
    merchant_tier       TEXT,              -- TIER A / B / C berdasarkan jumlah MID
    onboarding_year     INTEGER,
    years_partnership   INTEGER,
    channel_type        TEXT,              -- QRIS DOMINANT / EDC DOMINANT / MULTI-CHANNEL
    PRIMARY KEY (merchant_id)
)
""")

# ── Tabel fakta: transaksi bulanan per merchant_anchor ───────────
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_card_share (
    id                  INTEGER     PRIMARY KEY AUTOINCREMENT,
    merchant_group      TEXT        NOT NULL,
    merchant_anchor     TEXT        NOT NULL,
    trx_month           INTEGER     NOT NULL,   -- format YYYYMM (e.g. 202601)
    year                INTEGER     NOT NULL,
    -- Jumlah transaksi per channel
    trx_debit_onus      INTEGER     DEFAULT 0,
    trx_debit_offus     INTEGER     DEFAULT 0,
    trx_credit_offus    INTEGER     DEFAULT 0,
    trx_qris_onus       INTEGER     DEFAULT 0,
    trx_qris_offus      INTEGER     DEFAULT 0,
    -- Sales Volume (Rp) per channel
    sv_debit_onus       REAL        DEFAULT 0,
    sv_debit_offus      REAL        DEFAULT 0,
    sv_credit_offus     REAL        DEFAULT 0,
    sv_qris_onus        REAL        DEFAULT 0,
    sv_qris_offus       REAL        DEFAULT 0,
    -- Fee Based Income (Rp) per channel
    fbi_debit_onus      REAL        DEFAULT 0,
    fbi_debit_offus     REAL        DEFAULT 0,
    fbi_credit_offus    REAL        DEFAULT 0,
    fbi_qris_onus       REAL        DEFAULT 0,
    fbi_qris_offus      REAL        DEFAULT 0,
    -- Kolom derived (dihitung saat ingest)
    total_trx           INTEGER     DEFAULT 0,
    total_sv            REAL        DEFAULT 0,
    total_fbi           REAL        DEFAULT 0,
    avg_ticket_size     REAL        DEFAULT 0,  -- total_sv / total_trx
    mdr_effective_pct   REAL        DEFAULT 0,  -- total_fbi / total_sv * 100
    onus_ratio_pct      REAL        DEFAULT 0,  -- sv_onus / total_sv * 100
    dominant_channel    TEXT,
    mom_growth_pct      REAL,                   -- Month-over-Month growth SV (%)
    FOREIGN KEY (merchant_group) REFERENCES dim_merchant(merchant_group)
)
""")

# ── Tabel dimensi: target 2026 per merchant ──────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_target (
    id                  INTEGER     PRIMARY KEY AUTOINCREMENT,
    merchant_group      TEXT        NOT NULL UNIQUE,
    pm                  TEXT        NOT NULL,   -- nama Account Manager / PM
    target_vol_2026     REAL,                   -- target SV full year 2026
    target_trx_2026     REAL,                   -- target TRX full year 2026
    target_fbi_2026     REAL,                   -- target FBI full year 2026
    mdr_rate            REAL,                   -- MDR rate kontrak
    FOREIGN KEY (merchant_group) REFERENCES dim_merchant(merchant_group)
)
""")

# ── Tabel fakta: monitoring mingguan (long format) ───────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_monitoring_weekly (
    id              INTEGER     PRIMARY KEY AUTOINCREMENT,
    merchant_group  TEXT        NOT NULL,
    pm              TEXT,
    year            INTEGER     NOT NULL,
    week_number     INTEGER     NOT NULL,       -- 1 sampai 53
    dimensi         TEXT        NOT NULL,       -- TRX / VOL / FBI
    value           REAL,                       -- nilai aktual minggu tsb
    ytd_cumulative  REAL,                       -- kumulatif YTD s.d. minggu ini
    FOREIGN KEY (merchant_group) REFERENCES dim_merchant(merchant_group)
)
""")

# ── Tabel staging: log proses ETL ───────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS etl_log (
    id          INTEGER     PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT,
    rows_loaded INTEGER,
    loaded_at   TEXT        DEFAULT (datetime('now','localtime')),
    status      TEXT
)
""")

conn.commit()
print("    ✅ 5 tabel berhasil dibuat: dim_merchant, fact_card_share,")
print("       dim_target, fact_monitoring_weekly, etl_log")


# ═════════════════════════════════════════════════════════════════
# TAHAP 2 — LOAD dim_merchant
# ═════════════════════════════════════════════════════════════════
print("\n[2/5] Loading dim_merchant dari ALL_MID_202601_MASTER.xlsx...")

df_mid = pd.read_excel(PATH_MASTER, sheet_name='Sheet1', dtype={'MERCHANT_ID': str})
df_mid.columns = [c.lower() for c in df_mid.columns]
df_mid['merchant_id'] = df_mid['merchant_id'].astype(str).str.strip()
df_mid['merchant_group'] = df_mid['merchant_group'].str.upper().str.strip()
df_mid['segmen'] = df_mid['segmen'].str.upper().str.strip()
df_mid['equip'] = df_mid['equip'].str.upper().str.strip()

# Enrichment: kategori, kota, tier, channel, onboarding
CATEGORY_MAP = {
    'ALFA GROUP':'RETAIL','INDOMARET':'RETAIL','HERO GROUP':'RETAIL','LOTTE GROUP':'RETAIL',
    'KAWAN LAMA':'RETAIL','MITRA10':'RETAIL','SARINAH':'RETAIL','SURYA BUMI RETAILINDO':'RETAIL',
    'SUPRA BOGA LESTARI':'RETAIL','STEVEN GROUP':'RETAIL','GRAMEDIA':'RETAIL','MERCHANT RETAIL':'RETAIL',
    'MAP GROUP':'LIFESTYLE & FASHION','IKEA':'LIFESTYLE & FASHION',
    'PIZZA HUT':'F&B','SOLARIA':'F&B','HOKBEN':'F&B','YOSHINOYA':'F&B','POPEYES':'F&B',
    'CHAMP RESTO':'F&B','BEARD PAPA':'F&B','BANBAN TEA':'F&B','SHIHLIN':'F&B','SOUR SALLY':'F&B',
    'MIXUE':'F&B','HOP HOP':'F&B','HOKKAIDO BAKED CHEESE':'F&B','MAMA ROZ':'F&B','TAMANI':'F&B',
    'HAUS':'F&B','ZENBU':'F&B','YOGURT REPUBLIC':'F&B','SUSHI TEI':'F&B','ES TELLER 77':'F&B',
    'HEAVENLY WANG':'F&B','BOGA GROUP':'F&B','BURGER KING':'F&B',
    'EKA HOSPITAL':'HEALTHCARE','KIMIA FARMA':'HEALTHCARE','OPTIK MELAWAI':'HEALTHCARE',
    'DAMRI':'TRANSPORTATION & TRAVEL','DELTA WIBAWA BERSAMA':'TRANSPORTATION & TRAVEL',
    'DWIDAYA':'TRANSPORTATION & TRAVEL','KCIC':'TRANSPORTATION & TRAVEL','PATRA JASA':'TRANSPORTATION & TRAVEL',
    'ANCOL':'ENTERTAINMENT','PLATINUM CINEPLEX':'ENTERTAINMENT',
    'PERTAMINA RETAIL':'ENERGY & FUEL',
}
CITY_MAP = {
    'ALFA GROUP':'TANGERANG','INDOMARET':'JAKARTA PUSAT','MAP GROUP':'JAKARTA SELATAN',
    'IKEA':'TANGERANG','PERTAMINA RETAIL':'JAKARTA TIMUR','HERO GROUP':'JAKARTA SELATAN',
    'KAWAN LAMA':'JAKARTA BARAT','LOTTE GROUP':'JAKARTA PUSAT','MITRA10':'JAKARTA TIMUR',
    'SARINAH':'JAKARTA PUSAT','SURYA BUMI RETAILINDO':'SURABAYA','SUPRA BOGA LESTARI':'JAKARTA SELATAN',
    'STEVEN GROUP':'JAKARTA SELATAN','GRAMEDIA':'JAKARTA PUSAT','MERCHANT RETAIL':'JAKARTA PUSAT',
    'PIZZA HUT':'JAKARTA SELATAN','SOLARIA':'JAKARTA PUSAT','HOKBEN':'JAKARTA BARAT',
    'YOSHINOYA':'JAKARTA PUSAT','POPEYES':'JAKARTA SELATAN','CHAMP RESTO':'JAKARTA SELATAN',
    'BEARD PAPA':'JAKARTA BARAT','BANBAN TEA':'JAKARTA SELATAN','SHIHLIN':'JAKARTA BARAT',
    'SOUR SALLY':'JAKARTA BARAT','MIXUE':'BEKASI','HOP HOP':'SURABAYA',
    'HOKKAIDO BAKED CHEESE':'BANDUNG','MAMA ROZ':'BOGOR','TAMANI':'JAKARTA SELATAN',
    'HAUS':'JAKARTA BARAT','ZENBU':'JAKARTA SELATAN','YOGURT REPUBLIC':'JAKARTA SELATAN',
    'SUSHI TEI':'JAKARTA SELATAN','ES TELLER 77':'JAKARTA PUSAT','HEAVENLY WANG':'JAKARTA BARAT',
    'EKA HOSPITAL':'PEKANBARU','KIMIA FARMA':'BANDUNG','OPTIK MELAWAI':'JAKARTA SELATAN',
    'DAMRI':'BANDUNG','DELTA WIBAWA BERSAMA':'JAKARTA UTARA','DWIDAYA':'JAKARTA PUSAT',
    'KCIC':'BEKASI','PATRA JASA':'JAKARTA PUSAT','ANCOL':'JAKARTA UTARA',
    'PLATINUM CINEPLEX':'BEKASI','BOGA GROUP':'JAKARTA SELATAN','BURGER KING':'JAKARTA SELATAN',
}
PROVINCE_MAP = {
    'TANGERANG':'BANTEN','JAKARTA PUSAT':'DKI JAKARTA','JAKARTA SELATAN':'DKI JAKARTA',
    'JAKARTA BARAT':'DKI JAKARTA','JAKARTA TIMUR':'DKI JAKARTA','JAKARTA UTARA':'DKI JAKARTA',
    'SURABAYA':'JAWA TIMUR','BANDUNG':'JAWA BARAT','BEKASI':'JAWA BARAT',
    'BOGOR':'JAWA BARAT','PEKANBARU':'RIAU',
}
ONBOARDING_MAP = {
    'ALFA GROUP':2018,'INDOMARET':2017,'MAP GROUP':2019,'PERTAMINA RETAIL':2018,
    'HERO GROUP':2019,'KAWAN LAMA':2020,'LOTTE GROUP':2019,'MITRA10':2020,
    'PIZZA HUT':2020,'SOLARIA':2021,'YOSHINOYA':2021,'HOKBEN':2022,'KIMIA FARMA':2021,
    'OPTIK MELAWAI':2021,'EKA HOSPITAL':2022,'ANCOL':2020,'PLATINUM CINEPLEX':2022,
    'GRAMEDIA':2021,'IKEA':2022,'HAUS':2022,'BANBAN TEA':2023,'MIXUE':2023,
    'SUSHI TEI':2022,'POPEYES':2023,'ZENBU':2022,'CHAMP RESTO':2021,'SHIHLIN':2022,
    'DAMRI':2021,'KCIC':2023,'DWIDAYA':2020,'STEVEN GROUP':2021,'HOP HOP':2022,
    'BOGA GROUP':2022,'BURGER KING':2022,'SARINAH':2020,'ES TELLER 77':2019,
}

mid_count = df_mid.groupby('merchant_group').size()
p66 = mid_count.quantile(0.66)
p33 = mid_count.quantile(0.33)

# QRIS share per group
equip_dist = df_mid.groupby('merchant_group')['equip'].value_counts(normalize=True).unstack(fill_value=0)

df_mid['merchant_category'] = df_mid['merchant_group'].map(CATEGORY_MAP).fillna('LAINNYA')
df_mid['merchant_city']     = df_mid['merchant_group'].map(CITY_MAP).fillna('JAKARTA PUSAT')
df_mid['merchant_province'] = df_mid['merchant_city'].map(PROVINCE_MAP).fillna('DKI JAKARTA')
df_mid['merchant_tier']     = df_mid['merchant_group'].apply(
    lambda g: 'TIER A' if mid_count.get(g, 1) >= p66
              else ('TIER B' if mid_count.get(g, 1) >= p33 else 'TIER C')
)
df_mid['onboarding_year']   = df_mid['merchant_group'].map(ONBOARDING_MAP).fillna(2021).astype(int)
df_mid['years_partnership'] = 2026 - df_mid['onboarding_year']
df_mid['channel_type']      = df_mid['merchant_group'].apply(
    lambda g: (
        'QRIS DOMINANT' if g in equip_dist.index and equip_dist.loc[g].get('QRIS', 0) >= 0.8
        else ('EDC DOMINANT' if g in equip_dist.index and equip_dist.loc[g].get('EDC', 0) >= 0.8
              else 'MULTI-CHANNEL')
    )
)

df_mid.to_sql('dim_merchant', conn, if_exists='replace', index=False)
row_count = conn.execute("SELECT COUNT(*) FROM dim_merchant").fetchone()[0]
cur.execute("INSERT INTO etl_log (table_name, rows_loaded, status) VALUES (?, ?, ?)",
            ('dim_merchant', row_count, 'SUCCESS'))
conn.commit()
print(f"    ✅ dim_merchant: {row_count:,} baris dimuat")
print(f"       Kolom enrichment ditambahkan: merchant_category, merchant_city,")
print(f"       merchant_province, merchant_tier, onboarding_year, channel_type")


# ═════════════════════════════════════════════════════════════════
# TAHAP 3 — LOAD fact_card_share
# ═════════════════════════════════════════════════════════════════
print("\n[3/5] Loading fact_card_share dari CARD_SHARE_MERCHANT_ANCHOR_2026.xlsx...")

df_card = pd.read_excel(PATH_CARD, sheet_name='Realisasi')
df_card.columns = [c.lower() for c in df_card.columns]
df_card['merchant_group']  = df_card['merchant_group'].str.upper().str.strip()
df_card['merchant_anchor'] = df_card['merchant_anchor'].str.upper().str.strip()

# Hitung kolom derived
df_card['total_trx'] = (df_card['trx_debit_onus'].fillna(0) + df_card['trx_debit_offus'].fillna(0) +
                         df_card['trx_credit_offus'].fillna(0) + df_card['trx_qris_onus'].fillna(0) +
                         df_card['trx_qris_offus'].fillna(0)).astype(int)

df_card['total_sv']  = (df_card['sv_debit_onus'].fillna(0) + df_card['sv_debit_offus'].fillna(0) +
                         df_card['sv_credit_offus'].fillna(0) + df_card['sv_qris_onus'].fillna(0) +
                         df_card['sv_qris_offus'].fillna(0))

df_card['total_fbi'] = (df_card['fbi_debit_onus'].fillna(0) + df_card['fbi_debit_offus'].fillna(0) +
                         df_card['fbi_credit_offus'].fillna(0) + df_card['fbi_qris_onus'].fillna(0) +
                         df_card['fbi_qris_offus'].fillna(0))

sv_onus = df_card['sv_debit_onus'].fillna(0) + df_card['sv_qris_onus'].fillna(0)

df_card['avg_ticket_size']  = np.where(df_card['total_trx'] > 0,
                                        df_card['total_sv'] / df_card['total_trx'], 0)
df_card['mdr_effective_pct']= np.where(df_card['total_sv'] > 0,
                                        df_card['total_fbi'] / df_card['total_sv'] * 100, 0).round(4)
df_card['onus_ratio_pct']   = np.where(df_card['total_sv'] > 0,
                                        sv_onus / df_card['total_sv'] * 100, 0).round(2)

def dominant_channel(row):
    channels = {
        'DEBIT ONUS': row.get('sv_debit_onus', 0) or 0,
        'DEBIT OFFUS': row.get('sv_debit_offus', 0) or 0,
        'CREDIT': row.get('sv_credit_offus', 0) or 0,
        'QRIS ONUS': row.get('sv_qris_onus', 0) or 0,
        'QRIS OFFUS': row.get('sv_qris_offus', 0) or 0,
    }
    return 'NO TRX' if sum(channels.values()) == 0 else max(channels, key=channels.get)

df_card['dominant_channel'] = df_card.apply(dominant_channel, axis=1)

# MoM Growth per merchant_anchor
df_card = df_card.sort_values(['merchant_anchor', 'year', 'trx_month'])
df_card['prev_sv']       = df_card.groupby('merchant_anchor')['total_sv'].shift(1)
df_card['mom_growth_pct']= np.where(df_card['prev_sv'] > 0,
                                     ((df_card['total_sv'] - df_card['prev_sv']) / df_card['prev_sv'] * 100).round(2),
                                     None)

# Drop kolom helper dan kolom key yang tidak perlu di DB
df_card_db = df_card.drop(columns=['key', 'prev_sv'], errors='ignore')
df_card_db.to_sql('fact_card_share', conn, if_exists='replace', index=False)

row_count = conn.execute("SELECT COUNT(*) FROM fact_card_share").fetchone()[0]
cur.execute("INSERT INTO etl_log (table_name, rows_loaded, status) VALUES (?, ?, ?)",
            ('fact_card_share', row_count, 'SUCCESS'))
conn.commit()
print(f"    ✅ fact_card_share: {row_count:,} baris dimuat")
print(f"       Tahun: 2024, 2025, 2026 | Kolom derived: total_sv, total_fbi,")
print(f"       avg_ticket_size, mdr_effective_pct, onus_ratio_pct, dominant_channel, mom_growth_pct")


# ═════════════════════════════════════════════════════════════════
# TAHAP 4 — LOAD dim_target + fact_monitoring_weekly
# ═════════════════════════════════════════════════════════════════
print("\n[4/5] Loading dim_target & fact_monitoring_weekly dari Monitoring_Weekly...")

df_target = pd.read_excel(PATH_MONITOR, sheet_name='Edit Target')
df_target = df_target.rename(columns={
    'MERCHANT GROUP': 'merchant_group',
    'PM': 'pm',
    'VOL NEW': 'target_vol_2026',
    'TRX': 'target_trx_2026',
    'FBI': 'target_fbi_2026',
    'MDR': 'mdr_rate',
})
df_target['merchant_group'] = df_target['merchant_group'].str.upper().str.strip()
df_target = df_target[['merchant_group','pm','target_vol_2026','target_trx_2026','target_fbi_2026','mdr_rate']]
df_target = df_target.dropna(subset=['merchant_group']).drop_duplicates(subset=['merchant_group'])
df_target.to_sql('dim_target', conn, if_exists='replace', index=False)

# Monitoring: ubah dari wide format → long format (tidy/normalized)
df_mon = pd.read_excel(PATH_MONITOR, sheet_name='2026')
df_mon['MERCHANT_GROUP'] = df_mon['MERCHANT_GROUP'].str.upper().str.strip()
week_cols = [i for i in range(1, 54) if i in df_mon.columns]

monitoring_rows = []
for _, row in df_mon.iterrows():
    ytd_sum = 0.0
    for w in week_cols:
        val = row.get(w, None)
        if pd.notna(val) and val != 0:
            ytd_sum += float(val)
            monitoring_rows.append({
                'merchant_group': row['MERCHANT_GROUP'],
                'pm'            : row['PM'],
                'year'          : int(row['VALUE']),
                'week_number'   : int(w),
                'dimensi'       : str(row['DIMENSI']),
                'value'         : float(val),
                'ytd_cumulative': round(ytd_sum, 2),
            })

df_mon_long = pd.DataFrame(monitoring_rows)
df_mon_long.to_sql('fact_monitoring_weekly', conn, if_exists='replace', index=False)

r1 = conn.execute("SELECT COUNT(*) FROM dim_target").fetchone()[0]
r2 = conn.execute("SELECT COUNT(*) FROM fact_monitoring_weekly").fetchone()[0]
cur.execute("INSERT INTO etl_log (table_name, rows_loaded, status) VALUES (?, ?, ?)",
            ('dim_target', r1, 'SUCCESS'))
cur.execute("INSERT INTO etl_log (table_name, rows_loaded, status) VALUES (?, ?, ?)",
            ('fact_monitoring_weekly', r2, 'SUCCESS'))
conn.commit()
print(f"    ✅ dim_target: {r1} merchant targets dimuat")
print(f"    ✅ fact_monitoring_weekly: {r2:,} baris dimuat (wide → long format)")
print(f"       Struktur: 1 baris = 1 merchant x 1 minggu x 1 dimensi (TRX/VOL/FBI)")


# ═════════════════════════════════════════════════════════════════
# TAHAP 5 — VERIFIKASI DATABASE
# ═════════════════════════════════════════════════════════════════
print("\n[5/5] Verifikasi database...")

tables_info = cur.execute("""
    SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
""").fetchall()

print(f"\n    📂 Database: {DB_PATH}")
print(f"    {'─'*50}")
for (tbl,) in tables_info:
    count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    cols  = len(conn.execute(f"PRAGMA table_info({tbl})").fetchall())
    print(f"    📋 {tbl:<30} {count:>8,} baris  |  {cols:>2} kolom")

print(f"\n    {'─'*50}")
print("    ETL Log:")
log = conn.execute("SELECT table_name, rows_loaded, loaded_at, status FROM etl_log").fetchall()
for row in log:
    print(f"    [{row[3]}] {row[0]:<30} → {row[1]:>8,} baris  @ {row[2]}")

conn.close()
print(f"\n{'='*65}")
print("  ✅  Database berhasil dibuat!")
print(f"  📁  Lokasi: {os.path.abspath(DB_PATH)}")
print("  ➡️   Jalankan etl_demo_full.py untuk demo SQL extraction")
print(f"{'='*65}\n")
