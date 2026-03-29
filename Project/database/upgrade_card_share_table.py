import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# ==========================================
# SETUP PATH OTOMATIS
# ==========================================
# 1. Posisi script ini berada (folder 'database')
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

# 2. Mundur satu folder ke folder utama ('Project')
PROJECT_DIR = os.path.dirname(BASE_DIR) 

# 3. Path ke database
PATH_DB = os.path.join(BASE_DIR, "staging.db")

# 4. Path ke file CSV mentah Card Share
PATH_CARD = os.path.join(PROJECT_DIR, "data", "raw", "CARD_SHARE_ANCHOR_2026.csv")
# ==========================================

print(f"Mencari file Card Share di: {PATH_CARD}")

try:
    print("\nMembaca file CSV Card Share mentah...")
    df_card = pd.read_csv(PATH_CARD)
except FileNotFoundError as e:
    print(f"\n❌ ERROR: File tidak ditemukan! Pastikan file CSV ada di folder 'data/raw'.\nDetail: {e}")
    exit()

print(f"Total baris awal: {len(df_card)} baris.")

# ==========================================
# TAHAP 1: MENGHITUNG TOTAL (TRX, VOL, FBI)
# ==========================================
print("Menghitung agregasi kolom Total (TRX, VOL, FBI)...")

# Memastikan semua kolom angka bersih dari NaN (diubah jadi 0)
num_cols = [c for c in df_card.columns if c.startswith(('TRX_', 'VOL_', 'FBI_'))]
for col in num_cols:
    df_card[col] = pd.to_numeric(df_card[col], errors='coerce').fillna(0)

# Menghitung Total Transaksi (TRX)
df_card['TOTAL_TRX'] = (
    df_card['TRX_DEBIT_ONUS'] + df_card['TRX_DEBIT_OFFUS'] + 
    df_card['TRX_CREDIT_OFFUS'] + df_card['TRX_QRIS_ONUS'] + df_card['TRX_QRIS_OFFUS']
)

# Menghitung Total Sales Volume (VOL/SV)
df_card['TOTAL_SV'] = (
    df_card['VOL_DEBIT_ONUS'] + df_card['VOL_DEBIT_OFFUS'] + 
    df_card['VOL_CREDIT_OFFUS'] + df_card['VOL_QRIS_ONUS'] + df_card['VOL_QRIS_OFFUS']
)

# Menghitung Total Fee Based Income (FBI)
df_card['TOTAL_FBI'] = (
    df_card['FBI_DEBIT_ONUS'] + df_card['FBI_DEBIT_OFFUS'] + 
    df_card['FBI_CREDIT_OFFUS'] + df_card['FBI_QRIS_ONUS'] + df_card['FBI_QRIS_OFFUS']
)

# ==========================================
# TAHAP 2: MEMPERBANYAK DATA (TIME MACHINE 2024 & 2025)
# ==========================================
print("Melakukan 'Time Machine' untuk membuat riwayat data tahun 2024 & 2025...")

# Pastikan TRANSACTION_MONTH terbaca sebagai angka
df_card['TRANSACTION_MONTH'] = df_card['TRANSACTION_MONTH'].astype(int)

# Fungsi untuk menduplikasi data dan memotong performanya (Simulasi bisnis)
def create_history_data(df_source, target_year, discount_rate):
    df_new = df_source.copy()
    
    # Ubah format bulan (Contoh: 202601 -> 202501)
    # Ambil 2 digit terakhir (bulan), lalu tambah ke tahun target
    months = df_new['TRANSACTION_MONTH'] % 100
    df_new['TRANSACTION_MONTH'] = (target_year * 100) + months
    
    # Potong transaksinya pakai nilai random supaya terlihat organik (tidak sama persis)
    for col in num_cols + ['TOTAL_TRX', 'TOTAL_SV', 'TOTAL_FBI']:
        noise = np.random.uniform(0.95, 1.05) # Efek random naik turun 5%
        df_new[col] = (df_new[col] * discount_rate * noise).round(2)
        
    return df_new

# Tarik data 2025 (Kira-kira performanya 80% dari tahun 2026)
df_2025 = create_history_data(df_card, 2025, 0.80)

# Tarik data 2024 (Kira-kira performanya 65% dari tahun 2026)
df_2024 = create_history_data(df_card, 2024, 0.65)

# Gabungkan semuanya jadi 1 tabel raksasa
df_final = pd.concat([df_card, df_2025, df_2024], ignore_index=True)

# ==========================================
# TAHAP 3: MENAMBAHKAN METADATA ENTERPRISE (Oracle)
# ==========================================
print("Menambahkan kolom Metadata Enterprise...")

# Metadata Standar
df_final['EXTRACT_BATCH_ID'] = 'CARD_BATCH_' + datetime.now().strftime('%Y%m%d')
df_final['SOURCE_SYSTEM'] = 'EDW_ORACLE_PROD'
df_final['EDW_FETCH_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_final['IS_PROCESSED_BY_ETL'] = 0

# Mengatur urutan kolom agar cantik dilihat (Merchant -> Transaksi -> Total -> Metadata)
ordered_cols = [
    'TRANSACTION_MONTH', 'MERCHANT_GROUP', 'MERCHANT_BRAND',
    
    'TRX_DEBIT_ONUS', 'TRX_DEBIT_OFFUS', 'TRX_CREDIT_OFFUS', 'TRX_QRIS_ONUS', 'TRX_QRIS_OFFUS', 'TOTAL_TRX',
    'VOL_DEBIT_ONUS', 'VOL_DEBIT_OFFUS', 'VOL_CREDIT_OFFUS', 'VOL_QRIS_ONUS', 'VOL_QRIS_OFFUS', 'TOTAL_SV',
    'FBI_DEBIT_ONUS', 'FBI_DEBIT_OFFUS', 'FBI_CREDIT_OFFUS', 'FBI_QRIS_ONUS', 'FBI_QRIS_OFFUS', 'TOTAL_FBI',
    
    'EXTRACT_BATCH_ID', 'SOURCE_SYSTEM', 'EDW_FETCH_DATE', 'IS_PROCESSED_BY_ETL'
]
df_final = df_final[ordered_cols]

print(f"Total baris sekarang (Setelah Diperbanyak): {len(df_final)} baris.")

# ==========================================
# TAHAP 4: MENYIMPAN KE STAGING DB
# ==========================================
print("\nMenyimpan ke dalam staging.db...")
conn = sqlite3.connect(PATH_DB)

# Membuat tabel baru bernama 'src_card_share_fetch'
df_final.to_sql('src_card_share_fetch', conn, if_exists='replace', index=False)
conn.close()

print("✅ Berhasil! Tabel 'src_card_share_fetch' sudah di-upgrade, ditambahkan kolom totalnya, dan datanya dilipatgandakan.")