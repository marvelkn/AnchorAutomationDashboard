import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ==========================================
# SETUP PATH OTOMATIS
# ==========================================
# 1. Posisi script ini berada (folder 'database')
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

# 2. Mundur satu folder ke folder utama ('Project')
PROJECT_DIR = os.path.dirname(BASE_DIR) 

# 3. Path ke database (tetap di dalam folder 'database')
PATH_DB = os.path.join(BASE_DIR, "staging.db")

# 4. Path ke file CSV mentah (Maju ke folder data -> raw)
PATH_MID_1 = os.path.join(PROJECT_DIR, "data", "raw", "MID_NULL_20260316.csv")
PATH_MID_2 = os.path.join(PROJECT_DIR, "data", "raw", "MID_NULL_2026.csv")
# ==========================================

print(f"Mencari file 1 di: {PATH_MID_1}")
print(f"Mencari file 2 di: {PATH_MID_2}")

try:
    print("\nMembaca file CSV MID mentah...")
    df1 = pd.read_csv(PATH_MID_1)
    df2 = pd.read_csv(PATH_MID_2)
except FileNotFoundError as e:
    print(f"\n❌ ERROR: File tidak ditemukan! Pastikan file CSV benar-benar ada di folder 'data/raw'.\nDetail: {e}")
    exit()

# Menggabungkan dan membersihkan duplikat
df_mid = pd.concat([df1, df2], ignore_index=True).drop_duplicates(subset=['MERCHANT_ID'])

print(f"Total Merchant Unik: {len(df_mid)} baris.")
print("Membangun kolom-kolom Enterprise EDW...")

# 2. Menambahkan TERMINAL_ID (TID - 8 Digit unik)
np.random.seed(42)
df_mid['TERMINAL_ID'] = np.random.randint(10000000, 99999999, size=len(df_mid)).astype(str)

# 3. Menambahkan BRANCH_CODE
branches = ['001', '014', '099', '120', '305']
df_mid['BRANCH_CODE'] = np.random.choice(branches, size=len(df_mid), p=[0.4, 0.2, 0.2, 0.1, 0.1])

# 4. Menambahkan MCC
mccs = ['5411', '5812', '5814', '5912', '5311', '8011', '5541'] 
df_mid['MCC'] = np.random.choice(mccs, size=len(df_mid))

# 5. Mengekstrak CITY
df_mid['CITY'] = df_mid['MERCHANT_NAME'].str.extract(r'([A-Z\s\(\)]+)\s+ID$')[0].str.strip()
df_mid['CITY'] = df_mid['CITY'].fillna('UNKNOWN')

# 6. Menambahkan INSTALLATION_DATE
def random_date(start, end):
    return start + timedelta(days=np.random.randint(0, int((end - start).days)))

start_date = datetime(2024, 1, 1)
end_date = datetime(2026, 3, 1)
df_mid['INSTALLATION_DATE'] = [random_date(start_date, end_date).strftime('%Y-%m-%d') for _ in range(len(df_mid))]

# 7. Menambahkan TERMINAL_STATUS
df_mid['TERMINAL_STATUS'] = np.random.choice(['ACTIVE', 'INACTIVE'], size=len(df_mid), p=[0.95, 0.05])

# 8. Menambahkan Metadata Waktu Tarik Database
df_mid['EDW_FETCH_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_mid['IS_PROCESSED_BY_ETL'] = 0 

# Mengatur urutan kolom agar rapi
cols = [
    'MERCHANT_ID', 'TERMINAL_ID', 'MERCHANT_NAME', 'EQUIP', 'MCC', 'CITY', 
    'BRANCH_CODE', 'INSTALLATION_DATE', 'TERMINAL_STATUS', 
    'EDW_FETCH_DATE', 'IS_PROCESSED_BY_ETL'
]
df_mid = df_mid[cols]

# Menyimpan ke SQLite
print("\nMenyimpan ke dalam staging.db...")
conn = sqlite3.connect(PATH_DB)
df_mid.to_sql('src_mid_fetch', conn, if_exists='replace', index=False)
conn.close()
print("✅ Berhasil! Tabel 'src_mid_fetch' sudah di-upgrade dengan kolom profesional.")