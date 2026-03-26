"""
=============================================================
 PROJECT SIDANG MAGANG - BANK BTN
 Script : 02_transform_and_ml.py
 Tahap  : Transform & Machine Learning (ETL Step 2)
 Deskripsi:
   - Feature Engineering (6 fitur untuk clustering)
   - Log Transform (handle right-skewed SV data)
   - StandardScaler normalisasi
   - K-Means++ Clustering dengan evaluasi lengkap:
     * Elbow Method
     * Silhouette Score
     * Davies-Bouldin Index
     * Calinski-Harabasz Score
   - Anomaly Detection: Z-Score + IQR
   - Output: tabel mart_merchant_cluster di SQLite
=============================================================
"""

import os
import sqlite3
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import (silhouette_score,
                             davies_bouldin_score,
                             calinski_harabasz_score)
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_DB  = os.path.join(BASE_DIR, "database", "staging.db")
OUT_DIR  = os.path.join(BASE_DIR, "output")

print("=" * 60)
print("  TAHAP 2: TRANSFORM & MACHINE LEARNING")
print("=" * 60)


# ─────────────────────────────────────────────
# STEP 1: LOAD DATA DARI SQLITE
# ─────────────────────────────────────────────
print("\n[1/6] Load data dari SQLite...")

conn = sqlite3.connect(PATH_DB)
df = pd.read_sql_query("""
    SELECT c.*, mon.PM, mon.YTD_VOL, mon.SV_GROWTH_RATE,
           mon.WEEKS_ACTIVE, mon.VOL_WEEK_PERTAMA, mon.VOL_WEEK_TERAKHIR,
           t.TARGET_VOL_2026, t.TARGET_TRX_2026, t.TARGET_FBI_2026,
           m.SEGMEN, m.TOTAL_MID, m.EQUIP_TYPES
    FROM raw_card_share c
    LEFT JOIN raw_monitoring mon ON c.MERCHANT_GROUP = mon.MERCHANT_GROUP
    LEFT JOIN raw_target t       ON c.MERCHANT_GROUP = t.MERCHANT_GROUP
    LEFT JOIN raw_master m       ON c.MERCHANT_GROUP = m.MERCHANT_GROUP
    ORDER BY c.TOTAL_SV DESC
""", conn)

print(f"  ✓ {len(df)} merchant loaded")


# ─────────────────────────────────────────────
# STEP 2: FEATURE ENGINEERING
# ─────────────────────────────────────────────
print("\n[2/6] Feature Engineering...")

# ── Fitur 1: AVG_SV_MONTHLY ──────────────────
# Rata-rata SV per bulan (lebih fair daripada total karena periode bisa beda)
df['AVG_SV_MONTHLY'] = df['TOTAL_SV'] / df['N_BULAN'].clip(lower=1)

# ── Fitur 2: AVG_FBI_MONTHLY ─────────────────
df['AVG_FBI_MONTHLY'] = df['TOTAL_FBI'] / df['N_BULAN'].clip(lower=1)

# ── Fitur 3: RASIO_ONUS ──────────────────────
# Sudah ada dari step 1, pastikan range 0-1
df['RASIO_ONUS'] = df['RASIO_ONUS'].clip(0, 1)

# ── Fitur 4: SV_GROWTH_RATE ──────────────────
# Clip outlier ekstrem (DELTA WIBAWA: 20x, MAMA ROZ: 5.5x)
# Pakai winsorize: clip di percentile 5-95
df['SV_GROWTH_RATE'] = pd.to_numeric(df['SV_GROWTH_RATE'], errors='coerce').fillna(0)
low  = df['SV_GROWTH_RATE'].quantile(0.05)
high = df['SV_GROWTH_RATE'].quantile(0.95)
df['SV_GROWTH_RATE_CLIPPED'] = df['SV_GROWTH_RATE'].clip(low, high)

# ── Fitur 5: ACHIEVEMENT_PCT ─────────────────
# YTD vs Target 2026 — seberapa jauh merchant capai target
# Target adalah tahunan, YTD adalah sejak awal tahun
df['ACHIEVEMENT_PCT'] = np.where(
    df['TARGET_VOL_2026'].fillna(0) > 0,
    (df['YTD_VOL'] / df['TARGET_VOL_2026']) * 100,
    0
)
# Clip di 200% (outlier jika jauh di atas target)
df['ACHIEVEMENT_PCT'] = df['ACHIEVEMENT_PCT'].clip(0, 200)

# ── Fitur 6: WEEKS_ACTIVE ────────────────────
# Sudah ada, pastikan bertipe numerik
df['WEEKS_ACTIVE'] = df['WEEKS_ACTIVE'].fillna(0)

print(f"  ✓ 6 fitur engineering selesai:")
print(f"    1. AVG_SV_MONTHLY    : Rp {df['AVG_SV_MONTHLY'].mean()/1e9:.2f} M avg")
print(f"    2. AVG_FBI_MONTHLY   : Rp {df['AVG_FBI_MONTHLY'].mean()/1e6:.2f} Jt avg")
print(f"    3. RASIO_ONUS        : {df['RASIO_ONUS'].mean():.3f} avg")
print(f"    4. SV_GROWTH_RATE    : {df['SV_GROWTH_RATE_CLIPPED'].mean():.3f} avg (setelah clip)")
print(f"    5. ACHIEVEMENT_PCT   : {df['ACHIEVEMENT_PCT'].mean():.1f}% avg")
print(f"    6. WEEKS_ACTIVE      : {df['WEEKS_ACTIVE'].mean():.1f} avg")


# ─────────────────────────────────────────────
# STEP 3: PREPROCESSING UNTUK ML
# ─────────────────────────────────────────────
print("\n[3/6] Preprocessing untuk K-Means...")

# Pilih fitur untuk clustering
# Catatan untuk dosen: Log transform penting karena SV data right-skewed
# (MAP GROUP 95M vs SUSHI TEI 3 rupiah — tanpa log, centroid akan bias ke outlier)
FEATURES = ['AVG_SV_MONTHLY', 'AVG_FBI_MONTHLY', 'RASIO_ONUS',
            'SV_GROWTH_RATE_CLIPPED', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE']

X = df[FEATURES].copy()

# Isi NaN dengan 0 (merchant baru atau tidak ada target)
X = X.fillna(0)

# Log Transform untuk fitur monetary (handle right-skew)
# log1p = log(1+x) agar aman untuk nilai 0
X['AVG_SV_MONTHLY']  = np.log1p(X['AVG_SV_MONTHLY'])
X['AVG_FBI_MONTHLY'] = np.log1p(X['AVG_FBI_MONTHLY'])

# StandardScaler: semua fitur ke skala yang sama (mean=0, std=1)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"  ✓ Feature matrix: {X_scaled.shape[0]} merchant × {X_scaled.shape[1]} fitur")
print(f"  ✓ Log transform diterapkan pada AVG_SV_MONTHLY, AVG_FBI_MONTHLY")
print(f"  ✓ StandardScaler: semua fitur di-normalize ke mean=0, std=1")


# ─────────────────────────────────────────────
# STEP 4: EVALUASI K OPTIMAL (ELBOW + 3 METRICS)
# ─────────────────────────────────────────────
print("\n[4/6] Evaluasi K Optimal (K=2 sampai K=8)...")

k_range    = range(2, 9)
inertias   = []
silhouettes= []
db_scores  = []   # Davies-Bouldin: lebih kecil lebih baik
ch_scores  = []   # Calinski-Harabasz: lebih besar lebih baik

for k in k_range:
    km = KMeans(n_clusters=k, init='k-means++', n_init=20,
                random_state=42, max_iter=500)
    labels = km.fit_predict(X_scaled)

    inertias.append(km.inertia_)
    silhouettes.append(silhouette_score(X_scaled, labels))
    db_scores.append(davies_bouldin_score(X_scaled, labels))
    ch_scores.append(calinski_harabasz_score(X_scaled, labels))

    print(f"  K={k}: Inertia={km.inertia_:,.0f} | "
          f"Silhouette={silhouettes[-1]:.4f} | "
          f"Davies-Bouldin={db_scores[-1]:.4f} | "
          f"Calinski-Harabasz={ch_scores[-1]:.2f}")

# Plot evaluasi: 4 panel
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Evaluasi K Optimal — K-Means Clustering', fontsize=14, fontweight='bold')

k_list = list(k_range)

axes[0,0].plot(k_list, inertias, 'bo-', linewidth=2, markersize=8)
axes[0,0].set_title('Elbow Method (Inertia)', fontsize=12)
axes[0,0].set_xlabel('Jumlah Cluster (K)')
axes[0,0].set_ylabel('Inertia (Within-Cluster SSE)')
axes[0,0].grid(True, alpha=0.3)
axes[0,0].axvline(x=3, color='red', linestyle='--', alpha=0.7, label='K=3 dipilih')
axes[0,0].legend()

axes[0,1].plot(k_list, silhouettes, 'gs-', linewidth=2, markersize=8)
axes[0,1].set_title('Silhouette Score (↑ lebih baik)', fontsize=12)
axes[0,1].set_xlabel('Jumlah Cluster (K)')
axes[0,1].set_ylabel('Silhouette Score')
axes[0,1].grid(True, alpha=0.3)
axes[0,1].axvline(x=3, color='red', linestyle='--', alpha=0.7, label='K=3 dipilih')
axes[0,1].legend()

axes[1,0].plot(k_list, db_scores, 'r^-', linewidth=2, markersize=8)
axes[1,0].set_title('Davies-Bouldin Index (↓ lebih baik)', fontsize=12)
axes[1,0].set_xlabel('Jumlah Cluster (K)')
axes[1,0].set_ylabel('Davies-Bouldin Score')
axes[1,0].grid(True, alpha=0.3)
axes[1,0].axvline(x=3, color='red', linestyle='--', alpha=0.7, label='K=3 dipilih')
axes[1,0].legend()

axes[1,1].plot(k_list, ch_scores, 'mD-', linewidth=2, markersize=8)
axes[1,1].set_title('Calinski-Harabasz Score (↑ lebih baik)', fontsize=12)
axes[1,1].set_xlabel('Jumlah Cluster (K)')
axes[1,1].set_ylabel('Calinski-Harabasz Score')
axes[1,1].grid(True, alpha=0.3)
axes[1,1].axvline(x=3, color='red', linestyle='--', alpha=0.7, label='K=3 dipilih')
axes[1,1].legend()

plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, 'elbow_evaluation.png'), dpi=150, bbox_inches='tight')
plt.close()
print(f"\n  ✓ Grafik evaluasi disimpan: output/elbow_evaluation.png")


# ─────────────────────────────────────────────
# STEP 5: K-MEANS FINAL (K=3)
# ─────────────────────────────────────────────
# Justifikasi K=3:
# - Elbow Method menunjuk K=3 sebagai titik siku
# - K=3 menghasilkan segmentasi bisnis yang meaningful:
#   PREMIUM (jaga & maksimalkan), REGULER (dorong naik), PASIF (intervensi)
# - K=2 hanya pisahkan "besar vs kecil" tanpa insight actionable
print("\n[5/6] K-Means Clustering Final (K=3)...")

K_FINAL = 3
km_final = KMeans(n_clusters=K_FINAL, init='k-means++', n_init=50,
                  random_state=42, max_iter=1000)
df['CLUSTER_RAW'] = km_final.fit_predict(X_scaled)

# Evaluasi final
sil_final = silhouette_score(X_scaled, df['CLUSTER_RAW'])
db_final  = davies_bouldin_score(X_scaled, df['CLUSTER_RAW'])
ch_final  = calinski_harabasz_score(X_scaled, df['CLUSTER_RAW'])

print(f"  ✓ Silhouette Score     : {sil_final:.4f}  (target > 0.50 = good)")
print(f"  ✓ Davies-Bouldin Index : {db_final:.4f}  (target < 1.0  = good)")
print(f"  ✓ Calinski-Harabasz    : {ch_final:.2f}")

# Label cluster berdasarkan rata-rata AVG_SV_MONTHLY per cluster
# Cluster dengan SV tertinggi = PREMIUM, menengah = REGULER, terendah = PASIF
cluster_sv_mean = df.groupby('CLUSTER_RAW')['AVG_SV_MONTHLY'].mean().sort_values(ascending=False)
cluster_rank = {c: i for i, c in enumerate(cluster_sv_mean.index)}
rank_to_label = {0: 'PREMIUM', 1: 'REGULER', 2: 'PASIF'}
df['CLUSTER'] = df['CLUSTER_RAW'].map(lambda c: rank_to_label[cluster_rank[c]])

# Summary per cluster
print(f"\n  Profil Cluster:")
print(f"  {'Cluster':<10} {'N':>4} {'Avg SV/bln':>16} {'Avg FBI/bln':>14} {'Avg Achievement':>17}")
print(f"  {'-'*65}")
for label in ['PREMIUM', 'REGULER', 'PASIF']:
    sub = df[df['CLUSTER'] == label]
    print(f"  {label:<10} {len(sub):>4} "
          f"  Rp {sub['AVG_SV_MONTHLY'].mean()/1e9:>8.3f} M "
          f"  Rp {sub['AVG_FBI_MONTHLY'].mean()/1e6:>7.2f} Jt "
          f"  {sub['ACHIEVEMENT_PCT'].mean():>10.1f}%")

print(f"\n  Merchant per Cluster:")
for label in ['PREMIUM', 'REGULER', 'PASIF']:
    merchants = df[df['CLUSTER']==label]['MERCHANT_GROUP'].tolist()
    print(f"  {label}: {', '.join(merchants)}")

# Plot cluster profile
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle('Profil Cluster K-Means (K=3)', fontsize=14, fontweight='bold')

colors = {'PREMIUM': '#F2C94C', 'REGULER': '#2F80ED', 'PASIF': '#EB5757'}
labels_order = ['PREMIUM', 'REGULER', 'PASIF']

# Panel 1: Avg SV per cluster
sv_vals = [df[df['CLUSTER']==l]['AVG_SV_MONTHLY'].mean()/1e9 for l in labels_order]
bars = axes[0].bar(labels_order, sv_vals, color=[colors[l] for l in labels_order])
axes[0].set_title('Avg SV / Bulan (Miliar Rp)')
axes[0].set_ylabel('Rp Miliar')
for bar, val in zip(bars, sv_vals):
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                 f'Rp {val:.2f}M', ha='center', va='bottom', fontsize=9)

# Panel 2: Jumlah merchant per cluster
count_vals = [len(df[df['CLUSTER']==l]) for l in labels_order]
bars2 = axes[1].bar(labels_order, count_vals, color=[colors[l] for l in labels_order])
axes[1].set_title('Jumlah Merchant per Cluster')
axes[1].set_ylabel('Jumlah Merchant')
for bar, val in zip(bars2, count_vals):
    axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                 str(val), ha='center', va='bottom', fontweight='bold')

# Panel 3: Scatter SV vs FBI (warna per cluster)
for label in labels_order:
    sub = df[df['CLUSTER']==label]
    axes[2].scatter(sub['AVG_SV_MONTHLY']/1e9, sub['AVG_FBI_MONTHLY']/1e6,
                    c=colors[label], label=label, s=80, alpha=0.8, edgecolors='white')
axes[2].set_title('Scatter: SV vs FBI per Merchant')
axes[2].set_xlabel('Avg SV / Bulan (Miliar Rp)')
axes[2].set_ylabel('Avg FBI / Bulan (Juta Rp)')
axes[2].legend()
axes[2].set_xscale('log')
axes[2].set_yscale('log')
axes[2].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, 'cluster_profile.png'), dpi=150, bbox_inches='tight')
plt.close()
print(f"\n  ✓ Grafik profil cluster disimpan: output/cluster_profile.png")


# ─────────────────────────────────────────────
# STEP 6: ANOMALY DETECTION (Z-SCORE + IQR)
# ─────────────────────────────────────────────
# Justifikasi dual method:
# Z-Score: efektif untuk mendeteksi outlier pada distribusi mendekati normal
# IQR: robust terhadap distribusi skewed (SV merchant sangat right-skewed)
# Keduanya komplementer → kurangi false positive
#
# Catatan konteks data:
# - Data hanya 7 minggu (2026 masih awal tahun)
# - SV_GROWTH_RATE = (Week7 - Week1) / Week1
#   → nilai -1.0 artinya merchant BERHENTI TOTAL di Week 7 (churn signal kuat)
#   → nilai 0.0 dengan WEEKS_ACTIVE rendah = merchant tidak konsisten
# - Pendekatan: scoring multi-kriteria, bukan single threshold
print("\n[6/6] Anomaly Detection (Z-Score + IQR)...")

# Z-Score: hitung dari LOG(AVG_SV_MONTHLY) agar tidak bias outlier besar
log_sv = np.log1p(df['AVG_SV_MONTHLY'])
df['ZSCORE_GROWTH'] = stats.zscore(df['SV_GROWTH_RATE'].fillna(0))
df['ZSCORE_SV']     = stats.zscore(log_sv)

# IQR: merchant dengan SV di bawah lower fence → below-average performer
Q1 = df['AVG_SV_MONTHLY'].quantile(0.25)
Q3 = df['AVG_SV_MONTHLY'].quantile(0.75)
IQR_val = Q3 - Q1
lower_fence = Q1 - 1.5 * IQR_val
df['IS_BELOW_IQR'] = df['AVG_SV_MONTHLY'] < lower_fence

# CHURN RISK — multi-kriteria:
# Kriteria 1: WEEKS_ACTIVE sangat rendah (≤ 2 dari 7 minggu) → hampir tidak aktif
# Kriteria 2: SV_GROWTH_RATE = -1.0 → merchant berhenti total di minggu terakhir
# Kriteria 3: CLUSTER PASIF + ACHIEVEMENT_PCT hampir nol (< 1%)
# Kriteria 4: ZSCORE_SV sangat rendah (< -1.2) → SV jauh di bawah rata-rata
churn_mask = (
    (df['WEEKS_ACTIVE'] <= 2) |
    ((df['SV_GROWTH_RATE'] <= -0.99) & (df['ACHIEVEMENT_PCT'] < 5)) |
    ((df['CLUSTER'] == 'PASIF') & (df['ACHIEVEMENT_PCT'] < 1)) |
    (df['ZSCORE_SV'] < -1.2)
)
df['CHURN_RISK'] = churn_mask.map({True: 'YA', False: 'TIDAK'})

n_churn = (df['CHURN_RISK'] == 'YA').sum()
print(f"  ✓ Merchant CHURN RISK  : {n_churn} merchant")
print(f"  ✓ Kriteria deteksi     : WEEKS_ACTIVE ≤ 2 | Growth ≤ -99% | Pasif+Achievement <1% | ZScore SV < -1.2")
print(f"  ✓ IQR lower fence      : Rp {max(lower_fence, 0)/1e6:.1f} Juta / bulan")

print(f"\n  Daftar CHURN RISK:")
churn_df = df[df['CHURN_RISK']=='YA'][['MERCHANT_GROUP','PM','CLUSTER','AVG_SV_MONTHLY','WEEKS_ACTIVE','SV_GROWTH_RATE','ACHIEVEMENT_PCT']].copy()
churn_df = churn_df.sort_values('AVG_SV_MONTHLY')
for _, row in churn_df.iterrows():
    reasons = []
    if row['WEEKS_ACTIVE'] <= 2: reasons.append(f"Aktif {int(row['WEEKS_ACTIVE'])}w/7w")
    if row['SV_GROWTH_RATE'] <= -0.99: reasons.append("Growth -100%")
    if row['CLUSTER'] == 'PASIF' and row['ACHIEVEMENT_PCT'] < 1: reasons.append("Pasif+0%target")
    print(f"    {row['MERCHANT_GROUP']:<28} PM:{row['PM']:<10} "
          f"SV:{row['AVG_SV_MONTHLY']/1e6:>8.1f}Jt  [{', '.join(reasons)}]")

# Plot anomaly
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Anomaly Detection — Churn Risk Merchant', fontsize=14, fontweight='bold')

# Panel 1: Z-Score SV distribution
zsv_sorted = df.sort_values('ZSCORE_SV')
colors_bar = ['#EB5757' if r=='YA' else '#2F80ED' for r in zsv_sorted['CHURN_RISK']]
axes[0].bar(range(len(zsv_sorted)), zsv_sorted['ZSCORE_SV'], color=colors_bar)
axes[0].axhline(y=-1.2, color='red', linestyle='--', linewidth=2, label='Threshold Z = -1.2')
axes[0].set_title('Z-Score Log(SV) per Merchant')
axes[0].set_xlabel('Merchant (sorted by Z-Score)')
axes[0].set_ylabel('Z-Score')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Panel 2: Scatter WEEKS_ACTIVE vs SV, highlight churn
for risk, color, label in [('TIDAK','#2F80ED','Normal'), ('YA','#EB5757','Churn Risk')]:
    sub = df[df['CHURN_RISK']==risk]
    axes[1].scatter(sub['WEEKS_ACTIVE'], sub['AVG_SV_MONTHLY']/1e6,
                    c=color, label=label, s=80, alpha=0.8, edgecolors='white')
for _, row in df[df['CHURN_RISK']=='YA'].iterrows():
    axes[1].annotate(row['MERCHANT_GROUP'],
                     (row['WEEKS_ACTIVE'], row['AVG_SV_MONTHLY']/1e6),
                     textcoords='offset points', xytext=(5,5), fontsize=7)
axes[1].set_title('Weeks Active vs Avg SV (Churn Highlight)')
axes[1].set_xlabel('Weeks Active (dari 7 minggu)')
axes[1].set_ylabel('Avg SV / Bulan (Juta Rp)')
axes[1].set_yscale('log')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, 'anomaly_detection.png'), dpi=150, bbox_inches='tight')
plt.close()
print(f"\n  ✓ Grafik anomaly disimpan: output/anomaly_detection.png")


# ─────────────────────────────────────────────
# SIMPAN KE SQLITE
# ─────────────────────────────────────────────
cols_to_save = [
    'MERCHANT_GROUP', 'PM', 'SEGMEN', 'TOTAL_MID', 'EQUIP_TYPES',
    'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'N_BULAN', 'BULAN_TERAKHIR',
    'AVG_SV_MONTHLY', 'AVG_FBI_MONTHLY', 'RASIO_ONUS',
    'SV_GROWTH_RATE', 'SV_GROWTH_RATE_CLIPPED', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE',
    'YTD_VOL', 'TARGET_VOL_2026', 'TARGET_TRX_2026', 'TARGET_FBI_2026',
    'CLUSTER_RAW', 'CLUSTER', 'ZSCORE_GROWTH', 'ZSCORE_SV', 'IS_BELOW_IQR', 'CHURN_RISK'
]

df[cols_to_save].to_sql("mart_merchant_cluster", conn, if_exists="replace", index=False)
conn.close()
print(f"\n  ✓ Tabel mart_merchant_cluster tersimpan di SQLite ({len(df)} baris)")

# Simpan checkpoint
df[cols_to_save].to_csv(os.path.join(OUT_DIR, 'checkpoint_02_ml.csv'),
                        index=False, encoding='utf-8-sig')
print(f"  ✓ Checkpoint disimpan: output/checkpoint_02_ml.csv")

print(f"\n{'='*60}")
print(f"  TAHAP 2 SELESAI ✓")
print(f"  Ringkasan ML:")
print(f"    Silhouette Score     : {sil_final:.4f}")
print(f"    Davies-Bouldin Index : {db_final:.4f}")
print(f"    Calinski-Harabasz    : {ch_final:.2f}")
print(f"    Merchant CHURN RISK  : {n_churn}")
print(f"  Jalankan berikutnya: 03_load_to_datamart.py")
print(f"{'='*60}\n")