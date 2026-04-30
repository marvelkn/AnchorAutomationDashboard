# ML Presentation Prep — BTN Anchor Automation Dashboard

## Prompt to paste at the start of a new chat

```
I am preparing a presentation about the Machine Learning pipeline inside our BTN Anchor
Automation Dashboard — a Streamlit web app that analyses a bank's anchor merchant portfolio
for churn risk and segmentation.

I need you to help me explain the ML clearly for a mixed audience (technical stakeholders +
business people who know basic ML terms but not data science math).

Below is the full context of the pipeline. Please be ready to:
1. Explain each step in plain language when I ask
2. Help me answer likely audience questions
3. Suggest how to present each concept visually or verbally

--- PIPELINE CONTEXT ---

The ML runs inside a single cached function: run_ml() in pages/4_Dashboard.py
It takes 3 inputs (already processed by the ETL pipeline):
  - PROCESSED_CARD_SHARE  → TOTAL_SV, TOTAL_TRX, TOTAL_FBI, RASIO_ONUS (YTD totals)
  - PROCESSED_MONITORING  → PM, WEEKS_ACTIVE, SV_GROWTH_RATE
  - TARGET                → MERCHANT_GROUP, TARGET_VOL_2026

Portfolio size: ~38 anchor merchants (small portfolio, so robustness matters a lot)


── STAGE 1: DATA MERGE ──────────────────────────────────────────────────────────────────
The three tables are LEFT JOINed on MERCHANT_GROUP.
Result: one row per merchant containing all columns.


── STAGE 2: FEATURE ENGINEERING ────────────────────────────────────────────────────────
The ETL only stores raw YTD totals. These can't go directly into ML because merchants
have been active for different durations — a merchant active 3 months looks worse than
one active 12 months even if their monthly rate is higher.

New derived columns calculated:

  AVG_SV         = TOTAL_SV / months_active
                   (months_active = WEEKS_ACTIVE / 4.33, clipped to 1–12)

  AVG_FBI        = TOTAL_FBI / months_active

  SV_GROWTH_CLIPPED = SV_GROWTH_RATE clipped at [5th percentile, 95th percentile]
                      (removes extreme outliers that would distort the model)

  ACHIEVEMENT_PCT = (TOTAL_SV / TARGET_VOL_2026) × 100, clipped to 0–200%

  RASIO_ONUS     = clipped to [0, 1], already in Card Share

  WEEKS_ACTIVE   = clipped to [1, 52]

Then: AVG_SV and AVG_FBI are log-transformed (log1p) to compress large value ranges.
Then: All 6 features are Z-normalised with StandardScaler so they are on the same scale.

Final 6 features fed to all ML models:
  [AVG_SV, AVG_FBI, RASIO_ONUS, SV_GROWTH_CLIPPED, ACHIEVEMENT_PCT, WEEKS_ACTIVE]


── STAGE 3a: K-MEANS++ CLUSTERING ──────────────────────────────────────────────────────
Algorithm: K-Means++ (smarter centroid initialisation than vanilla K-Means)
K: 3, 4, or 5 — user-selectable slider in the dashboard
n_init: 20 runs, keeps the best result
random_state: 42 (reproducible)

What it does:
  Groups merchants into K clusters based on their 6 scaled features.
  The algorithm finds natural groupings — it is not told what the groups should look like.

Cluster ranking (so labels are stable and business-meaningful):
  After clustering, each cluster is ranked by a composite score:
    SV 60% + Achievement 25% + Growth 15%
  The highest-scoring cluster gets the top label, and so on.

Label maps:
  K=3 → PREMIUM, REGULER, PASIF
  K=4 → ELITE, PREMIUM, REGULER, PASIF
  K=5 → ELITE, PREMIUM, REGULER, PASIF, DORMANT

Quality check: Silhouette Score (-1 to 1)
  > 0.5  = strong cluster separation (K is a good fit)
  0.25–0.5 = moderate
  < 0.25 = weak (try a different K)

Outputs added to each merchant row:
  CLUSTER          → tier label (e.g. PREMIUM)
  SILHOUETTE_SCORE → cluster quality metric


── STAGE 3b: ISOLATION FOREST ──────────────────────────────────────────────────────────
Algorithm: Isolation Forest (Liu et al., 2008)
n_estimators: 100 random trees
contamination: 0.10 (expects ~10% of portfolio to be anomalous, ~3–4 merchants)
random_state: 42, n_jobs: -1

What it does:
  Detects merchants that are statistically unusual across ALL 6 features simultaneously.
  Core idea: anomalies are easier to isolate — they need fewer random splits in a decision
  tree to be separated from the rest. Shorter average path length = anomaly.

  This catches patterns that single-metric thresholds miss. For example: a merchant whose
  sales volume looks normal BUT has an unusual combination of low growth + high on-us ratio
  + low activity weeks — the combination is anomalous even if each number looks OK alone.

LOFO Feature Contribution (Leave-One-Feature-Out):
  For each of the 6 features, the feature is neutralised (set to 0 = portfolio mean in
  scaled space) and the model is re-scored WITHOUT re-fitting.
  Delta = base_score − neutralised_score
  Positive delta = that feature is driving the anomaly.
  This makes the Isolation Forest explainable — the PM can see exactly which metric
  is responsible for flagging a merchant.

Outputs added to each merchant row:
  IF_ANOMALY_SCORE      → continuous anomaly score (higher = more anomalous)
  IF_IS_ANOMALY         → boolean flag (top ~10% flagged as True)
  IF_CONTRIB_AVG_SV     → LOFO contribution of sales volume
  IF_CONTRIB_AVG_FBI    → LOFO contribution of fee income
  IF_CONTRIB_RASIO_ONUS → LOFO contribution of on-us ratio
  IF_CONTRIB_SV_GROWTH  → LOFO contribution of growth rate
  IF_CONTRIB_ACHIEVEMENT→ LOFO contribution of target achievement
  IF_CONTRIB_WEEKS_ACTIVE→ LOFO contribution of activity weeks


── STAGE 4: MAD Z-SCORE ────────────────────────────────────────────────────────────────
Formula: Z = 0.6745 × (x − median) / MAD
Where MAD = Median Absolute Deviation = median(|x − median(x)|)

Why MAD instead of standard Z-Score:
  Standard Z-Score uses the mean, which is pulled by outliers.
  In a small 38-merchant portfolio, one extreme merchant can shift the mean significantly.
  MAD uses the median, which is resistant to outliers — making it more reliable here.

Applied to three dimensions (log-transformed):
  ZSCORE_SV     ← log(AVG_SV)
  ZSCORE_FBI    ← log(AVG_FBI)
  ZSCORE_GROWTH ← SV_GROWTH_CLIPPED

A negative Z-Score means the merchant is below the portfolio median for that metric.


── STAGE 5: COMPOSITE RISK SCORE ───────────────────────────────────────────────────────
Formula:
  RISK_SCORE =
    clip(-ZSCORE_GROWTH, 0, 3) / 3 × 40   ← Growth trend (40%)
  + clip(-ZSCORE_SV,     0, 3) / 3 × 30   ← Sales volume anomaly (30%)
  + clip(-ZSCORE_FBI,    0, 3) / 3 × 20   ← Fee income anomaly (20%)
  + clip(1 − ACHIEVEMENT_PCT/100, 0, 1)   × 10   ← Target gap (10%)

Result is clipped to [0, 100].
Higher = more risk. 0 = perfect, 100 = critical.

Weight rationale:
  Growth trend is the strongest churn predictor (declining trajectory = leaving soon).
  Volume anomaly is second (dropping sales = losing business).
  FBI follows volume. Target gap is last (some merchants have no targets set).


── STAGE 6: CHURN RISK CLASSIFICATION ──────────────────────────────────────────────────
Primary rule (risk score):
  RISK_SCORE ≥ 60  → HIGH RISK ⚠️
  RISK_SCORE 30–59 → MEDIUM RISK 🟡
  RISK_SCORE < 30  → STABLE ✅

Z-Score override (added recently):
  If ANY of ZSCORE_SV, ZSCORE_FBI, ZSCORE_GROWTH < z_thresh (user-controlled slider,
  default -1.2), a STABLE merchant is upgraded to MEDIUM RISK.
  This ensures that a severe single-dimension drop is never silently ignored.

Ensemble Alert:
  Merchants flagged as HIGH RISK by the composite score AND flagged by Isolation Forest
  simultaneously are highlighted with an Ensemble Alert — dual-method confirmation
  significantly increases confidence.


── SEPARATE: HOLT-WINTERS FORECASTING ──────────────────────────────────────────────────
Runs per merchant on demand (not part of the main run_ml() function).
Uses PROCESSED_CARD_HISTORY — monthly TOTAL_SV per merchant.

Algorithm: Holt-Winters Exponential Smoothing (Winters, 1960)
  ≥ 24 months of history → trend + seasonal decomposition (seasonal_periods=12)
  6–23 months           → trend only (Holt's Double Smoothing)
  < 6 months            → linear extrapolation fallback

Parameters optimised automatically via MLE (Maximum Likelihood Estimation).

Outputs:
  - Historical + projected sales line chart
  - Seasonality multiplier curve (which months typically spike)
  - Projected year-end run rate
  - Plain-English verdict: on track / at risk / critical

--- END CONTEXT ---

Start by giving me a clean summary of the full pipeline in 5–6 sentences, as if you're
introducing it to a boardroom audience that knows what K-Means and anomaly detection are
but doesn't know the specifics of this implementation.
```

---

## Key points to cover in your presentation

### Feature Engineering
- Raw totals from the DB can't go into ML directly because merchants have different activity durations
- The key insight: normalise by `months_active`, not by a fixed /12 — a merchant active 3 months is judged on their 3-month rate
- `ACHIEVEMENT_PCT` is a brand-new column that doesn't exist anywhere in the database — it's created here
- `SV_GROWTH_CLIPPED` removes extreme outliers at the 5th and 95th percentile so one unusual merchant doesn't warp the whole model
- Log transformation on SV and FBI compresses the value range (Rp 100M and Rp 10B shouldn't be treated as linearly different)

### K-Means++
- "The model is not told what a PREMIUM merchant looks like — it finds the natural groupings by itself"
- The composite ranking (SV 60%, Achievement 25%, Growth 15%) is applied **after** clustering to give the groups stable, business-meaningful names
- Silhouette Score is how we know whether the K we chose is actually a good fit
- Key question you'll likely get: *"Why not just set manual thresholds?"* → Answer: K-Means finds groupings that exist in the actual data, not groupings that an analyst guessed might exist

### Isolation Forest
- "It finds merchants that are hard to hide in a crowd"
- Works on all 6 features **simultaneously** — catches combinations that single-metric checks miss
- The LOFO (Leave-One-Feature-Out) contribution is the key explainability mechanism — without it, the model is a black box
- Key question you'll likely get: *"Why 10% contamination?"* → Answer: in a portfolio of ~38 merchants, 10% flags ~4 merchants, which is a manageable number for PMs to investigate

### Why both K-Means AND Isolation Forest?
- K-Means segments performance tiers — it answers "which group does this merchant belong to?"
- Isolation Forest detects anomalies — it answers "is this merchant behaving unusually **within** their group?"
- A PASIF merchant that is behaving consistently is less urgent than a PREMIUM merchant that suddenly looks anomalous — the two models together capture this nuance

---

## Likely audience questions and suggested answers

| Question | Answer |
|---|---|
| Why K-Means and not a supervised model? | We have no historical labels for "churned" vs "not churned" — there is no training data. K-Means is unsupervised so it works from patterns alone. |
| What does the Silhouette Score actually mean? | It measures how similar each merchant is to their own cluster vs the nearest other cluster. Close to 1 = well-separated groups. Close to 0 = overlapping groups. |
| Why MAD Z-Score instead of regular Z-Score? | Regular Z-Score uses the mean, which is sensitive to outliers. With only 38 merchants, one extreme value can shift the mean significantly. MAD uses the median, which is resistant to outliers. |
| How often does the ML run? | Every time the Dashboard page is loaded. There are no pre-trained model files — the models are retrained fresh each session using the latest database data. Results are cached within a session for performance. |
| Can the PM override the risk label? | Not directly in the ML — but the Z-Score Tripwire slider adjusts how aggressively the system flags merchants, effectively letting the user control sensitivity. |
| What does LOFO mean? | Leave-One-Feature-Out. We temporarily remove one feature from the anomaly calculation and measure how much the anomaly score changes. A big positive change means that feature is responsible for the flag. |

---

## Presentasi — Narasi & Struktur Slide (Bahasa Indonesia)

> Bagian ini adalah skrip presentasi siap pakai. Setiap slide berisi **judul**, **poin visual yang disarankan**, dan **narasi yang bisa diucapkan langsung**.

---

### Slide 1 — Judul

**Judul:** Machine Learning Pipeline pada BTN Anchor Automation Dashboard
**Sub-judul:** Deteksi Dini Risiko Churn & Segmentasi Performa Merchant

**Narasi:**
> "Selamat pagi. Hari ini saya akan menjelaskan bagaimana sistem machine learning yang tertanam di dalam dashboard ini bekerja — mulai dari data mentah yang masuk, sampai ke label risiko yang muncul di layar PM. Saya akan fokus pada *logika di balik* setiap keputusan teknis, bukan hanya cara kerjanya secara matematis."

---

### Slide 2 — Gambaran Besar Pipeline

**Visual:** Diagram alir horizontal: `Data Mentah → Feature Engineering → K-Means++ → Isolation Forest → MAD Z-Score → Risk Score → Klasifikasi Risiko`

**Narasi:**
> "Pipeline ML ini terdiri dari enam tahap utama. Data dari tiga tabel ETL digabung menjadi satu baris per merchant, lalu diubah menjadi fitur yang bisa dibandingkan secara adil antar merchant. Setelah itu, dua model berjalan secara paralel: K-Means untuk segmentasi performa, dan Isolation Forest untuk deteksi anomali. Hasilnya digabungkan menjadi satu skor risiko komposit yang menjadi dasar klasifikasi akhir."

---

### Slide 3 — Tantangan: Data yang Tidak Bisa Langsung Dipakai

**Visual:** Tabel dua kolom — kiri: "Data Mentah (YTD Total)", kanan: "Masalah"
| Data Mentah | Mengapa Bermasalah |
|---|---|
| TOTAL_SV = Rp 5 Miliar | Merchant A aktif 12 bulan, Merchant B aktif 3 bulan — tidak bisa dibandingkan langsung |
| SV_GROWTH_RATE = 500% | Satu merchant outlier bisa mendistorsi seluruh model |
| TOTAL_SV = Rp 100M vs Rp 10T | Skala terlalu jauh — model akan terlalu fokus ke merchant besar |

**Narasi:**
> "Data yang masuk dari ETL adalah total YTD — artinya akumulasi sejak merchant mulai aktif. Merchant yang baru bergabung 3 bulan lalu akan selalu terlihat lebih kecil dari merchant yang sudah aktif setahun, padahal boleh jadi laju bulanannya lebih tinggi. Karena itu, kita tidak bisa langsung memasukkan angka mentah ke model ML."

---

### Slide 4 — Feature Engineering: Membuat Data Bisa Dibandingkan

**Visual:** Tiga transformasi dengan panah:
- `TOTAL_SV ÷ months_active` → `AVG_SV` (dinormalkan per bulan aktif)
- `log(AVG_SV)` → skala besar dikompresi
- `StandardScaler` → semua fitur pada skala yang sama

**Narasi:**
> "Ada tiga langkah utama di feature engineering. Pertama, kita bagi semua total dengan jumlah bulan merchant aktif — bukan dibagi 12 secara kaku, tapi dibagi bulan yang sebenarnya aktif. Ini membuat perbandingan menjadi adil. Kedua, kita terapkan transformasi logaritma pada volume dan fee income — karena perbedaan antara Rp 100 juta dan Rp 1 miliar secara bisnis tidak sama besarnya dengan perbedaan antara Rp 1 miliar dan Rp 10 miliar. Ketiga, semua fitur dinormalisasi ke skala yang sama agar model tidak didominasi oleh satu variabel saja."

**Poin tambahan untuk diucapkan jika ada pertanyaan:**
> "ACHIEVEMENT_PCT adalah kolom yang kita buat sendiri di sini — tidak ada di database mana pun. Ini adalah persentase pencapaian target 2026 per merchant, yang kita butuhkan sebagai sinyal seberapa jauh merchant dari ekspektasi bisnis."

---

### Slide 5 — K-Means++: Segmentasi Performa

**Visual:** Diagram scatter plot (ilustratif) dengan 3–5 kelompok berwarna berbeda, diberi label ELITE / PREMIUM / REGULER / PASIF / DORMANT

**Narasi:**
> "Model pertama adalah K-Means++. Ini adalah algoritma *unsupervised* — artinya kita tidak memberi tahu model seperti apa merchant PREMIUM itu. Model sendiri yang mencari pengelompokan alami berdasarkan enam fitur yang sudah kita siapkan. Setelah kelompok terbentuk, baru kita beri nama berdasarkan skor komposit: 60% volume penjualan, 25% pencapaian target, 15% pertumbuhan. Kelompok dengan skor tertinggi dapat label ELITE atau PREMIUM, yang terendah dapat label PASIF atau DORMANT."

**Antisipasi pertanyaan:**
> *"Kenapa tidak pakai threshold manual saja?"*
> "Threshold manual berarti kita menebak di mana batasnya. K-Means menemukan batas yang memang ada di dalam data — bukan asumsi analis."

---

### Slide 6 — Silhouette Score: Cara Kita Tahu K yang Dipilih Sudah Tepat

**Visual:** Gauge atau bar chart kecil dengan zona merah/kuning/hijau: < 0.25 Lemah | 0.25–0.5 Sedang | > 0.5 Kuat

**Narasi:**
> "Salah satu tantangan K-Means adalah memilih jumlah klaster K yang tepat. Untuk itu kita gunakan Silhouette Score — angka antara -1 dan 1 yang mengukur seberapa jelas pemisahan antar kelompok. Di atas 0.5 artinya kelompok-kelompoknya benar-benar berbeda satu sama lain. Di bawah 0.25 artinya batas antar kelompok buram — perlu coba K yang lain. PM bisa menggeser slider K di dashboard dan melihat efeknya langsung pada skor ini."

---

### Slide 7 — Isolation Forest: Deteksi Merchant yang Anomali

**Visual:** Ilustrasi pohon keputusan dengan satu titik yang "terisolasi" lebih cepat dari titik-titik lain

**Narasi:**
> "Model kedua adalah Isolation Forest. Cara kerjanya berbeda dari K-Means — alih-alih mencari kesamaan, ia mencari *ketidakbiasaan*. Bayangkan sebuah permainan: Anda memilah data dengan pertanyaan acak berulang kali. Merchant yang aneh akan 'tertangkap' lebih cepat karena ia berbeda dari yang lain. Makin sedikit pertanyaan yang dibutuhkan untuk memisahkan seorang merchant, makin anomali ia."

> "Yang penting: Isolation Forest bekerja pada *kombinasi* enam fitur sekaligus. Seorang merchant bisa terlihat normal di volume penjualan, normal di pertumbuhan, tapi kombinasi low growth + high on-us ratio + sedikit minggu aktif bisa jadi sangat tidak biasa secara keseluruhan. Itulah yang tidak bisa ditangkap oleh threshold satu dimensi."

---

### Slide 8 — LOFO: Membuat Black Box Menjadi Transparan

**Visual:** Bar chart kontribusi LOFO per fitur untuk satu merchant contoh (AVG_SV, AVG_FBI, RASIO_ONUS, dll.)

**Narasi:**
> "Masalah klasik anomaly detection adalah: model bisa mengatakan 'merchant ini aneh', tapi tidak bisa menjelaskan *mengapa*. Kita selesaikan ini dengan teknik LOFO — Leave-One-Feature-Out. Caranya: kita netralkan satu fitur (set ke nilai rata-rata portofolio), lalu lihat seberapa besar skor anomali berubah. Jika skor turun drastis setelah kita netralkan AVG_SV, berarti masalahnya ada di volume penjualan. PM tidak perlu menebak — sistem langsung menunjuk fitur yang paling bertanggung jawab."

---

### Slide 9 — MAD Z-Score: Mengapa Bukan Z-Score Biasa?

**Visual:** Dua kurva distribusi — satu dengan mean bergeser karena outlier (Z-Score biasa), satu yang stabil di median (MAD Z-Score)

**Narasi:**
> "Untuk mengukur seberapa jauh seorang merchant dari norma portofolio, kita gunakan MAD Z-Score — bukan Z-Score standar. Perbedaannya ada di titik acuannya: Z-Score standar menggunakan *mean*, yang mudah tertarik oleh outlier. Dengan hanya 38 merchant, satu merchant ekstrem bisa menggeser mean cukup jauh sehingga semua merchant lain terlihat lebih aman dari yang sebenarnya. MAD menggunakan *median* yang tidak terpengaruh outlier — jauh lebih andal untuk portofolio kecil seperti ini."

---

### Slide 10 — Risk Score Komposit: Menyatukan Semua Sinyal

**Visual:** Formula dengan bobot divisualisasikan sebagai pie chart: Growth 40% | Volume 30% | Fee Income 20% | Target Gap 10%

**Narasi:**
> "Semua sinyal dari MAD Z-Score digabungkan menjadi satu angka: Risk Score antara 0 sampai 100. Bobotnya mencerminkan logika bisnis: penurunan tren pertumbuhan adalah sinyal churn paling kuat, karena merchant yang akan pergi biasanya terlebih dahulu menunjukkan laju penurunan sebelum volume benar-benar jatuh. Volume penjualan adalah sinyal kedua terkuat, diikuti fee income, dan terakhir gap target — karena tidak semua merchant memiliki target yang terdefinisi."

---

### Slide 11 — Klasifikasi Risiko & Ensemble Alert

**Visual:** Tabel tiga baris berwarna: Merah (HIGH ≥ 60) | Kuning (MEDIUM 30–59) | Hijau (STABLE < 30), plus badge khusus "Ensemble Alert"

**Narasi:**
> "Dari Risk Score, kita dapatkan tiga kategori risiko. Ada juga satu lapisan tambahan: Z-Score Tripwire. Jika sebuah merchant diklasifikasikan STABLE tapi salah satu dimensi Z-Score-nya sangat negatif, sistem secara otomatis mengangkatnya ke MEDIUM RISK — agar tidak ada sinyal bahaya yang terlewat hanya karena dua dimensi lain masih terlihat oke."

> "Dan yang paling kuat: Ensemble Alert. Ini diberikan kepada merchant yang sekaligus mendapat HIGH RISK dari skor komposit *dan* diflag oleh Isolation Forest. Dua metode berbeda menunjuk merchant yang sama — tingkat keyakinannya jauh lebih tinggi. Ini yang harus menjadi prioritas pertama PM."

---

### Slide 12 — Holt-Winters: Proyeksi Penjualan Per Merchant

**Visual:** Line chart dua segmen: garis historis (solid) + garis proyeksi (dashed), dengan band musiman

**Narasi:**
> "Di luar pipeline utama, ada satu modul tambahan: Holt-Winters Forecasting. Ini berjalan per merchant, on-demand, dan menggunakan riwayat penjualan bulanan. Jika merchant memiliki minimal 24 bulan data, model dapat memisahkan tren jangka panjang dari pola musiman — misalnya, merchant yang selalu naik di bulan Desember. Jika data lebih sedikit, sistem otomatis turun ke model yang lebih sederhana. Output-nya adalah proyeksi penjualan sampai akhir tahun dan verdict langsung: on track, at risk, atau critical."

---

### Slide 13 — Kenapa Dua Model Sekaligus?

**Visual:** Matriks 2×2: sumbu X = Cluster Tier (Tinggi/Rendah), sumbu Y = Anomali (Ya/Tidak)

| | Tidak Anomali | Anomali |
|---|---|---|
| **PREMIUM** | Merchant sehat — pantau rutin | Prioritas tinggi — ada yang berubah |
| **PASIF** | Performa rendah tapi konsisten | Butuh perhatian berbeda |

**Narasi:**
> "K-Means dan Isolation Forest menjawab pertanyaan yang berbeda. K-Means menjawab: 'Merchant ini termasuk kelompok mana?' Isolation Forest menjawab: 'Apakah merchant ini berperilaku tidak biasa *untuk kelompoknya*?' Kombinasi keduanya menghasilkan nuansa yang tidak bisa didapat dari satu model saja. Merchant PASIF yang konsisten tidak seurgent merchant PREMIUM yang tiba-tiba anomali — meskipun secara nilai absolut yang PASIF lebih rendah."

---

### Slide 14 — Kesimpulan & Nilai Bisnis

**Narasi:**
> "Secara keseluruhan, pipeline ini dirancang untuk menjawab satu pertanyaan bisnis yang sederhana: merchant mana yang perlu diperhatikan sekarang, dan mengapa? Setiap keputusan teknis — dari pemilihan MAD Z-Score, LOFO contribution, sampai Ensemble Alert — dibuat untuk memastikan PM mendapatkan sinyal yang actionable, bukan sekadar output model yang perlu diterjemahkan sendiri."

> "Model ini bukan prediksi yang bisa salah atau benar secara absolut. Ini adalah sistem peringatan dini yang memberi PM waktu untuk bereaksi sebelum merchant benar-benar churn."

---

## Tips Penyampaian

| Situasi | Saran |
|---|---|
| Audiens mulai bingung di Feature Engineering | Pakai analogi: "Bayangkan membandingkan IPK mahasiswa S1 4 tahun vs mahasiswa yang baru 1 semester — harus dinormalisasi dulu" |
| Pertanyaan tentang akurasi model | "Model ini tidak menggunakan label 'churn' karena tidak ada data historis churn. Yang kita ukur adalah keanehan dan penurunan performa, bukan prediksi biner" |
| Pertanyaan mengapa tidak pakai model supervised | "Kita tidak punya data training berlabel. Tidak ada catatan merchant mana yang sudah churn. K-Means dan Isolation Forest bekerja tanpa label" |
| Pertanyaan tentang threshold 10% Isolation Forest | "10% dari 38 merchant = sekitar 3–4 merchant. Cukup kecil untuk bisa diinvestigasi PM, tidak terlalu besar sampai jadi noise" |
| Pertanyaan apakah model bisa salah | "Ya, bisa. Itulah kenapa PM tetap punya kontrol — slider sensitivity dan kemampuan melihat kontribusi per fitur. Model memberi sinyal, PM yang memutuskan" |
