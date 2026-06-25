# 🎬 Script Video Demo — BTN Anchor Merchant Decision Intelligence Dashboard

**Konteks:** Materi Sidang Magang (UMN — Semester 6) · **Audiens:** Dosen Penguji
**Target durasi:** 9–11 menit (versi mendalam — algoritma inti K-Means & Isolation Forest dibahas penuh) · **Format:** scene-by-scene (cue layar + narasi + timing)
**Catatan bahasa:** narasi Bahasa Indonesia, istilah teknis dibiarkan dalam Bahasa Inggris.

> **Cara pakai dokumen ini.** Tiap scene punya tiga bagian: **🖥️ Tampilan Layar** (apa yang harus terlihat saat rekaman), **🎙️ Narasi** (teks yang dibacakan — ±150 kata/menit), dan **💡 Catatan** (tips teknis / transisi). Dua algoritma yang ada di judul — **K-Means** (Scene 5) dan **Isolation Forest** (Scene 6) — sengaja dibahas paling dalam karena itu yang akan paling dikejar penguji. Angka pada Scene 5 dihitung ulang langsung dari data portofolio nyata (extract 25 Mei 2026). Rumus lengkap kedua algoritma ada di **Lampiran D**.

---

## 🗺️ Peta Scene & Alokasi Waktu

| Scene | Segmen | Durasi | Kumulatif |
|------|--------|--------|-----------|
| 0 | Pembuka & identitas | 0:40 | 0:40 |
| 1 | Masalah bisnis & tujuan | 0:50 | 1:30 |
| 2 | Arsitektur sistem (end-to-end) | 1:15 | 2:45 |
| 3 | Penjelasan teknis Front End | 1:45 | 4:30 |
| 4 | ML Engine — gambaran 3 lapis | 0:45 | 5:15 |
| 5 | **K-Means: cara kerja + kenapa K=5** ⭐ | 2:40 | 7:55 |
| 6 | **Isolation Forest + LOFO** ⭐ | 1:45 | 9:40 |
| 7 | Health Alerts (Composite Risk) | 0:35 | 10:15 |
| 8 | Kontribusi & penutup | 0:40 | 10:55 |

*Buffer ±1 menit untuk jeda transisi. **Mau versi ~8 menit?** Pangkas Scene 1 & 7 jadi 25 detik, dan ringkas Scene 3. Jangan potong Scene 5 dan 6 — itu inti judul.*

---

## 🎬 SCENE 0 — Pembuka (0:00 – 0:40)

**🖥️ Tampilan Layar:**
Halaman **Dashboard** terbuka penuh dalam mode **Navy & Gold** (dark). Logo BTN terlihat di sidebar. Diam 2 detik sebelum mulai bicara agar penonton menyerap tampilan.

**🎙️ Narasi:**
> "Selamat siang, Bapak/Ibu penguji. Pada video ini saya akan mendemokan hasil kerja magang saya: sebuah *Decision Intelligence Dashboard* untuk portofolio **Anchor Merchant** Bank BTN. Sistem ini mengubah proses yang tadinya manual berbasis Excel menjadi satu *pipeline* otomatis — mulai dari *ingestion* data mentah, klasifikasi *machine learning* berlapis, sampai *monitoring* mingguan dan deteksi *churn*. Dua metode inti yang menjadi judul penelitian ini, yaitu **K-Means Clustering** dan **Isolation Forest**, akan saya bahas paling dalam. Saya mulai dari masalahnya, lalu arsitektur, sisi teknis *front end*, dan ditutup dengan metodologi *machine learning*-nya."

**💡 Catatan:** Bicara tenang. Jangan klik apa pun dulu — biarkan layar statis sebagai "establishing shot".

---

## 🎬 SCENE 1 — Masalah Bisnis & Tujuan (0:40 – 1:30)

**🖥️ Tampilan Layar:**
Klik tab **Overview**. Sorot **KPI cards** di atas (total portfolio, jumlah merchant, coverage PM) dan tabel **PM Assignment** di bawah.

**🎙️ Narasi:**
> "Bank BTN mengelola puluhan *Anchor Merchant* besar yang ditangani sejumlah *Portfolio Manager*. Sebelum sistem ini, datanya tersebar di banyak file Excel — *Master MID*, *Card Share*, dan laporan *Monitoring* mingguan. Untuk tahu merchant mana yang tumbuh, mana yang mulai turun, dan siapa PM-nya, analis harus menggabungkan file-file itu manual setiap minggu — lambat dan rawan *human error*.
>
> Tujuan proyek ada tiga: **pertama**, mengotomasi *ETL* dari file mentah; **kedua**, memberi *layer* *machine learning* supaya merchant otomatis tersegmentasi dan risiko *churn*-nya terdeteksi dini; **ketiga**, menyajikan semuanya di satu dashboard yang langsung dipakai tim bisnis."

**💡 Catatan:** Saat menyebut tiga tujuan, gerakkan kursor ke KPI cards untuk penekanan visual.

---

## 🎬 SCENE 2 — Arsitektur Sistem End-to-End (1:30 – 2:45)

**🖥️ Tampilan Layar:**
Tampilkan **diagram arsitektur** (`README.md` bagian *Architecture*, atau slide terpisah): `Excel → ETL → SQLite → Ingestion → Neon PostgreSQL → Streamlit App`. Lalu klik halaman **Automated Pipeline**.

**🎙️ Narasi:**
> "Secara arsitektur, ada empat lapisan. Di **hulu**, tiga *master file* Excel — MID, Card Share, Monitoring. Lapisan **ETL** — berjalan di Windows lewat *Excel-COM automation* (`win32com`) — membersihkan dan mengklasifikasi data, lalu mengeluarkan *extract* **SQLite**. Saya pilih *Excel-COM* secara sengaja, supaya formula, *pivot*, dan format di file korporat tidak rusak.
>
> *Extract* itu di-*ingest* ke **Neon PostgreSQL**, *database cloud* yang jadi satu-satunya sumber data aplikasi. *Ingestion*-nya **incremental dan idempotent** — hanya baris baru yang masuk, jadi *re-run* tidak menduplikasi. Sebelum data masuk ada **governance gate**: kalau ada *Anchor* atau PM baru yang belum disetujui, *pipeline* otomatis diblokir. Di **hilir**, aplikasi **Streamlit** membaca Neon dan menyajikan lima halaman fungsional."

**💡 Catatan:** Pakai slide diagram kalau ada yang lebih rapi dari README. Tunjukkan UI Automated Pipeline sekilas saja.

---

## 🎬 SCENE 3 — Penjelasan Teknis Front End (2:45 – 4:30)

**🖥️ Tampilan Layar:**
Dari **sidebar**, berurutan: (1) toggle **tema** dark↔light, (2) sorot **navigation** 5 halaman + **status strip** Neon, (3) perkecil window untuk **bottom nav** mobile (jika bisa), (4) klik antar tab agar terlihat 6 tab.

**🎙️ Narasi:**
> "Sisi *front end*-nya. Seluruh antarmuka dibangun dengan **Streamlit** dan Python — tanpa *framework* JavaScript terpisah. Navigasinya memakai **`st.navigation`** *multipage* dengan lima halaman: Dashboard, Automated Pipeline, Data Editor, PM Manager, dan Global Settings.
>
> Tampilannya bukan tema default. Saya bangun **design system** sendiri di `utils/theme.py` — *palette* **Navy & Gold** sesuai *brand* BTN — yang disuntikkan sebagai *custom CSS*, dan bersifat **dual-mode**: *dark* dan *high-contrast light*, di-*toggle* dari sidebar. Seluruh komponen mengikuti *palette* yang sama otomatis. *(Lakukan toggle.)* Navigasinya juga *responsive*: di mobile di bawah 768 piksel muncul **bottom navigation bar** yang di-*fix* lewat CSS.
>
> Satu detail *engineering* penting: dashboard punya **read-only snapshot tier**. Pemuatan data di-*cache* dengan `st.cache_data` dan hanya menarik ulang dari Neon saat *pipeline* menulis data baru — *keyed* pada `LAST_DATA_UPDATE`, bukan timer. Kalau Neon sempat tak terjangkau, aplikasi menyajikan *snapshot* lokal terakhir agar dashboard tetap hidup. Grafiknya dirender dengan **Plotly** interaktif — *hover*, *zoom*, *drill-down*."

**💡 Catatan:** Scene andalan kompetensi teknis. Gerakan toggle tema harus jelas — lakukan pelan. Kalau resize mobile tak praktis, cukup sebutkan.

---

## 🎬 SCENE 4 — ML Engine: Tiga Lapis Analitik (4:30 – 5:15)

**🖥️ Tampilan Layar:**
Klik tab **Merchant Tiers**. Tampilkan **cluster scatter (PCA 2-D)** dan kartu ringkasan tier.

**🎙️ Narasi:**
> "Mesin analitiknya ada di `utils/ml_engine.py` — modul Python murni, terpisah dari UI, dan sudah *unit-tested*. Ada tiga lapis yang saling melengkapi. **Lapis pertama: segmentasi** dengan **K-Means** — mengelompokkan merchant ke tier performa. **Lapis kedua: skor risiko** — *Composite Risk Score* 0 sampai 100. **Lapis ketiga: deteksi anomali** dengan **Isolation Forest** — menangkap pola menyimpang yang tidak terlihat oleh dua lapis lain. Saya akan bahas K-Means dan Isolation Forest satu per satu, karena keduanya inti dari penelitian ini."

**💡 Catatan:** Scene jembatan — singkat. Nada "sekarang masuk ke intinya".

---

## 🎬 SCENE 5 — K-Means Clustering: Cara Kerja + Kenapa K = 5 (5:15 – 7:55) ⭐⭐

### Bagian A — Apa & kenapa K-Means (5:15 – 5:45)

**🖥️ Tampilan Layar:** Tetap di **Merchant Tiers**, tunjukkan scatter PCA dengan 5 warna tier.

**🎙️ Narasi:**
> "**K-Means** adalah algoritma *unsupervised* — ia mengelompokkan merchant tanpa label awal, murni dari kemiripan profil performa. Saya pakai ini untuk *automated tiering*: alih-alih analis menentukan 'siapa premium, siapa pasif' secara subjektif, algoritma yang menemukan kelompok alaminya dari data. Klasterisasi memakai **enam fitur**: rata-rata *Sales Volume*, *Fee-Based Income*, rasio kartu *on-us*, *growth*, persentase pencapaian target, dan jumlah minggu aktif."

### Bagian B — Mekanisme algoritma (5:45 – 6:30)

**🖥️ Tampilan Layar:** Tampilkan **slide overlay** berisi 4 langkah + rumus jarak Euclidean (lihat Lampiran D). Boleh animasi sederhana titik→centroid.

**🎙️ Narasi:**
> "Cara kerjanya iteratif, empat langkah. **Satu — inisialisasi.** Sistem menaruh lima titik pusat awal, *centroid*. Saya pakai **K-Means++**, bukan acak murni — *centroid* awal sengaja disebar berjauhan supaya hasilnya stabil dan tidak terjebak solusi buruk. Saya juga set `n_init` sama dengan 20, artinya proses diulang 20 kali dari titik awal berbeda lalu diambil yang terbaik, dengan `random_state` 42 agar hasilnya *reproducible*.
>
> **Dua — *assignment*.** Setiap merchant dihitung **jarak Euclidean**-nya ke tiap *centroid*, lalu masuk ke *centroid* terdekat. **Tiga — *update*.** *Centroid* dihitung ulang sebagai rata-rata anggota klasternya. **Empat**, langkah dua dan tiga diulang sampai *centroid* tidak bergerak lagi — itu tanda konvergen.
>
> Tujuan matematis yang diminimalkan adalah **WCSS** atau *inertia* — total jarak kuadrat tiap merchant ke pusat klasternya. Makin kecil WCSS, makin solid kelompoknya. Satu hal penting sebelum klasterisasi: dua fitur uang saya *log-transform* karena nilainya miliaran rupiah, dan semua fitur di-*standardize* dengan `StandardScaler`. Tanpa itu, variabel Rupiah akan mendominasi variabel persen — *scaling* membuat keenam fitur dipandang adil."

### Bagian C — Kenapa tepat K = 5 (6:30 – 7:40)

**🖥️ Tampilan Layar:**
Buka *expander* **"Cluster Diagnostics — Methodology"** → grafik **"Choosing K — Elbow Method & Silhouette Sweep"** (garis merah di K=5), lalu kartu **Cluster Cohesion**. Siapkan **slide tabel** di bawah sebagai overlay.

**🎙️ Narasi (prinsip + mekanisme):**
> "Pertanyaan kuncinya: kenapa lima klaster? Hal pertama yang ingin saya tegaskan — **angka K tidak saya *hardcode*.** K **dipilih otomatis dari data setiap kali pipeline berjalan**, lewat fungsi `select_optimal_k`. Caranya, sistem menyapu kandidat K dari **2 sampai 5**, dan untuk tiap K menghitung tiga metrik: **Inertia** untuk *Elbow Method*, **Silhouette Score**, dan **Davies-Bouldin Index**. K operasional dipilih pada **Silhouette tertinggi**, di-*clamp* di rentang dua sampai lima untuk mencegah *over-segmentation* — prinsip *parsimony*."

**📊 Tabel untuk ditampilkan di layar (slide overlay):**

| K | Inertia (WCSS) ↓ | **Silhouette** ↑ | Davies-Bouldin ↓ | Catatan |
|:--:|:--:|:--:|:--:|:--|
| 2 | 152.50 | 0.443 | 0.919 | Terlalu kasar (cuma 2 tier) |
| 3 | 112.53 | 0.372 | 1.008 | Titik *elbow*, tapi Silhouette **terendah** |
| 4 | 83.55 | 0.424 | 0.916 | Membaik, belum optimal |
| **5** | **63.74** | **0.458** ⭐ | **0.745** ⭐ | **Silhouette tertinggi & DBI terendah** |

**🎙️ Narasi (angka + kesimpulan):**
> "Ini hasil nyata pada portofolio kami, 76 merchant. Perhatikan baris K sama dengan lima: di situ **Silhouette tertinggi, 0,458**, sekaligus **Davies-Bouldin terendah, 0,745**. Dua metrik ini berlawanan arah — Silhouette makin tinggi makin bagus, Davies-Bouldin makin rendah makin bagus — dan **keduanya sepakat menunjuk K sama dengan lima**. Davies-Bouldin di bawah 0,8 masuk kategori *Strong*: kelima tier benar-benar terpisah, bukan potongan sembarang.
>
> Menariknya, *Elbow* menunjuk K sama dengan tiga, tapi di K tiga justru Silhouette-nya paling rendah. Inilah kenapa Silhouette saya jadikan penentu utama — ia menangkap struktur yang tidak terlihat *Elbow*, dan lebih objektif untuk kriteria otomatis. Maka sistem menetapkan **K sama dengan lima**, dan kelima klaster diberi label by *rank composite score* — dari terbaik: **ELITE, PREMIUM, REGULER, PASIF, DORMANT**. Karena penamaan mengikuti peringkat skor, bukan nomor klaster mentah, **labelnya tetap stabil** walau data berubah.
>
> Singkatnya: **K sama dengan lima bukan asumsi, melainkan keputusan yang dimenangkan oleh data.**"

**💡 Catatan:** Klimaks teknis. Sediakan **slide tabel** karena angka grafik live kecil. Tekankan **"tertinggi"** dan **"terendah"**. Antisipasi pertanyaan K=2 ada di Lampiran A.

---

## 🎬 SCENE 6 — Isolation Forest + Explainable AI (LOFO) (7:55 – 9:40) ⭐⭐

### Bagian A — Konsep & kenapa Isolation Forest (7:55 – 8:25)

**🖥️ Tampilan Layar:**
Klik tab **Anomaly Detection**. Tampilkan grafik sinyal anomali. Siapkan **slide ilustrasi** "memotong area padat vs sepi".

**🎙️ Narasi:**
> "Lapis ketiga: **Isolation Forest**, dari Liu dan rekan tahun 2008. Kalau K-Means mengelompokkan data normal, Isolation Forest justru bertugas **memburu yang menyimpang** — merchant dengan pola transaksi tidak wajar secara *multivariate*, artinya melihat kombinasi banyak fitur sekaligus, bukan satu per satu.
>
> Saya memilih metode ini karena tiga alasan: ia **tidak mengasumsikan distribusi normal**, sangat **efisien** walau fiturnya banyak, dan **andal pada portofolio kecil** seperti milik kami. Penting juga: ia memakai **ruang fitur yang sama persis** dengan K-Means — sudah di-*log-transform* dan di-*standardize* — supaya metodologinya konsisten."

### Bagian B — Cara kerja & skor anomali (8:25 – 9:05)

**🖥️ Tampilan Layar:** **Slide overlay** intuisi pohon isolasi + rumus skor anomali (Lampiran D).

**🎙️ Narasi:**
> "Intuisinya begini. Sistem membangun **100 pohon acak** — `n_estimators` sama dengan 100 — dengan cara berulang kali **memotong fitur di nilai acak**. Bayangkan memisahkan satu titik dari kerumunan: kalau titik itu berada di area **padat** — yaitu merchant normal — butuh **banyak potongan** untuk mengisolasinya. Tapi kalau titik itu **menyimpang jauh**, ia berada di area sepi dan **cukup sedikit potongan** sudah terisolasi.
>
> Jumlah potongan itu disebut *path length*. Jadi logikanya: **makin pendek *path length*, makin anomali**. Nilai ini diringkas jadi **skor anomali** dengan rumus dua pangkat negatif rata-rata *path length* dibagi normalisasinya — hasilnya antara 0 dan 1, dan **makin mendekati 1 makin aneh**. Saya set `contamination` sama dengan 0,10, artinya sistem mengasumsikan sekitar 10 persen portofolio layak ditandai — kira-kira tiga sampai empat merchant — sebagai kandidat anomali teratas."

### Bagian C — LOFO: Explainable AI (9:05 – 9:40)

**🖥️ Tampilan Layar:** Sorot baris merchant anomali dan **bar kontribusi LOFO** per fitur.

**🎙️ Narasi:**
> "Tapi mendeteksi anomali saja tidak cukup untuk bank — pengguna harus tahu **kenapa**. Di sinilah fitur *Explainable AI* yang saya bangun: **LOFO, Leave One Feature Out.** Logikanya, untuk tiap fitur, sistem **menetralkannya sementara** — menyetelnya ke nilai rata-rata — lalu menghitung ulang skor anomalinya. Kalau skor keanehan **turun drastis** saat fitur *Volume* dimatikan, berarti **penurunan Volume itulah penyebab utama** anomali tersebut.
>
> Yang efisien, LOFO tidak perlu melatih ulang model — cukup *re-score*. Jadi tiap *alert* datang dengan diagnosis: bukan sekadar 'merchant ini anomali', tapi 'anomali **karena** fitur X'. Sebagai penutup lapis ini, ada *safety override*: kalau **Modified Z-Score berbasis MAD** — statistik yang tahan *outlier* — mendeteksi penurunan ekstrem, status STABLE otomatis dinaikkan ke MEDIUM RISK. Jadi K-Means, Isolation Forest, dan Z-Score bekerja bersama menghasilkan satu penilaian akhir yang akurat."

**💡 Catatan:** Scene inti kedua. Pakai ilustrasi "memotong kerumunan" — penguji suka analogi visual. Jangan terburu di bagian LOFO; itu nilai jual *Explainable AI*-nya.

---

## 🎬 SCENE 7 — Health Alerts & Composite Risk Score (9:40 – 10:15)

**🖥️ Tampilan Layar:**
Klik tab **Health Alerts** (perhatikan badge angka di label tab) → sorot *risk register*, HIGH RISK di atas.

**🎙️ Narasi:**
> "Hasil ketiga lapis tadi bermuara di tab **Health Alerts** — *register* risiko *churn*. Skornya, **Composite Risk Score** 0 sampai 100, dihitung dengan bobot: *Growth* 40 persen, *Volume* 30 persen, *FBI* 20 persen, dan pencapaian target 10 persen — lalu dibagi tiga tingkat: HIGH di atas 60, MEDIUM 30 sampai 59, dan STABLE. Badge angka di tab ini memberi tahu pengguna berapa merchant yang butuh perhatian hari itu, lengkap dengan alasan *multi-factor* kenapa tiap merchant ditandai."

**💡 Catatan:** Singkat — ini sintesis, bukan algoritma baru. Kalau perlu versi pendek, scene ini bisa jadi 20 detik.

---

## 🎬 SCENE 8 — Kontribusi & Penutup (10:15 – 10:55)

**🖥️ Tampilan Layar:**
Kembali ke tab **Overview** (tampilan utuh), atau slide ringkasan kontribusi.

**🎙️ Narasi:**
> "Sebagai penutup — kontribusi proyek ini ada tiga. **Pertama**, otomasi *end-to-end* yang menghapus proses Excel manual mingguan. **Kedua**, *machine learning* yang **dapat dipertanggungjawabkan**: jumlah klaster K-Means dipilih dari data, dan setiap anomali Isolation Forest bisa dijelaskan lewat LOFO. **Ketiga**, *dashboard* yang langsung dipakai tim bisnis BTN dengan *standar engineering* layak produksi — *cloud database*, *caching*, *governance gate*, dan *fallback* offline.
>
> Sekian demo dari saya. Terima kasih, Bapak/Ibu penguji, saya siap untuk sesi tanya jawab."

**💡 Catatan:** Tahan layar 2 detik setelah kalimat terakhir sebelum stop. Senyum di akhir kalau on-camera.

---

## 📎 LAMPIRAN A — Antisipasi Pertanyaan Penguji

*Tidak dibacakan — bekal sesi tanya jawab.*

**T: "Silhouette K=2 (0,443) hampir sama dengan K=5 (0,458). Kenapa bukan K=2 yang lebih sederhana?"**
J: (1) Silhouette tetap **maksimum di K=5**, dan Davies-Bouldin K=5 (0,745) **jauh lebih baik** dari K=2 (0,919) — bukan seri. (2) Secara bisnis, K=2 hanya menghasilkan dua tier yang terlalu kasar; tim butuh membedakan ELITE dari PREMIUM, dan PASIF dari DORMANT. (3) *Clamp* [2,5] memang dirancang agar pemilihan tetap pada Silhouette-optimal.

**T: "Kenapa Silhouette jadi penentu, bukan Elbow?"**
J: *Elbow* membaca titik belok kurva Inertia secara visual dan sering ambigu (di sini menunjuk K=3). Silhouette mengukur kualitas pemisahan klaster secara kuantitatif per titik data — lebih cocok jadi kriteria **otomatis**. Elbow tetap dilaporkan sebagai *supporting evidence*.

**T: "Kenapa Isolation Forest, bukan DBSCAN atau Local Outlier Factor?"**
J: Isolation Forest **tidak butuh asumsi distribusi**, kompleksitasnya **linear** (efisien), stabil pada **portofolio kecil**, dan *anomaly score*-nya langsung bisa di-*ranking*. DBSCAN sensitif terhadap parameter `eps` dan kepadatan; LOF lebih mahal secara komputasi. IF juga mudah dipadukan dengan LOFO untuk *explainability*.

**T: "Apa arti `contamination = 0.10`? Apakah itu bias?"**
J: Itu *prior* proporsi anomali yang diharapkan — di sini ~10% portofolio sebagai kandidat teratas. Ini memang asumsi yang bisa di-*tune*; saya pilih konservatif agar *alert* tidak membanjiri pengguna. Penting: IF tetap memberi **skor kontinu**, jadi *threshold* bisa digeser tanpa melatih ulang.

**T: "K bisa berubah? Bagaimana stabilitas label?"**
J: Ya — `select_optimal_k` jalan tiap data baru masuk, jadi K dinamis di [2,5]. Agar label tak "loncat", penamaan tier di-*assign by rank* dari *composite score* (Volume 60%, Achievement 25%, Growth 15%), bukan nomor klaster mentah.

**T (kritis): "Apakah keenam fitur benar-benar dipakai?"**
J — *jawab jujur*: Pada *feed* `PROCESSED_MONITORING` saat ini, kolom **SV_GROWTH_RATE** dan **WEEKS_ACTIVE** belum terisi sehingga masuk konstan; `StandardScaler` menetralkannya (varians nol → kontribusi nol). Jadi *clustering* efektif berjalan pada **4 fitur bervariasi**: AVG_SV, AVG_FBI, RASIO_ONUS, ACHIEVEMENT_PCT. Ini **limitasi data, bukan bug model** — begitu *feed* growth/weeks-active dilengkapi, keenam fitur aktif tanpa ubah kode.

---

## 📎 LAMPIRAN B — Checklist Sebelum Rekam

- [ ] `DATABASE_URL` ter-*set*; Neon **Connected** (titik biru di sidebar).
- [ ] Data ter-*load* (bukan banner *snapshot* read-only), kecuali sengaja mendemokan *fallback*.
- [ ] Mulai **dark mode**; latih sekali gerakan *toggle* tema.
- [ ] Buka *expander* **Cluster Diagnostics** sebelum mulai agar grafik K sudah ter-*render*.
- [ ] Buka tab **Anomaly Detection** dulu agar bar LOFO sudah ter-*render* (hindari *loading* saat bicara).
- [ ] Siapkan **slide overlay**: (a) 4 langkah K-Means + rumus Euclidean, (b) tabel K (Scene 5), (c) ilustrasi pohon isolasi + rumus skor anomali (Scene 6).
- [ ] Tutup aplikasi lain; sembunyikan data sensitif; *zoom* browser ±110%.
- [ ] Tes mikrofon; rekam 10 detik percobaan.

---

## 📎 LAMPIRAN C — Angka Kunci (Cheat Sheet)

| Item | Nilai |
|------|-------|
| Jumlah merchant diklaster (n) | **76** |
| Rentang sweep K | **2 – 5** |
| K terpilih (operasional) | **5** |
| Silhouette @ K=5 | **0,458** (tertinggi) |
| Davies-Bouldin @ K=5 | **0,745** (terendah, *Strong*) |
| K menurut Elbow | 3 (pembanding) |
| Label tier (best → worst) | ELITE · PREMIUM · REGULER · PASIF · DORMANT |
| K-Means init / restart / seed | k-means++ · `n_init`=20 · `random_state`=42 |
| Isolation Forest | `n_estimators`=100 · `contamination`=0.10 · seed 42 |
| Bobot composite (label rank) | Volume 60% · Achievement 25% · Growth 15% |
| Bobot Risk Score | Growth 40% · Volume 30% · FBI 20% · Achievement 10% |
| Tech stack inti | Python · Streamlit · scikit-learn · Plotly · Neon PostgreSQL · SQLAlchemy |

---

## 📎 LAMPIRAN D — Formula & Istilah Kunci (untuk Slide Overlay)

### K-Means Clustering

**1. Jarak Euclidean** — mengukur kemiripan merchant *x* ke *centroid* *c*:

> **d(x, c) = √ Σᵢ (xᵢ − cᵢ)²**
> *Makin kecil jarak, makin mirip → masuk klaster yang sama.*

**2. Objective / WCSS (Within-Cluster Sum of Squares)** — yang diminimalkan algoritma:

> **WCSS = Σⱼ Σ(x∈Cⱼ) ‖x − cⱼ‖²**
> *Total jarak kuadrat tiap titik ke pusat klasternya. Makin kecil = klaster makin solid. Inilah "Inertia" pada kurva Elbow.*

**3. Silhouette Score** (untuk satu titik, dirata-rata seluruh data) — penentu pemilihan K:

> **s = (b − a) / max(a, b)**, rentang −1 … 1
> *a = rata-rata jarak ke anggota klaster sendiri; b = rata-rata jarak ke klaster tetangga terdekat. Makin tinggi makin baik.*

**4. Davies-Bouldin Index** — rasio sebaran dalam-klaster terhadap jarak antar-klaster; **makin rendah makin baik** (<0,8 = *Strong*).

**5. Composite Score (penamaan tier, by rank):**

> **Score = 0,60·Volume + 0,25·Achievement + 0,15·Growth**
> *Klaster dengan skor tertinggi → ELITE, terendah → DORMANT.*

### Isolation Forest

**Skor anomali** untuk titik *x* pada sampel berukuran *n*:

> **s(x, n) = 2^( − E[h(x)] / c(n) )**, rentang 0 … 1
> *E[h(x)] = rata-rata path length (jumlah potongan untuk mengisolasi x) di semua pohon; c(n) = faktor normalisasi. Path pendek → s mendekati 1 → sangat anomali.*

**LOFO (Leave One Feature Out) — kontribusi penyebab:**

> **Kontribusiᵢ = Skor_anomali_penuh − Skor_anomali(fitur ke-i dinetralkan)**
> *Delta besar & positif = fitur i adalah penyebab utama anomali tersebut. Tanpa melatih ulang model — hanya re-score.*

**Modified Z-Score (MAD) — safety override, tahan outlier:**

> **z = 0,6745 · (x − median) / MAD**, dengan **MAD = median(|x − median|)**
> *Jika z menembus ambang (−1,2), STABLE dinaikkan ke MEDIUM RISK.*

> *Angka K-sweep dihitung ulang dari `PROCESSED_CARD_SHARE` + `PROCESSED_MONITORING` + `TARGET` (extract 25 Mei 2026) memakai logika identik dengan `utils/ml_engine.select_optimal_k`. Dashboard live menghitung ulang otomatis bila portofolio diperbarui — nilai bisa sedikit bergeser, tapi mekanismenya tetap sama.*
