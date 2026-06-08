# Penjelasan Machine Learning Pipeline (Anchor Automation Dashboard)

File ini berisi rangkuman lengkap hasil diskusi mengenai arsitektur Machine Learning pada sistem *Anchor Automation Dashboard*, yang dirancang khusus dengan bahasa yang mudah dipahami (namun tetap akademis) untuk keperluan presentasi PPT maupun skripsi.

## 1. Feature Engineering (Tahap Persiapan)
**Konsep:** "Menyiapkan dan membersihkan bahan mentah sebelum dimasukkan ke dalam Oven (Machine Learning)."

Proses ini terbagi menjadi 2 tahap:
*   **Tahap Bisnis (Domain-Driven):** Mengolah data agar masuk akal secara aturan perbankan (contoh: menghitung rerata transaksi berdasarkan durasi aktif, melimitasi *growth* yang tidak masuk akal, dan *log transform* agar angka miliaran Rupiah tidak merusak perhitungan).
*   **Tahap Statistik (Algorithm-Driven):** Menggunakan `StandardScaler` di akhir proses. Tujuannya agar AI memandang semua kolom data secara adil (menyamakan skala antara variabel yang menggunakan Rupiah, Persen, dan Jumlah Minggu).

---

## 2. K-Means Clustering (Merchant Segmentation)
**Konsep:** "Mengelompokkan merchant (Automated Tiering) ke dalam kasta/segmen strategis berdasarkan kemiripan profil performa."

**Rumus Perhitungan:**
1.  **Metrik Jarak (Euclidean Distance):** 
    > *d(x, c) = √ Σ (xi - ci)²*
    > *Cara Membaca:* AI mengukur tingkat kemiripan antar merchant. Semakin kecil jarak selisih antar fiturnya, berarti mereka berada di kelompok yang sama.
2.  **Fungsi Optimalisasi (WCSS):**
    > *WCSS = Σ Σ ||xi - cj||²*
    > *Cara Membaca:* AI memastikan pembagian kelompok adalah yang paling solid dengan cara meminimalkan total jarak setiap merchant ke pusat kelompoknya.
3.  **Skor Gabungan (Weighted Composite Score):**
    > *Score = (0.60 * Volume) + (0.25 * Target) + (0.15 * Growth)*
    > *Cara Membaca:* Ini adalah aturan bisnis untuk merangking setiap kelompok. Cluster dengan performa gabungan terbaik akan mendapatkan label **ELITE**, dan yang terendah akan menjadi **DORMANT**.

---

## 3. Isolation Forest (Anomaly Detection / Early Warning System)
**Konsep:** "Mendeteksi merchant yang memiliki pola transaksi aneh, tidak wajar, atau menyimpang secara simultan (multivariate)."

Berbeda dengan K-Means yang mengelompokkan data normal, algoritma ini justru bekerja dengan cara memotong/mempartisi kriteria secara acak untuk **mengisolasi data**. Merchant normal berada di area padat (butuh banyak potongan untuk diisolasi), sedangkan merchant aneh berada di area sepi (sangat cepat terisolasi).

**Rumus Perhitungan Skor Anomali:**
> *Skor Anomali = 2 ^ -( Tingkat kemiripan merchant / Standar kemiripan kelompok )*

*Cara Membaca (Logika Bisnis):* 
"Semakin berbeda atau menyimpang pola data seorang merchant dibandingkan mayoritas merchant lainnya, maka ia akan semakin cepat terisolasi. Semakin cepat terisolasi, maka **Skor Anomalinya akan semakin membesar mendekati angka 1**."

**Fitur Explainable AI (LOFO - Leave One Feature Out):**
*   Sistem dapat mendiagnosa *alasan* sebuah anomali. 
*   **Logika:** Kontribusi Penyebab = (Skor Keanehan Total) dikurangi (Skor Keanehan jika sebuah fitur ditutupi sementara).
*   Jika skor keanehan turun drastis saat variabel `Volume` dihilangkan, sistem menyimpulkan bahwa penurunan volume adalah penyebab utama anomali tersebut.

---

## 4. Modified Z-Score / MAD (Statistical Risk Filter)
**Konsep:** "Pengaman (Safety Override) secara statistik."

**Penjelasan:**
*   **Kenapa digunakan?** Sebagai filter statistik yang kebal terhadap pencilan data (Robust Statistics). Jika algoritma ML (Isolation Forest) mendeteksi pola aneh yang halus, Z-Score bertugas berteriak jika ada nilai yang benar-benar anjlok drastis (di luar batas standar deviasi).
*   Ketiga mesin ini (K-Means, Isolation Forest, Z-Score) kemudian digabung untuk melahirkan satu nilai akhir yang akurat, yaitu **Composite Risk Score (0-100)**.
