-- =============================================================================
-- EXTRACT 02 — TRANSAKSI BULANAN (fact_card_share)
-- Sumber Excel : CARD_SHARE_MERCHANT_ANCHOR_2026.xlsx → Sheet Realisasi
-- Sumber DB    : tabel fact_card_share
-- Output       : raw_02_card_share.csv
--
-- Cara jalankan di terminal:
--   sqlite3 -header -csv database/btn_anchor.db < sql/extract_02_card_share.sql > output/raw_02_card_share.csv
--
-- Keterangan kolom:
--   merchant_group   = nama grup merchant
--   merchant_anchor  = nama brand/anchor spesifik
--   trx_month        = periode bulan format YYYYMM (misal: 202601 = Jan 2026)
--   year             = tahun transaksi
--   trx_debit_onus   = jumlah transaksi debit kartu BTN
--   trx_debit_offus  = jumlah transaksi debit kartu bank lain
--   trx_credit_offus = jumlah transaksi kartu kredit
--   trx_qris_onus    = jumlah transaksi QRIS on-us
--   trx_qris_offus   = jumlah transaksi QRIS off-us
--   sv_*             = Sales Volume (Rp) per channel
--   fbi_*            = Fee Based Income (Rp) per channel
-- =============================================================================

-- Tarik SEMUA baris raw dari tabel transaksi bulanan
-- Data mencakup 3 tahun: 2024, 2025, 2026
-- Tidak ada agregasi — 1 baris = 1 merchant_anchor x 1 bulan

SELECT
    merchant_group,
    merchant_anchor,
    trx_month,
    year,
    -- Jumlah transaksi per channel (raw)
    trx_debit_onus,
    trx_debit_offus,
    trx_credit_offus,
    trx_qris_onus,
    trx_qris_offus,
    -- Sales Volume per channel (Rp) — raw
    sv_debit_onus,
    sv_debit_offus,
    sv_credit_offus,
    sv_qris_onus,
    sv_qris_offus,
    -- Fee Based Income per channel (Rp) — raw
    fbi_debit_onus,
    fbi_debit_offus,
    fbi_credit_offus,
    fbi_qris_onus,
    fbi_qris_offus
FROM fact_card_share
ORDER BY merchant_group, merchant_anchor, year, trx_month;
