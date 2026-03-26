-- =============================================================================
-- EXTRACT 01 — MASTER MERCHANT (dim_merchant)
-- Sumber Excel : ALL_MID_202601_MASTER.xlsx → Sheet1
-- Sumber DB    : tabel dim_merchant
-- Output       : raw_01_master_merchant.csv
--
-- Cara jalankan di terminal:
--   sqlite3 -header -csv database/btn_anchor.db < sql/extract_01_master_merchant.sql > output/raw_01_master_merchant.csv
--
-- Keterangan kolom:
--   merchant_id      = ID unik per terminal/mesin (EDC/QRIS)
--   segmen           = RETAIL atau ANCHOR
--   merchant_group   = nama grup merchant (key join utama)
--   merchant_brand   = nama brand spesifik dalam grup
--   equip            = jenis perangkat: EDC atau QRIS
--   merchant_name    = nama merchant individual
--   + kolom enrichment (category, city, tier, dll)
-- =============================================================================

-- Tarik SEMUA baris raw dari tabel master merchant
-- Tidak ada agregasi, tidak ada GROUP BY
-- Filter: hanya merchant ANCHOR (bukan RETAIL umum)

SELECT
    merchant_id,
    segmen,
    merchant_group,
    merchant_brand,
    equip,
    merchant_name,
    merchant_category,
    merchant_city,
    merchant_province,
    merchant_tier,
    onboarding_year,
    years_partnership,
    channel_type
FROM dim_merchant
WHERE segmen = 'ANCHOR'
ORDER BY merchant_group, merchant_brand, merchant_id;
