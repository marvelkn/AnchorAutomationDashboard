-- =============================================================================
-- EXTRACT 03 — MONITORING MINGGUAN & TARGET (fact_monitoring_weekly + dim_target)
-- Sumber Excel : Monitoring_Weekly_Anchor_2026.xlsx → Sheet 2026 & Edit Target
-- Sumber DB    : tabel fact_monitoring_weekly + dim_target
-- Output       : raw_03_monitoring.csv
--
-- Cara jalankan di terminal:
--   sqlite3 -header -csv database/btn_anchor.db < sql/extract_03_monitoring.sql > output/raw_03_monitoring.csv
--
-- Keterangan kolom:
--   merchant_group   = nama grup merchant
--   pm               = nama Account Manager / Product Manager
--   year             = tahun monitoring
--   week_number      = nomor minggu (1-53)
--   dimensi          = jenis metrik: TRX (transaksi), VOL (volume/SV), FBI (fee)
--   value            = nilai aktual minggu tersebut (raw, belum diagregasi)
--   ytd_cumulative   = akumulasi dari week 1 s.d. week ini
--   target_vol_2026  = target SV full year 2026 (dari sheet Edit Target)
--   target_trx_2026  = target jumlah transaksi full year 2026
--   target_fbi_2026  = target FBI full year 2026
--   mdr_rate         = MDR rate kontrak merchant
-- =============================================================================

-- Tarik SEMUA baris raw monitoring mingguan
-- JOIN dengan tabel target untuk dapat data target per merchant
-- 1 baris = 1 merchant x 1 minggu x 1 dimensi (TRX/VOL/FBI)

SELECT
    m.merchant_group,
    m.pm,
    m.year,
    m.week_number,
    m.dimensi,
    m.value,
    m.ytd_cumulative,
    -- Join target dari dim_target
    t.target_vol_2026,
    t.target_trx_2026,
    t.target_fbi_2026,
    t.mdr_rate
FROM fact_monitoring_weekly m
LEFT JOIN dim_target t
    ON m.merchant_group = t.merchant_group
WHERE m.year = 2026
ORDER BY m.merchant_group, m.dimensi, m.week_number;
