-- Query 2: Fetch Card Share Data for Anchor Merchants (New/Unprocessed Data)
--
-- FIX: Replaced INNER JOIN master_mid with an EXISTS subquery.
-- The original INNER JOIN caused a fan-out because master_mid is a MID-level
-- table (one row per terminal). Each (MERCHANT_GROUP, MERCHANT_BRAND) pair
-- had ~560 matching MID rows, multiplying 3,369 card share records to
-- 1,888,641 rows -- exceeding Excel's 1,048,576 row limit and causing
-- the COM error 0x800A03EC. EXISTS filters correctly with no row multiplication.

SELECT
    c.TRANSACTION_MONTH,
    c.MERCHANT_GROUP,
    c.MERCHANT_BRAND,
    c.TRX_DEBIT_ONUS,
    c.TRX_DEBIT_OFFUS,
    c.TRX_CREDIT_OFFUS,
    c.TRX_QRIS_ONUS,
    c.TRX_QRIS_OFFUS,
    c.VOL_DEBIT_ONUS,
    c.VOL_DEBIT_OFFUS,
    c.VOL_CREDIT_OFFUS,
    c.VOL_QRIS_ONUS,
    c.VOL_QRIS_OFFUS,
    c.FBI_DEBIT_ONUS,
    c.FBI_DEBIT_OFFUS,
    c.FBI_CREDIT_OFFUS,
    c.FBI_QRIS_ONUS,
    c.FBI_QRIS_OFFUS
FROM
    CARD_SHARE c
WHERE
    c.IS_PROCESSED_BY_ETL = 0
    AND c.EDW_FETCH_DATE >= '2026-03-01 00:00:00'
    AND c.EDW_FETCH_DATE <= '2026-03-31 23:59:59'
    AND EXISTS (
        SELECT 1
        FROM   PROCESSED_MID m
        WHERE  m.MERCHANT_GROUP = c.MERCHANT_GROUP
          AND  m.MERCHANT_BRAND = c.MERCHANT_BRAND
          AND  m.SEGMENT = 'ANCHOR'
    );
