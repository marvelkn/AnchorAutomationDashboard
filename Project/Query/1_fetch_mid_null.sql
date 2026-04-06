-- Query 1: Fetch New Unprocessed MIDs for the MID Cleaner
-- Updated to query directly from your `staging.db` database table

SELECT DISTINCT 
    MERCHANT_ID, 
    MERCHANT_NAME, 
    EQUIP 
FROM 
    raw_edw_mid 
WHERE 
    (IS_PROCESSED_BY_ETL = 0 OR MAPPED_MERCHANT_GROUP IS NULL)
    AND EDW_FETCH_DATE >= '2026-03-01 00:00:00' 
    AND EDW_FETCH_DATE <= '2026-03-31 23:59:59';
