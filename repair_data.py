"""
Repair / scrub utilities for the Neon (PostgreSQL) cloud database.

The previous SQLite-equivalents (scrub_database, scrub_staging_tables,
scrub_excel_card_share, scrub_excel_monitoring) were removed alongside the
local-DB fallback — see plan act-as-a-senior-glistening-lovelace.md.
The Master Configuration page already manages its own Excel backups via
utils.backup_manager, so Excel-side deduplication is no longer required here.
"""

from sqlalchemy import text


def scrub_staging_neon(engine, schema="public"):
    """
    Removes duplicates from the three raw staging tables in Neon PostgreSQL.
    Uses ctid to keep only the first occurrence per natural business key.
    Returns a dict of {table: message}.
    """
    staging_targets = {
        "all_mid":        ["merchant_id", "terminal_id"],
        "card_share":     ["merchant_group", "merchant_brand", "transaction_month"],
        "weekly_monitor": ["merchant_group", "year", "week_num"],
    }

    results = {}
    with engine.begin() as conn:
        for table, keys in staging_targets.items():
            full_table = f'"{schema}"."{table}"'
            keys_str = ", ".join(keys)
            try:
                res = conn.execute(text(f"""
                    DELETE FROM {full_table}
                    WHERE ctid NOT IN (
                        SELECT MIN(ctid)
                        FROM {full_table}
                        GROUP BY {keys_str}
                    )
                """))
                results[table] = f"Removed {res.rowcount} duplicate(s)."
            except Exception as e:
                results[table] = f"Error: {e}"
    return results


def scrub_neon_database(engine, schema="public"):
    """
    Performs scrubbing on the Neon PostgreSQL cloud database.
    Uses 'ctid' for targeting duplicates in the absence of a unique ID.
    """
    scrub_targets = {
        "processed_card_monthly": ["merchant_group", "merchant_brand", "transaction_month"],
        "processed_card_history": ["merchant_group", "merchant_brand", "transaction_month"],
        "processed_card_share":   ["merchant_group", "merchant_brand", "transaction_month"],
        "target":                 ["merchant_group", "pm"]
    }

    results = {}
    with engine.begin() as conn:
        for table, keys in scrub_targets.items():
            full_table = f"{schema}.{table}"
            keys_str = ", ".join(keys)

            scrub_sql = text(f"""
                DELETE FROM {full_table}
                WHERE ctid NOT IN (
                    SELECT MIN(ctid)
                    FROM {full_table}
                    GROUP BY {keys_str}
                )
            """)
            try:
                res = conn.execute(scrub_sql)
                results[table] = f"Removed {res.rowcount} duplicates."
            except Exception as e:
                results[table] = f"Error: {str(e)}"

        # Yoshinoya 50x Correction on Neon
        yoshi_sql = text(f"""
            UPDATE {schema}.processed_card_monthly
            SET trx_qris_offus = trx_qris_offus / 50
            WHERE UPPER(merchant_group) = 'YOSHINOYA'
              AND transaction_month LIKE '2025-03%'
              AND trx_qris_offus > 5000000
        """)
        try:
            res_y = conn.execute(yoshi_sql)
            results["yoshinoya_fix"] = f"Normalized {res_y.rowcount} record(s)."
        except Exception as e:
            results["yoshinoya_fix"] = f"Correction failed: {str(e)}"

    return results


def reset_neon_database(engine, schema="public"):
    """
    Purges all data from the Neon PostgreSQL cloud database.
    Truncates all project-relevant tables. Highly Destructive.
    """
    tables_to_clear = [
        "processed_card_monthly", "processed_card_history", "processed_card_share",
        "processed_monitoring",   "processed_monitoring_weekly", "target",
        "mart_merchant_cluster", "raw_master", "raw_card_share",
        "raw_monitoring", "raw_weekly", "raw_target", "ingestion_runs"
    ]

    results = {}
    with engine.begin() as conn:
        for table in tables_to_clear:
            check_sql = text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_schema = :schema AND table_name = :table"
                ")"
            )
            exists = conn.execute(check_sql, {"schema": schema, "table": table}).scalar()

            if exists:
                try:
                    conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY CASCADE'))
                    results[table] = "Successfully purged."
                except Exception as e:
                    try:
                        conn.execute(text(f'DELETE FROM "{schema}"."{table}"'))
                        results[table] = "Cleared (via DELETE)."
                    except Exception as e2:
                        results[table] = f"Failed to clear: {str(e2)}"
            else:
                results[table] = "Table not found (Skipped)."

    return results
