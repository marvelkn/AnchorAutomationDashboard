import sqlite3
import os
from pathlib import Path

def merge_incremental_data(source_db_path, target_db_path):
    """
    Merges data from source_db to target_db only if it doesn't exist.
    Uses natural keys to define 'newness'.
    """
    src = Path(source_db_path).resolve()
    if src.suffix.lower() != ".db":
        raise ValueError(f"Invalid source DB path (must be .db): {source_db_path}")
    if any(c in str(src) for c in ["'", '"', ";"]):
        raise ValueError(f"Path contains illegal characters: {source_db_path}")

    if not src.exists() or not os.path.exists(target_db_path):
        return 0

    conn_target = sqlite3.connect(target_db_path)
    cursor_target = conn_target.cursor()

    # Attach the source database (path validated above)
    cursor_target.execute(f"ATTACH DATABASE '{src}' AS source")

    tables_and_keys = {
        "ALL_MID": ["MERCHANT_ID", "TERMINAL_ID"],
        "CARD_SHARE": ["MERCHANT_GROUP", "MERCHANT_BRAND", "TRANSACTION_MONTH"],
        "WEEKLY_MONITOR": ["MERCHANT_GROUP", "YEAR", "WEEK_NUM"]
    }

    total_added = 0

    for table, keys in tables_and_keys.items():
        # Get all columns except 'ID' (since it's autoincrement in target)
        cursor_target.execute(f"PRAGMA table_info({table})")
        cols = [col[1] for col in cursor_target.fetchall() if col[1].upper() != 'ID']
        cols_str = ", ".join(cols)

        # Build the WHERE NOT EXISTS clause
        # Using 'IS' instead of '=' to correctly handle NULL values
        key_matches = " AND ".join([f"main.{k} IS source.{k}" for k in keys])
        
        sql = f"""
            INSERT INTO main.{table} ({cols_str})
            SELECT DISTINCT {cols_str} FROM source.{table} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM main.{table} AS main
                WHERE {key_matches}
            )
        """
        
        try:
            cursor_target.execute(sql)
            total_added += cursor_target.rowcount
            
            # Also update a metadata table if it exists
            # We'll create it later in Phase 3
        except sqlite3.OperationalError as e:
            # Table might not exist in source or target
            print(f"Skipping {table}: {e}")

    # Update metadata timestamp
    cursor_target.execute("CREATE TABLE IF NOT EXISTS APP_METADATA (key TEXT PRIMARY KEY, value TEXT)")
    import datetime
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor_target.execute("INSERT OR REPLACE INTO APP_METADATA (key, value) VALUES ('LAST_DATA_UPDATE', ?)", (now_str,))
    cursor_target.execute("INSERT OR REPLACE INTO APP_METADATA (key, value) VALUES ('NEW_DATA_SIGNAL', '1')")

    conn_target.commit()
    cursor_target.execute("DETACH DATABASE source")
    conn_target.close()

    return total_added
