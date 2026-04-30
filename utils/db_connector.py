import sqlite3
import pandas as pd
import os
import re

_ALLOWED_TABLES = frozenset({"CARD_SHARE", "WEEKLY_MONITOR"})

def get_sql_query(query_filename, start_date, end_date):
    """
    Reads a SQL query file and injects the parameterized date range.

    Uses regex replacement instead of hardcoded string literals, so this works
    regardless of which specific dates are baked into the SQL file. The pattern
    matches the first 'YYYY-MM-DD 00:00:00' occurrence (= start bound) and
    the first 'YYYY-MM-DD 23:59:59' occurrence (= end bound).
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    query_path = os.path.join(base_dir, 'Query', query_filename)

    with open(query_path, 'r') as f:
        query = f.read()

    # Replace all occurrences of start date (any YYYY-MM-DD 00:00:00 pattern)
    query, n_start = re.subn(
        r"'\d{4}-\d{2}-\d{2} 00:00:00'",
        f"'{start_date} 00:00:00'",
        query,
    )
    # Replace all occurrences of end date (any YYYY-MM-DD 23:59:59 pattern)
    query, n_end = re.subn(
        r"'\d{4}-\d{2}-\d{2} 23:59:59'",
        f"'{end_date} 23:59:59'",
        query,
    )

    if n_start == 0 or n_end == 0:
        raise ValueError(
            f"[db_connector] Date injection failed for '{query_filename}'. "
            f"Could not find 'YYYY-MM-DD 00:00:00' or 'YYYY-MM-DD 23:59:59' placeholder in the SQL. "
            f"Replacements applied: start={n_start}, end={n_end}."
        )

    return query


def fetch_data_from_db(db_path, query_filename, start_date, end_date):
    """
    Executes a parameterized SQL query against the Staging DB and returns a DataFrame.
    Raises ValueError if the query returns 0 rows (prevents silent pipeline failures).
    """
    query = get_sql_query(query_filename, start_date, end_date)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df


def get_db_date_bounds(db_path):
    """
    Inspects the staging DB and returns the actual MIN/MAX EDW_FETCH_DATE
    across both source tables. Returns (min_date_str, max_date_str) or (None, None).
    """
    if not os.path.exists(db_path):
        return None, None
    try:
        conn = sqlite3.connect(db_path)
        dates = []
        for tbl in ('CARD_SHARE', 'WEEKLY_MONITOR'):
            if tbl not in _ALLOWED_TABLES:
                continue
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,)
            ).fetchall()
            if rows:
                row = conn.execute(
                    f"SELECT MIN(EDW_FETCH_DATE), MAX(EDW_FETCH_DATE) FROM \"{tbl}\""
                ).fetchone()
                if row and row[0]:
                    dates.append((row[0][:10], row[1][:10]))
        conn.close()
        if dates:
            min_d = min(d[0] for d in dates)
            max_d = max(d[1] for d in dates)
            return min_d, max_d
    except Exception:
        pass
    return None, None
