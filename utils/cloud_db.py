import os
from io import BytesIO
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def build_engine() -> Engine:
    """Create pooled SQLAlchemy engine for Neon/PostgreSQL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not configured.")

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
        pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
        future=True,
    )


def test_connection(engine: Engine) -> None:
    """Raise if the DB connection is not healthy."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def read_uploaded_dataframe(uploaded_file) -> pd.DataFrame:
    """Read CSV/XLS/XLSX file from Streamlit uploader in-memory."""
    raw = uploaded_file.getvalue()
    filename = uploaded_file.name.lower()
    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(raw))
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(BytesIO(raw))
    raise ValueError("Unsupported file type. Use CSV or Excel.")


def upsert_dataframe(
    engine: Engine,
    dataframe: pd.DataFrame,
    table_name: str,
    conflict_columns: Iterable[str],
    schema: str = "public",
) -> int:
    """Bulk insert to temp table then upsert into target table."""
    df = dataframe.copy()
    if df.empty:
        raise ValueError("Uploaded file is empty.")

    df.columns = [str(col).strip().lower() for col in df.columns]
    conflict_cols = [str(col).strip().lower() for col in conflict_columns if str(col).strip()]
    if not conflict_cols:
        raise ValueError("Conflict columns are required.")

    missing = [col for col in conflict_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing conflict column(s): {', '.join(missing)}")

    temp_table = f"_tmp_{table_name}"
    all_cols = [str(col) for col in df.columns]
    quoted_cols = ", ".join([f'"{col}"' for col in all_cols])
    conflict_clause = ", ".join([f'"{col}"' for col in conflict_cols])
    update_cols = [col for col in all_cols if col not in conflict_cols]
    if not update_cols:
        raise ValueError("At least one non-conflict column is required for update.")
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    upsert_sql = f"""
    INSERT INTO "{schema}"."{table_name}" ({quoted_cols})
    SELECT {quoted_cols}
    FROM "{schema}"."{temp_table}"
    ON CONFLICT ({conflict_clause})
    DO UPDATE SET {set_clause};
    """

    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"'))
        conn.execute(
            text(
                f'CREATE TABLE "{schema}"."{temp_table}" '
                f'(LIKE "{schema}"."{table_name}" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'
            )
        )
        df.to_sql(
            temp_table,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"'))

    return len(df)
