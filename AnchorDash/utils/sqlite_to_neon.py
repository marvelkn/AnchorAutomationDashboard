"""
Cloud-native ingestion: copy all user tables from an uploaded SQLite .db into Neon PostgreSQL.

Phase-1: per-table replace (TRUNCATE + bulk insert). Column names normalized to lowercase.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import sqlite3
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Chunk size for reading from SQLite and writing to Postgres
DEFAULT_CHUNKSIZE = 10_000

ProgressCallback = Optional[Callable[[int, int, str, str], None]]


def _pg_ident(name: str) -> str:
    """Safe identifier: only allow alphanumeric + underscore after lowercasing."""
    s = name.strip().lower()
    if not re.match(r"^[a-z][a-z0-9_]*$", s):
        raise ValueError(f"Invalid identifier: {name!r}")
    return s


def _sqlite_user_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [r[0] for r in rows]


def _sqlite_row_count(conn: sqlite3.Connection, table: str) -> int:
    # table name from sqlite_master only — safe for interpolation in controlled context
    q = f'SELECT COUNT(*) FROM "{table}"'
    return int(conn.execute(q).fetchone()[0])


def _sqlite_column_info(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str, int]]:
    """Returns list of (name, sqlite_declared_type, pk_flag)."""
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    out = []
    for r in rows:
        # cid, name, type, notnull, dflt_value, pk
        out.append((r[1], r[2] or "TEXT", int(r[5] or 0)))
    return out


def _map_sqlite_type_to_pg(decl: str) -> str:
    d = (decl or "TEXT").upper().strip()
    if "INT" in d:
        return "BIGINT"
    if "CHAR" in d or "CLOB" in d or "TEXT" in d:
        return "TEXT"
    if "BLOB" in d:
        return "BYTEA"
    if "REAL" in d or "FLOA" in d or "DOUB" in d:
        return "DOUBLE PRECISION"
    if "BOOL" in d:
        return "BOOLEAN"
    if "DATE" in d and "TIME" not in d:
        return "TEXT"
    if "TIME" in d:
        return "TEXT"
    return "TEXT"


def _neon_table_exists(engine: Engine, schema: str, table: str) -> bool:
    q = text(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
        )
        """
    )
    with engine.connect() as conn:
        return bool(conn.execute(q, {"schema": schema, "table": table}).scalar())


def fetch_recent_ingestion_runs(
    engine: Engine, schema: str = "public", limit: int = 10
) -> pd.DataFrame:
    """Return recent rows from ingestion_runs for the Streamlit UI."""
    ensure_ingestion_audit_table(engine, schema)
    sch = _pg_ident(schema)
    q = text(
        f"""
        SELECT run_id, started_at, finished_at, status, source_filename,
               tables_total, tables_ok, tables_failed, total_rows, error_message
        FROM "{sch}".ingestion_runs
        ORDER BY started_at DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, {"lim": int(limit)}).mappings().all()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def ensure_ingestion_audit_table(engine: Engine, schema: str = "public") -> None:
    """Create ingestion_runs audit table if missing."""
    sch = _pg_ident(schema)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{sch}".ingestion_runs (
        id BIGSERIAL PRIMARY KEY,
        run_id UUID NOT NULL UNIQUE,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        source_filename TEXT,
        tables_total INTEGER,
        tables_ok INTEGER,
        tables_failed INTEGER,
        total_rows BIGINT,
        details_json JSONB,
        error_message TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _create_empty_neon_table(
    engine: Engine,
    schema: str,
    pg_table: str,
    column_info: List[Tuple[str, str, int]],
) -> None:
    sch = _pg_ident(schema)
    tbl = _pg_ident(pg_table)
    parts = []
    for name, sqlite_type, pk in column_info:
        col = _pg_ident(name.lower())
        pg_type = _map_sqlite_type_to_pg(sqlite_type)
        parts.append(f'"{col}" {pg_type}')
    cols_sql = ", ".join(parts)
    ddl = f'CREATE TABLE IF NOT EXISTS "{sch}"."{tbl}" ({cols_sql})'
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    return out


def _load_dataframe_chunks(
    sqlite_path: str,
    table: str,
    chunksize: int,
):
    """Yield normalized DataFrame chunks from SQLite."""
    q = f'SELECT * FROM "{table}"'
    for chunk in pd.read_sql_query(q, sqlite3.connect(sqlite_path), chunksize=chunksize):
        yield _normalize_chunk(chunk)


def ingest_sqlite_bytes_to_neon(
    engine: Engine,
    sqlite_bytes: bytes,
    *,
    schema: str = "public",
    source_filename: str = "uploaded.db",
    chunksize: int = DEFAULT_CHUNKSIZE,
    progress_callback: ProgressCallback = None,
) -> Dict[str, Any]:
    """
    Ingest all user tables from SQLite bytes into Neon.

    Returns dict with keys: run_id, status, tables_total, tables_ok, tables_failed,
    total_rows, table_results, error_message, elapsed_seconds.
    """
    sch = _pg_ident(schema)
    run_id = uuid.uuid4()
    started = time.perf_counter()
    table_results: List[Dict[str, Any]] = []
    total_rows = 0
    tables_ok = 0
    tables_failed = 0
    error_message: Optional[str] = None

    ensure_ingestion_audit_table(engine, sch)

    _audit_insert = text(
        f"""
        INSERT INTO "{sch}".ingestion_runs (
            run_id, started_at, status, source_filename,
            tables_total, tables_ok, tables_failed, total_rows, details_json, error_message
        ) VALUES (
            CAST(:run_id AS uuid), :started_at, 'running', :source_filename,
            NULL, NULL, NULL, NULL, NULL, NULL
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            _audit_insert,
            {
                "run_id": str(run_id),
                "started_at": datetime.now(timezone.utc),
                "source_filename": source_filename[:500],
            },
        )

    tmp_path: Optional[str] = None
    n_tables = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            tmp.write(sqlite_bytes)
            tmp_path = tmp.name

        sl_conn = sqlite3.connect(tmp_path)
        try:
            tables = _sqlite_user_tables(sl_conn)
        finally:
            sl_conn.close()

        n_tables = len(tables)
        if progress_callback:
            progress_callback(0, max(n_tables, 1), "", f"Found {n_tables} table(s)")

        for idx, raw_name in enumerate(tables):
            pg_table = raw_name.lower()
            if progress_callback:
                progress_callback(
                    idx + 1,
                    max(n_tables, 1),
                    pg_table,
                    f"Processing `{pg_table}`...",
                )

            sl_conn = sqlite3.connect(tmp_path)
            try:
                row_count = _sqlite_row_count(sl_conn, raw_name)
                col_info = _sqlite_column_info(sl_conn, raw_name)
            finally:
                sl_conn.close()

            table_needs_truncate = _neon_table_exists(engine, sch, pg_table)
            t0 = time.perf_counter()

            try:
                if row_count == 0:
                    _create_empty_neon_table(engine, sch, pg_table, col_info)
                    table_results.append(
                        {
                            "table": pg_table,
                            "source_rows": 0,
                            "loaded_rows": 0,
                            "neon_rows_after": 0,
                            "seconds": round(time.perf_counter() - t0, 3),
                            "status": "ok",
                            "note": "empty_table_schema_only",
                        }
                    )
                    tables_ok += 1
                    continue

                loaded = 0
                first_chunk = True
                for chunk in _load_dataframe_chunks(tmp_path, raw_name, chunksize):
                    n = len(chunk)
                    if n == 0:
                        continue
                    with engine.begin() as conn:
                        if first_chunk:
                            if table_needs_truncate:
                                conn.execute(
                                    text(
                                        f'TRUNCATE "{sch}"."{pg_table}" RESTART IDENTITY CASCADE'
                                    )
                                )
                                chunk.to_sql(
                                    pg_table,
                                    conn,
                                    schema=sch,
                                    if_exists="append",
                                    index=False,
                                    method="multi",
                                    chunksize=2000,
                                )
                            else:
                                chunk.to_sql(
                                    pg_table,
                                    conn,
                                    schema=sch,
                                    if_exists="replace",
                                    index=False,
                                    method="multi",
                                    chunksize=2000,
                                )
                            first_chunk = False
                        else:
                            chunk.to_sql(
                                pg_table,
                                conn,
                                schema=sch,
                                if_exists="append",
                                index=False,
                                method="multi",
                                chunksize=2000,
                            )
                    loaded += n

                # Validation: row count in Neon
                with engine.connect() as c2:
                    neon_cnt = c2.execute(
                        text(f'SELECT COUNT(*) FROM "{sch}"."{pg_table}"')
                    ).scalar()

                if int(neon_cnt) != row_count:
                    raise RuntimeError(
                        f"Row count mismatch: source={row_count}, neon={neon_cnt}"
                    )

                total_rows += loaded
                table_results.append(
                    {
                        "table": pg_table,
                        "source_rows": row_count,
                        "loaded_rows": loaded,
                        "neon_rows_after": int(neon_cnt),
                        "seconds": round(time.perf_counter() - t0, 3),
                        "status": "ok",
                    }
                )
                tables_ok += 1

            except Exception as ex:
                tables_failed += 1
                table_results.append(
                    {
                        "table": pg_table,
                        "source_rows": row_count,
                        "status": "error",
                        "error": str(ex),
                    }
                )
                error_message = str(ex)

        status = "complete" if tables_failed == 0 else "partial_error"
        if tables_failed > 0 and tables_ok == 0:
            status = "failed"

        elapsed = round(time.perf_counter() - started, 2)

        details = {
            "run_id": str(run_id),
            "schema": sch,
            "tables": table_results,
            "elapsed_seconds": elapsed,
        }

        _audit_update = text(
            f"""
            UPDATE "{sch}".ingestion_runs SET
                finished_at = :finished_at,
                status = :status,
                tables_total = :tables_total,
                tables_ok = :tables_ok,
                tables_failed = :tables_failed,
                total_rows = :total_rows,
                details_json = CAST(:details_json AS jsonb),
                error_message = :error_message
            WHERE run_id = CAST(:run_id AS uuid)
            """
        )
        with engine.begin() as conn:
            conn.execute(
                _audit_update,
                {
                    "finished_at": datetime.now(timezone.utc),
                    "status": status,
                    "tables_total": n_tables,
                    "tables_ok": tables_ok,
                    "tables_failed": tables_failed,
                    "total_rows": total_rows,
                    "details_json": json.dumps(details),
                    "error_message": error_message,
                    "run_id": str(run_id),
                },
            )

        if progress_callback:
            progress_callback(
                n_tables,
                max(n_tables, 1),
                "",
                f"Done: {status}, {tables_ok} ok, {tables_failed} failed",
            )

        return {
            "run_id": str(run_id),
            "status": status,
            "tables_total": n_tables,
            "tables_ok": tables_ok,
            "tables_failed": tables_failed,
            "total_rows": total_rows,
            "table_results": table_results,
            "error_message": error_message,
            "elapsed_seconds": elapsed,
            "details": details,
        }

    except Exception as ex:
        error_message = str(ex)
        elapsed = round(time.perf_counter() - started, 2)
        _audit_fail = text(
            f"""
            UPDATE "{sch}".ingestion_runs SET
                finished_at = :finished_at,
                status = 'failed',
                details_json = CAST(:details_json AS jsonb),
                error_message = :error_message
            WHERE run_id = CAST(:run_id AS uuid)
            """
        )
        with engine.begin() as conn:
            conn.execute(
                _audit_fail,
                {
                    "finished_at": datetime.now(timezone.utc),
                    "details_json": json.dumps({"error": error_message, "tables_total": n_tables}),
                    "error_message": error_message,
                    "run_id": str(run_id),
                },
            )
        return {
            "run_id": str(run_id),
            "status": "failed",
            "tables_total": 0,
            "tables_ok": 0,
            "tables_failed": 0,
            "total_rows": 0,
            "table_results": table_results,
            "error_message": error_message,
            "elapsed_seconds": elapsed,
            "details": {"error": error_message},
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
