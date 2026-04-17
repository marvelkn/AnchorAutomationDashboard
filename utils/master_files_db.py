"""
utils/master_files_db.py
------------------------
Cloud-native persistence for the three master reference Excel files:
  - master_mid        (ALL MID Master)
  - master_card       (Card Share Master)
  - master_mon        (Monitoring Master)

Files are stored as BYTEA blobs in a Neon `master_files` table.
Local disk copies are always kept in sync as a read-cache so the
existing Windows pipeline code (which uses file paths) keeps working.

Public API
----------
ensure_master_files_table(engine)
save_master_to_db(engine, file_key, filename, content_bytes)  -> bool
load_master_from_db(engine, file_key)                          -> (filename, bytes) | (None, None)
list_master_files(engine)                                      -> dict[file_key -> info_dict]
sync_master_to_disk(engine, file_key, dest_path)               -> bool
sync_all_masters_to_disk(engine, path_mid, path_card, path_mon) -> dict
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

# WIB (Western Indonesia Time) = UTC+7
_LOCAL_TZ = timezone(timedelta(hours=7))

# ── Constants ─────────────────────────────────────────────────────────────────
VALID_KEYS = {"master_mid", "master_card", "master_mon"}

_DDL = """
CREATE TABLE IF NOT EXISTS public.master_files (
    file_key   TEXT PRIMARY KEY,
    filename   TEXT NOT NULL,
    content    BYTEA NOT NULL,
    size_bytes BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_key(file_key: str) -> None:
    if file_key not in VALID_KEYS:
        raise ValueError(f"Invalid file_key '{file_key}'. Must be one of {VALID_KEYS}")


# ── Public API ────────────────────────────────────────────────────────────────

def ensure_master_files_table(engine: Engine) -> None:
    """Create the master_files table in Neon if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text(_DDL))


def save_master_to_db(
    engine: Engine,
    file_key: str,
    filename: str,
    content_bytes: bytes,
) -> bool:
    """
    Upsert a master file into Neon.

    Parameters
    ----------
    engine        : SQLAlchemy Engine connected to Neon.
    file_key      : One of 'master_mid', 'master_card', 'master_mon'.
    filename      : Original filename (used for the download button).
    content_bytes : Raw bytes of the Excel file.

    Returns
    -------
    True on success, False on failure.
    """
    _check_key(file_key)
    try:
        ensure_master_files_table(engine)
        upsert_sql = text("""
            INSERT INTO public.master_files (file_key, filename, content, size_bytes, updated_at)
            VALUES (:key, :fname, :content, :sz, NOW())
            ON CONFLICT (file_key) DO UPDATE SET
                filename   = EXCLUDED.filename,
                content    = EXCLUDED.content,
                size_bytes = EXCLUDED.size_bytes,
                updated_at = NOW()
        """)
        with engine.begin() as conn:
            conn.execute(upsert_sql, {
                "key":     file_key,
                "fname":   filename,
                "content": content_bytes,
                "sz":      len(content_bytes),
            })
        return True
    except Exception:
        return False


def load_master_from_db(
    engine: Engine,
    file_key: str,
) -> Tuple[Optional[str], Optional[bytes]]:
    """
    Load a master file from Neon.

    Returns
    -------
    (filename, content_bytes)  — or  (None, None) if not found / error.
    """
    _check_key(file_key)
    try:
        ensure_master_files_table(engine)
        q = text("""
            SELECT filename, content
            FROM public.master_files
            WHERE file_key = :key
        """)
        with engine.connect() as conn:
            row = conn.execute(q, {"key": file_key}).fetchone()
        if row is None:
            return None, None
        fname, raw = row
        # SQLAlchemy / psycopg2 may return memoryview for BYTEA
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        return fname, raw
    except Exception:
        return None, None


def list_master_files(engine: Engine) -> dict:
    """
    Return metadata for all master files stored in Neon.

    Returns
    -------
    dict keyed by file_key, each value is:
        { 'filename': str, 'size_bytes': int, 'updated_at': str }
    If a key is missing from the DB, its value is None.
    """
    result = {k: None for k in VALID_KEYS}
    try:
        ensure_master_files_table(engine)
        q = text("""
            SELECT file_key, filename, size_bytes, updated_at
            FROM public.master_files
            WHERE file_key = ANY(:keys)
        """)
        with engine.connect() as conn:
            rows = conn.execute(q, {"keys": list(VALID_KEYS)}).fetchall()
        for row in rows:
            key, fname, sz, updated_at = row
            result[key] = {
                "filename":   fname,
                "size_bytes": sz or 0,
                "updated_at": (
                    updated_at.astimezone(_LOCAL_TZ).strftime("%d %b %Y, %H:%M")
                    if updated_at else "Unknown"
                ),
            }
    except Exception:
        pass
    return result


def sync_master_to_disk(
    engine: Engine,
    file_key: str,
    dest_path: str,
) -> bool:
    """
    Pull a master file from Neon and write it to *dest_path* on disk.

    This keeps the local file cache fresh so that code that reads
    local paths (e.g. the governance check in Automated Pipeline)
    always has the latest version.

    Returns True if the file was written, False otherwise.
    """
    _check_key(file_key)
    fname, content = load_master_from_db(engine, file_key)
    if content is None:
        return False
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(content)
        return True
    except Exception:
        return False


def sync_all_masters_to_disk(
    engine: Engine,
    path_mid: str,
    path_card: str,
    path_mon: str,
) -> dict:
    """
    Sync all three master files from Neon → local disk.

    Returns
    -------
    dict with keys 'master_mid', 'master_card', 'master_mon'
    mapped to True (synced) or False (not in DB / error).
    """
    return {
        "master_mid":  sync_master_to_disk(engine, "master_mid",  path_mid),
        "master_card": sync_master_to_disk(engine, "master_card", path_card),
        "master_mon":  sync_master_to_disk(engine, "master_mon",  path_mon),
    }
