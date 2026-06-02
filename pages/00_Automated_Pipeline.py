"""
Automated Pipeline — Neon ingest UI.

After the SQLite-fallback removal (see plan act-as-a-senior-glistening-lovelace.md)
this page is strictly cloud-only. It does three things:

1. Validates that the three master files required by the ML pipeline
   (master_mid, master_card, master_mon) are present in Neon's
   master_files table BEFORE allowing any ingest. Missing masters block
   the ingest button.
2. Runs the full SQLite -> Neon ingest (`ingest_sqlite_bytes_to_neon`).
3. Exposes maintenance (scrub / VACUUM / reset) and an ingestion audit log.

If `DATABASE_URL` is not set the page renders a configuration error and
stops. There is no longer a local-mode pipeline UI on this page.
"""

import logging
import os
import sys
import re as _re

import streamlit as st
import pandas as pd

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

from utils.theme import (
    apply_theme, page_header, section_label, pipeline_stepper, DANGER,
)
from utils.cloud_db import build_engine, test_connection, read_uploaded_dataframe, upsert_dataframe
from utils.sqlite_to_neon import ingest_sqlite_bytes_to_neon, fetch_recent_ingestion_runs
from utils.master_files_db import list_master_files, sync_all_masters_to_disk
from utils.rate_limiter import enforce_rate_limit, is_pipeline_cooling_down, set_pipeline_cooldown

log = logging.getLogger(__name__)


st.set_page_config(
    page_title="Automated Pipeline — BTN Anchor",
    page_icon=os.path.join(_BASE, "static", "btn_logo.png"),
    layout="wide",
)
apply_theme()
enforce_rate_limit("pipeline_page", max_calls=30, window_seconds=60, label="page loads")

# ── Strict Neon gate ──────────────────────────────────────────────────────────
if not bool(os.getenv("DATABASE_URL")):
    page_header("", "Automated pipeline", "Cloud database not configured")
    st.error(
        "**Cloud database not configured.** This app is Neon-only — set the "
        "`DATABASE_URL` environment variable to your Neon connection string "
        "and restart the app."
    )
    st.stop()

# ── Paths (local Excel cache for governance) ──────────────────────────────────
MASTER_DIR = os.path.join(_BASE, "data", "master")
PATH_MID   = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD  = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON   = os.path.join(MASTER_DIR, "master_monitoring.xlsx")
os.makedirs(MASTER_DIR, exist_ok=True)

@st.cache_resource
def _get_cloud_engine():
    """One pooled SQLAlchemy engine per session — avoids per-rerun pool churn."""
    return build_engine()


# ── Sync master files from Neon -> local disk (refresh ephemeral cloud FS) ────
# Best-effort: if Neon is unreachable, the master-file check below will catch
# the truly-missing case loudly anyway. Uses the cached engine so a rerun does
# not spin up (and leak) a fresh connection pool.
try:
    sync_all_masters_to_disk(_get_cloud_engine(), PATH_MID, PATH_CARD, PATH_MON)
except Exception:
    log.warning("master-file sync to disk failed; the pre-flight gate below still validates Neon", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# MASTER-FILE PRE-FLIGHT GATE  (Task 1)
# ══════════════════════════════════════════════════════════════════════════════
# The ML pipeline cannot run correctly without these three reference files.
# We check Neon's `master_files` table directly — disk presence is not enough
# because the cloud filesystem is ephemeral and could drift from Neon.

_REQUIRED_MASTERS = {
    "master_mid":  "ALL MID master (master_mid.xlsx)",
    "master_card": "CARDSHARE master (master_card_share.xlsx)",
    "master_mon":  "WEEKLY master (master_monitoring.xlsx)",
}


def _check_required_masters(engine) -> tuple[bool, list[str]]:
    """Inspect Neon's master_files table; return (all_present, missing_labels).

    Any DB failure is treated as "all missing" so the user sees a clear error
    rather than a silent ingest that produces broken analytics."""
    try:
        info = list_master_files(engine)
    except Exception:
        return False, list(_REQUIRED_MASTERS.values())

    missing = [
        label for key, label in _REQUIRED_MASTERS.items()
        if info.get(key) is None
    ]
    return (len(missing) == 0), missing


def _render_master_gate(missing_labels: list[str]) -> None:
    """Render the blocking error + link to Master Configuration."""
    st.error(
        "**Pipeline locked — master files missing from Neon.** "
        "The ML pipeline requires the following reference files to run, "
        "but they are not yet uploaded:\n\n"
        + "".join(f"- {label}\n" for label in missing_labels)
        + "\nUpload them in **Global Settings** before ingesting any "
        "database. Ingesting without these files would produce broken or "
        "blank analytics on the Dashboard."
    )
    st.page_link(
        "pages/0_Master_Configuration.py",
        label="Go to Global Settings to upload master files",
        icon=":material/settings:",
    )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER + ENGINE
# ══════════════════════════════════════════════════════════════════════════════
page_header(
    "",
    "Automated pipeline — cloud",
    "Load staging data into Neon (PostgreSQL)",
)
st.markdown(
    """<div class="tab-desc">
    <p><b>Choose how you want to load data</b> — both options use the same Neon database:</p>
    <ul style="margin:0.35rem 0 0 1rem;line-height:1.55;">
    <li><b>Full database</b> — Upload your SQLite <code>.db</code> file (e.g. <code>staging.db</code>).
    All business tables are copied into Neon in one step (each table is replaced on load). Use this when you have a full export.</li>
    <li><b>Optional manual file</b> — Upload a single <b>CSV</b> or <b>Excel</b> file to upsert <i>one</i> table only.
    Use this for small corrections without re-uploading the whole database.</li>
    </ul>
    <p style="margin-top:0.65rem;opacity:0.88;font-size:var(--fs-base);">
    <b>Bahasa:</b> <i>(1) Upload <b>.db</b> lengkap = semua tabel masuk Neon sekaligus.
    (2) <b>Opsional</b> — CSV/Excel untuk perbarui <b>satu tabel</b> saja.</i>
    </p>
    <p style="margin-top:0.5rem;opacity:0.85;font-size:var(--fs-sm);">
    Phase-1 cloud ingestion only — it does not run the legacy Windows + Excel COM analytics pipeline.</p>
    </div>""",
    unsafe_allow_html=True,
)


try:
    engine = _get_cloud_engine()
    test_connection(engine)
except Exception as conn_err:
    st.error(f"Neon connection failed: {conn_err}")
    engine = None


# ── Tabs ──────────────────────────────────────────────────────────────────────
cloud_tab_ingest, cloud_tab_maintenance, cloud_tab_audit = st.tabs(
    ["Ingest Data", "Maintenance", "Audit Log"]
)

# ──────────────────────────────────────────────────────────────────────────────
# TAB 1: INGEST
# ──────────────────────────────────────────────────────────────────────────────
with cloud_tab_ingest:
    pipeline_stepper(
        [("📤", "Upload"), ("🔍", "Validate"), ("☁", "Push to Neon")],
        current_step=-1,
    )

    # ── Master-file pre-flight gate (Task 1) ──────────────────────────────────
    if engine is None:
        masters_ready = False
    else:
        masters_ready, missing_labels = _check_required_masters(engine)
        if not masters_ready:
            _render_master_gate(missing_labels)

    section_label("A — Full SQLite database")
    neon_schema_ingest = st.text_input(
        "Schema for imported tables",
        value="public",
        key="neon_schema_ingest",
        help="Table names are lowercased in Neon (e.g. ALL_MID -> all_mid).",
    )
    _IDENT_RE = _re.compile(r'^[a-z][a-z0-9_]*$')
    _schema_val = (neon_schema_ingest or "public").strip() or "public"
    if not _IDENT_RE.match(_schema_val):
        st.error("Schema name must start with a letter and contain only lowercase letters, digits, and underscores (e.g. 'public').")
        st.stop()
    neon_schema_ingest = _schema_val
    cloud_db_upload = st.file_uploader(
        "SQLite file (.db / .sqlite)",
        type=["db", "sqlite"],
        key="cloud_db_full_upload",
        help="Written to a temp file only while reading; data is loaded into Neon.",
    )
    if st.button(
        "Ingest full database to Neon",
        type="primary",
        width="stretch",
        disabled=(engine is None or not masters_ready),
        key="btn_ingest_full_db",
    ):
        # Defense-in-depth: re-check master files at click time. Streamlit
        # reruns can race the disabled-attribute, and a master could be
        # deleted between page load and click.
        _ready_now, _missing_now = _check_required_masters(engine)
        if not _ready_now:
            st.error(
                "Ingest blocked — master files are still missing in Neon: "
                + ", ".join(_missing_now)
            )
            st.stop()

        if not cloud_db_upload:
            st.warning("Please upload a .db file first.")
        else:
            _blocked, _remaining = is_pipeline_cooling_down()
            if _blocked:
                st.warning(f"Ingest ran recently — please wait {_remaining:.0f}s before running again.", icon="⏳")
                st.stop()
            set_pipeline_cooldown()
            progress_placeholder = st.empty()
            status_placeholder   = st.empty()
            results_placeholder  = st.empty()

            try:
                def _on_progress(cur: int, total: int, tbl: str, msg: str):
                    frac = min(cur / max(total, 1), 1.0)
                    with progress_placeholder.container():
                        st.progress(frac, text=(msg[:120] if msg else "…"))
                    with status_placeholder.container():
                        st.info(f"Ingesting table {cur}/{total}: `{tbl}`")

                with st.spinner("Ingesting SQLite -> Neon…"):
                    result = ingest_sqlite_bytes_to_neon(
                        engine,
                        cloud_db_upload.getvalue(),
                        schema=(neon_schema_ingest or "public").strip() or "public",
                        source_filename=getattr(cloud_db_upload, "name", "uploaded.db") or "uploaded.db",
                        progress_callback=_on_progress,
                    )

                progress_placeholder.empty()
                status_placeholder.empty()

                _ok     = result.get("tables_ok", 0)
                _total  = (_ok + result.get("tables_failed", 0))
                _rows   = result.get("total_rows", 0)
                _elapsed = result.get("elapsed_seconds", 0)
                _failed  = result.get("tables_failed", 0)

                with results_placeholder.container():
                    if result.get("status") == "complete":
                        st.cache_data.clear()
                        st.success(f"Ingest complete · run `{result.get('run_id')}`")
                    elif result.get("status") == "partial_error":
                        st.cache_data.clear()
                        st.warning(f"Partial success · run `{result.get('run_id')}`")
                    else:
                        st.error(f"Ingest failed: {result.get('error_message', 'unknown')}")

                    _failed_color = "red" if _failed else "green"
                    _failed_meta  = "errors" if _failed else "all clear"
                    st.markdown(f"""<div class="stats-grid">
                        <div class="stat-card green">
                            <div class="stat-label">Tables Ingested</div>
                            <div class="stat-value">{_ok}/{_total}</div>
                            <div class="stat-meta">tables ok</div>
                        </div>
                        <div class="stat-card blue">
                            <div class="stat-label">Total Rows</div>
                            <div class="stat-value">{_rows:,}</div>
                            <div class="stat-meta">rows loaded</div>
                        </div>
                        <div class="stat-card amber">
                            <div class="stat-label">Elapsed Time</div>
                            <div class="stat-value">{_elapsed:.1f}s</div>
                            <div class="stat-meta">ingest duration</div>
                        </div>
                        <div class="stat-card {_failed_color}">
                            <div class="stat-label">Failed Tables</div>
                            <div class="stat-value">{_failed}</div>
                            <div class="stat-meta">{_failed_meta}</div>
                        </div>
                    </div>""", unsafe_allow_html=True)

                    tr = result.get("table_results") or []
                    if tr:
                        st.dataframe(pd.DataFrame(tr), width="stretch", hide_index=True)
                    with st.expander("Technical details (JSON)"):
                        st.json(result.get("details") or {})

            except Exception as ingest_err:
                progress_placeholder.empty()
                status_placeholder.empty()
                with results_placeholder.container():
                    st.error(f"Full database ingest failed: {ingest_err}")

    with st.expander("B — Optional: single-table upsert (CSV / Excel)", expanded=False):
        st.caption(
            "Target table must already exist in Neon with a PRIMARY KEY or UNIQUE constraint on your conflict column(s)."
        )
        cloud_upload = st.file_uploader(
            "File",
            type=["csv", "xlsx", "xls"],
            key="cloud_upload",
        )
        u1, u2, u3 = st.columns(3)
        cloud_table = u1.text_input("Target table", value="target")
        cloud_schema = u2.text_input("Schema", value="public")
        cloud_keys_raw = u3.text_input("Conflict key(s)", value="merchant_group,pm")
        if st.button(
            "Run upsert",
            type="primary",
            width="stretch",
            disabled=(engine is None or not masters_ready),
            key="btn_cloud_upsert",
        ):
            if not cloud_upload:
                st.warning("Please upload a CSV or Excel file first.")
            else:
                prog = st.progress(0.0, text="Starting…")
                try:
                    with st.spinner("Reading file…"):
                        cloud_df = read_uploaded_dataframe(cloud_upload)
                    prog.progress(0.35, text=f"Parsed {len(cloud_df):,} rows")
                    conflict_cols = [x.strip() for x in cloud_keys_raw.split(",") if x.strip()]
                    with st.spinner("Upserting…"):
                        affected = upsert_dataframe(
                            engine=engine,
                            dataframe=cloud_df,
                            table_name=cloud_table.strip(),
                            conflict_columns=conflict_cols,
                            schema=(cloud_schema.strip() or "public"),
                        )
                    prog.progress(1.0, text="Done")
                    st.cache_data.clear()
                    st.success(
                        f"Upsert complete · {affected:,} row(s) -> `{cloud_schema}.{cloud_table}`."
                    )
                    st.dataframe(cloud_df.head(20), width="stretch")
                except Exception as upload_err:
                    st.error(f"Upsert failed: {upload_err}")


# ──────────────────────────────────────────────────────────────────────────────
# TAB 2: MAINTENANCE
# ──────────────────────────────────────────────────────────────────────────────
with cloud_tab_maintenance:
    section_label("Database Maintenance")
    st.info(
        "Use these tools to clean your **Neon (PostgreSQL)** database. Fixes data anomalies like duplicates or historical spikes."
    )
    with st.expander("Scrub / de-duplicate Neon Cloud Database", expanded=False):
        st.markdown(
            "Removes duplicates in **all** Neon tables — both staging "
            "(ALL_MID, CARD_SHARE, WEEKLY_MONITOR) and processed "
            "(PROCESSED_CARD_MONTHLY, etc.) — then applies the Yoshinoya normalization fix."
        )

        # ── Duplicate diagnostics ────────────────────────────────────────────
        if st.button("Check duplicate counts", key="btn_diag_neon", disabled=(engine is None)):
            _schema_diag = (neon_schema_ingest or "public").strip() or "public"
            _diag_queries = {
                "all_mid":                     ("merchant_id", "terminal_id"),
                "card_share":                  ("merchant_group", "merchant_brand", "transaction_month"),
                "weekly_monitor":              ("merchant_group", "year", "week_num"),
                "processed_card_share":        ("merchant_group", "merchant_brand", "transaction_month"),
                "processed_card_history":      ("merchant_group", "merchant_brand", "transaction_month"),
                "processed_card_monthly":      ("merchant_group", "merchant_brand", "transaction_month"),
                "processed_monitoring":        ("merchant_group", "pm"),
                "processed_monitoring_weekly": ("merchant_group", "year", "week_num"),
            }
            _diag_rows = []
            try:
                from sqlalchemy import text as _satext
                with engine.connect() as _dc:
                    for _tbl, _keys in _diag_queries.items():
                        _key_expr = ", ".join(_keys)
                        try:
                            _total = _dc.execute(_satext(
                                f'SELECT COUNT(*) FROM "{_schema_diag}"."{_tbl}"'
                            )).scalar() or 0
                            _unique = _dc.execute(_satext(
                                f'SELECT COUNT(*) FROM (SELECT DISTINCT {_key_expr} FROM "{_schema_diag}"."{_tbl}") _u'
                            )).scalar() or 0
                            _diag_rows.append({
                                "Table": _tbl,
                                "Total Rows": _total,
                                "Unique Keys": _unique,
                                "Duplicates": _total - _unique,
                            })
                        except Exception:
                            _diag_rows.append({"Table": _tbl, "Total Rows": "—", "Unique Keys": "—", "Duplicates": "—"})
                st.dataframe(pd.DataFrame(_diag_rows), use_container_width=True)
            except Exception as _de:
                st.error(f"Diagnostics failed: {_de}")

        # ── Scrub button ─────────────────────────────────────────────────────
        if st.button(
            "Run cloud scrub / de-duplicate",
            type="primary",
            width="stretch",
            disabled=(engine is None),
            key="btn_scrub_neon_cloud",
        ):
            with st.spinner("Cleaning Neon PostgreSQL tables..."):
                try:
                    from repair_data import scrub_neon_database, scrub_staging_neon
                    target_schema = (neon_schema_ingest or "public").strip() or "public"
                    staging_res   = scrub_staging_neon(engine, schema=target_schema)
                    processed_res = scrub_neon_database(engine, schema=target_schema)
                    st.success("Cloud scrub complete — staging + processed tables cleaned!")
                    st.json({"staging_tables": staging_res, "processed_tables": processed_res})
                except Exception as e:
                    st.error(f"Cloud scrub failed: {e}")

        # ── VACUUM ANALYZE button ────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Reclaim physical disk space** after scrubbing (runs `VACUUM ANALYZE` on each table).")
        if st.button("VACUUM Neon Tables", disabled=(engine is None), key="btn_vacuum_neon"):
            _vac_schema = (neon_schema_ingest or "public").strip() or "public"
            _vac_tables = [
                "all_mid", "card_share", "weekly_monitor",
                "processed_card_share", "processed_card_history",
                "processed_card_monthly", "processed_monitoring",
                "processed_monitoring_weekly", "target",
            ]
            _vac_results = {}
            with st.spinner("Running VACUUM ANALYZE on Neon tables..."):
                try:
                    import psycopg2
                    _raw = psycopg2.connect(os.getenv("DATABASE_URL"))
                    _raw.autocommit = True
                    _cur = _raw.cursor()
                    for _vt in _vac_tables:
                        try:
                            _cur.execute(f'VACUUM ANALYZE "{_vac_schema}"."{_vt}"')
                            _vac_results[_vt] = "OK"
                        except Exception as _ve:
                            _vac_results[_vt] = f"Skipped: {_ve}"
                    _raw.close()
                    st.success("VACUUM ANALYZE complete!")
                    st.json(_vac_results)
                except Exception as _verr:
                    st.warning(f"VACUUM requires psycopg2 direct connection: {_verr}")

    # ── Danger Zone: Neon reset ──────────────────────────────────────────────
    with st.expander("⚠ Danger Zone — Reset Neon Cloud Database", expanded=False):
        st.error(
            "**This will permanently PURGE ALL DATA** from business, raw, and audit "
            "tables in your Neon production database. There is no undo. "
            "Two-step confirmation is required."
        )
        _reset_schema = (neon_schema_ingest or "public").strip() or "public"
        st.markdown(f"**Target schema:** `{_reset_schema}`")

        confirm_reset_neon = st.checkbox(
            "I understand this will permanently delete all data in the cloud (PostgreSQL).",
            key="confirm_reset_neon_cloud",
        )
        _typed_confirm = st.text_input(
            f"To confirm, type the schema name **`{_reset_schema}`** below exactly:",
            key="reset_typed_confirm",
            placeholder=_reset_schema,
            help="This second confirmation prevents accidental purges from a misclicked checkbox.",
        )
        _typed_match = (_typed_confirm or "").strip() == _reset_schema

        if not confirm_reset_neon:
            st.caption("☐ Step 1: tick the acknowledgement checkbox above.")
        elif not _typed_match:
            st.caption(f"☐ Step 2: type `{_reset_schema}` exactly to enable the reset button.")
        else:
            st.caption("✓ Both confirmations received. Reset is now armed.")

        if st.button(
            "RESET NEON CLOUD DATABASE",
            type="primary",
            disabled=not (confirm_reset_neon and _typed_match),
            width="stretch",
            key="btn_reset_neon_cloud",
        ):
            with st.spinner("Purging Neon PostgreSQL tables..."):
                try:
                    from repair_data import reset_neon_database
                    target_schema = _reset_schema
                    results = reset_neon_database(engine, schema=target_schema)
                    st.success("Neon database reset successfully!")
                    st.json(results)
                except Exception as e:
                    st.error(f"Reset failed: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# TAB 3: AUDIT LOG
# ──────────────────────────────────────────────────────────────────────────────
with cloud_tab_audit:
    section_label("Recent Ingestion Runs")
    if engine is not None:
        try:
            _aud_schema = st.session_state.get("neon_schema_ingest", "public") or "public"
            st.caption(f"Table `{_aud_schema}.ingestion_runs` — last 10 runs.")
            st.dataframe(
                fetch_recent_ingestion_runs(engine, schema=_aud_schema, limit=10),
                width="stretch",
                hide_index=True,
            )
        except Exception as aud_err:
            st.info(f"No ingestion history yet. Run an ingest to populate this log. ({aud_err})")
    else:
        st.warning("Connect to Neon first to view audit logs.")
