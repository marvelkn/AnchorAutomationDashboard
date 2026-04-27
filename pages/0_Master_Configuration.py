import streamlit as st
import os
import sys
import shutil
from datetime import datetime, timezone, timedelta
from io import BytesIO

# WIB (Western Indonesia Time) = UTC+7
_LOCAL_TZ = timezone(timedelta(hours=7))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils.theme import (
    apply_theme, page_header, section_label,
    GOLD, GOLD_DIM, SURFACE, BORDER, TEXT_PRI, TEXT_SEC, GREEN, RED, AMBER
)
from utils.backup_manager import rotate_backups, get_available_backups, restore_backup

st.set_page_config(page_title="Global Settings — BTN Anchor", page_icon=os.path.join(BASE_DIR, "static", "btn_logo.png"), layout="wide")
apply_theme()

page_header("", "Global Settings", "Upload and manage your Master Reference Files")

# ── Cloud mode detection ───────────────────────────────────────────────────────
cloud_mode = bool(os.getenv("DATABASE_URL"))

if cloud_mode:
    from utils.cloud_db import build_engine
    from utils.master_files_db import (
        ensure_master_files_table,
        save_master_to_db,
        load_master_from_db,
        list_master_files,
        sync_all_masters_to_disk,
    )

    @st.cache_resource
    def _get_engine():
        return build_engine()

    try:
        _engine = _get_engine()
        ensure_master_files_table(_engine)
        _engine_ok = True
    except Exception as _eng_err:
        st.error(f"Could not connect to Neon: {_eng_err}")
        _engine_ok = False
        _engine = None

    st.markdown(
        f'<div style="background:{GREEN}14;border:1px solid {GREEN}40;'
        f'border-left:5px solid {GREEN};border-radius:0 14px 14px 0;padding:12px 16px;'
        f'font-size:0.85rem;color:{GREEN};margin-bottom:22px;">'
        f'<b>Cloud Mode Active</b> — Master files are persisted in <b>Neon (PostgreSQL)</b> and '
        f'survive app restarts. Uploaded files are also cached locally for pipeline compatibility.'
        f'</div>',
        unsafe_allow_html=True,
    )
else:
    _engine = None
    _engine_ok = False
    st.markdown(
        f'<div style="background:{GOLD}14;border:1px solid {GOLD}40;'
        f'border-left:5px solid {GOLD};border-radius:0 14px 14px 0;padding:12px 16px;'
        f'font-size:0.85rem;color:{GOLD};margin-bottom:22px;">'
        f'These master files are saved permanently on the server and used automatically by all '
        f'processing modules. After your first upload, the system auto-updates them — you never need to '
        f're-upload unless the reference data changes.'
        f'</div>',
        unsafe_allow_html=True,
    )

# ── Paths ──────────────────────────────────────────────────────────────────────
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")
os.makedirs(MASTER_DIR, exist_ok=True)

PATH_MID  = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON  = os.path.join(MASTER_DIR, "master_monitoring.xlsx")
BACKUP_DIR = os.path.join(MASTER_DIR, "backup_uploads")
os.makedirs(BACKUP_DIR, exist_ok=True)

# ── On cloud mode: sync all masters from Neon → local disk (once per session) ─
if cloud_mode and _engine_ok and not st.session_state.get("_masters_synced"):
    sync_all_masters_to_disk(_engine, PATH_MID, PATH_CARD, PATH_MON)
    st.session_state["_masters_synced"] = True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _neon_info(file_key: str) -> dict | None:
    if not cloud_mode or not _engine_ok:
        return None
    try:
        return list_master_files(_engine).get(file_key)
    except Exception:
        return None


def is_configured(path: str, file_key: str) -> bool:
    if cloud_mode and _engine_ok and _neon_info(file_key):
        return True
    return os.path.exists(path)


def _file_size_kb(path: str, file_key: str) -> str:
    """Return human-readable file size, preferring Neon metadata in cloud mode."""
    if cloud_mode and _engine_ok:
        info = _neon_info(file_key)
        if info:
            return f"{(info.get('size_bytes') or 0) // 1024:,} KB"
    if os.path.exists(path):
        return f"{os.path.getsize(path) // 1024:,} KB"
    return "—"


def _last_modified(path: str, file_key: str) -> str | None:
    if cloud_mode and _engine_ok:
        info = _neon_info(file_key)
        if info:
            return info.get("updated_at")
    if os.path.exists(path):
        mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=_LOCAL_TZ)
        return mtime.strftime("%d %b %Y, %H:%M WIB")
    return None


def _sync_status_label(path: str, file_key: str) -> str:
    if cloud_mode and _engine_ok:
        if _neon_info(file_key):
            return "Synced to Neon"
        if os.path.exists(path):
            return "Local only"
        return "Not configured"
    if os.path.exists(path):
        return "Configured"
    return "Not configured"


def get_download_bytes(path: str, file_key: str) -> bytes | None:
    if cloud_mode and _engine_ok:
        _, content = load_master_from_db(_engine, file_key)
        if content:
            return content
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    return None


def save_master(uploaded_file, dest_path: str, prefix: str, file_key: str, orig_filename: str) -> bool:
    if uploaded_file is None:
        return False
    content = uploaded_file.getvalue()
    if os.path.exists(dest_path):
        rotate_backups(dest_path, BACKUP_DIR, prefix=prefix, extension=".xlsx")
    with open(dest_path, "wb") as f:
        f.write(content)
    if cloud_mode and _engine_ok:
        ok = save_master_to_db(_engine, file_key, orig_filename, content)
        if not ok:
            st.warning(f"Local save succeeded but Neon upload failed for `{orig_filename}`.")
    return True


# ── MASTER FILE DEFINITIONS ───────────────────────────────────────────────────
MASTERS = [
    dict(
        title="ALL MID Master",     icon="",
        path=PATH_MID,              file_key="master_mid",
        prefix="master_mid",        orig_filename="master_mid.xlsx",
        uploader_label="Upload ALL_MID_UPDATED.xlsx",
        uploader_key="up_mid",      backup_prefix="master_mid",
    ),
    dict(
        title="Card Share Master",  icon="",
        path=PATH_CARD,             file_key="master_card",
        prefix="master_card",       orig_filename="master_card_share.xlsx",
        uploader_label="Upload CARD_SHARE_MERCHANT_ANCHOR.xlsx",
        uploader_key="up_card",     backup_prefix="master_card",
    ),
    dict(
        title="Monitoring Master",  icon="",
        path=PATH_MON,              file_key="master_mon",
        prefix="master_mon",        orig_filename="master_monitoring.xlsx",
        uploader_label="Upload Monitoring Weekly Anchor.xlsx",
        uploader_key="up_mon",      backup_prefix="master_mon",
    ),
]

# ══════════════════════════════════════════════════════════════════════════════
# TAB LAYOUT
# ══════════════════════════════════════════════════════════════════════════════
tab_files, tab_history = st.tabs(["Master Files", "Version History"])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — MASTER FILES
# ─────────────────────────────────────────────────────────────────────────────
with tab_files:

    # ── Status summary row — stat-cards ──────────────────────────────────────
    section_label("Configuration Status")
    cards_html = ""
    for m in MASTERS:
        configured = is_configured(m["path"], m["file_key"])
        size_str   = _file_size_kb(m["path"], m["file_key"])
        mod_str    = _last_modified(m["path"], m["file_key"]) or "—"
        sync_lbl   = _sync_status_label(m["path"], m["file_key"])
        variant    = "green" if configured else "red"
        status_txt = "READY" if configured else "MISSING"
        cards_html += f"""<div class="stat-card {variant}">
            <div class="stat-label">{m['icon']} {m['title']}</div>
            <div class="stat-value" style="font-size:1rem;font-weight:700;">{status_txt}</div>
            <div class="stat-meta">{size_str} · {sync_lbl}</div>
            <div class="stat-meta">{mod_str}</div>
        </div>"""
    st.markdown(
        f'<div class="stats-grid" style="grid-template-columns:repeat(3,1fr);">{cards_html}</div>',
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Download buttons (OUTSIDE form — st.download_button incompatible inside form) ─
    section_label("Download Current Files")
    dl1, dl2, dl3 = st.columns(3)
    for col, m in zip([dl1, dl2, dl3], MASTERS):
        with col:
            dl_bytes = get_download_bytes(m["path"], m["file_key"])
            if dl_bytes:
                st.download_button(
                    f"Download {m['title']}",
                    data=dl_bytes,
                    file_name=m["orig_filename"],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_{m['uploader_key']}",
                    width="stretch",
                )
            else:
                st.button(
                    f"Download {m['title']} (not available)",
                    disabled=True,
                    width="stretch",
                    key=f"dl_disabled_{m['uploader_key']}",
                )

    # ── Success banners (survive the rerun after save) ─────────────────────────
    for m in MASTERS:
        success_key = f"_saved_{m['file_key']}"
        if st.session_state.pop(success_key, False):
            st.success(f"**{m['orig_filename']}** uploaded and saved successfully!")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Upload form (single Save All button for all three files) ───────────────
    section_label("Upload New Versions")
    with st.form("master_upload_form", clear_on_submit=True):
        uc1, uc2, uc3 = st.columns(3)
        uploaded = {}
        for col, m in zip([uc1, uc2, uc3], MASTERS):
            with col:
                configured = is_configured(m["path"], m["file_key"])
                st.markdown(
                    f"**{m['title']}**  \n"
                    f"{'Currently configured' if configured else 'Not yet uploaded'}",
                )
                uploaded[m["file_key"]] = st.file_uploader(
                    m["uploader_label"],
                    type=["xlsx"],
                    key=m["uploader_key"],
                )

        submitted = st.form_submit_button(
            "Save All Changes",
            type="primary",
            width="stretch",
        )

    if submitted:
        any_uploaded = any(f is not None for f in uploaded.values())
        if not any_uploaded:
            st.warning("Please upload at least one file before saving.")
        else:
            with st.spinner("Saving master files…"):
                for m in MASTERS:
                    up_file = uploaded.get(m["file_key"])
                    if up_file is not None:
                        ok = save_master(up_file, m["path"], m["prefix"], m["file_key"], m["orig_filename"])
                        if ok:
                            st.session_state.pop("_masters_synced", None)
                            st.session_state[f"_saved_{m['file_key']}"] = True
            st.rerun()

    # ── Quick Actions ─────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    section_label("Quick Actions")
    qa1, qa2 = st.columns(2)
    with qa1:
        st.page_link(
            "pages/00_Automated_Pipeline.py",
            label="**Go to Automated Pipeline**",
            help="Navigate to the ETL pipeline to run the end-to-end data refresh.",
        )
    with qa2:
        db_exists = os.path.exists(os.path.join(BASE_DIR, "database", "staging.db"))
        has_neon  = cloud_mode and bool(os.getenv("DATABASE_URL"))
        if db_exists or has_neon:
            st.page_link(
                "pages/4_Dashboard.py",
                label="**View Analytics Dashboard**",
                help="Jump straight to the analytics and ML insights dashboard.",
            )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — VERSION HISTORY (consolidated, replaces 3 separate expanders)
# ─────────────────────────────────────────────────────────────────────────────
with tab_history:
    section_label("Version History & Rollback")
    st.markdown(
        "Each master file keeps up to **3 prior versions**. Click **Restore** to roll back to any version.",
        unsafe_allow_html=False,
    )

    # Build a unified backup table across all 3 masters
    all_backups: list[dict] = []
    for m in MASTERS:
        backups = get_available_backups(BACKUP_DIR, prefix=m["backup_prefix"], extension=".xlsx")
        for b in backups:
            all_backups.append({
                "file":        m["title"],
                "icon":        m["icon"],
                "version":     b["version"],
                "timestamp":   b["timestamp"],
                "path":        b["path"],
                "file_key":    m["file_key"],
                "dest_path":   m["path"],
                "orig_filename": m["orig_filename"],
                "restore_key": f"restore_{m['backup_prefix']}_{b['version']}",
            })

    if not all_backups:
        st.info("No backup versions available yet. Upload a master file to start version history.")
    else:
        import pandas as pd
        df_bkp = pd.DataFrame([
            {"File": f"{b['icon']} {b['file']}", "Version": f"v{b['version']}", "Timestamp": b["timestamp"]}
            for b in all_backups
        ])
        st.dataframe(
            df_bkp,
            hide_index=True,
            width="stretch",
        )

        st.markdown("<br>", unsafe_allow_html=True)
        section_label("Restore a Version")
        for b in all_backups:
            rc1, rc2 = st.columns([4, 1])
            rc1.markdown(f"**{b['icon']} {b['file']}** — Version {b['version']}  \n{b['timestamp']}")
            if rc2.button("Restore", key=b["restore_key"], width="stretch"):
                with st.spinner(f"Restoring {b['file']} v{b['version']}…"):
                    restored = restore_backup(b["path"], b["dest_path"])
                if restored:
                    if cloud_mode and _engine_ok:
                        with open(b["dest_path"], "rb") as _f:
                            _rb = _f.read()
                        save_master_to_db(_engine, b["file_key"], b["orig_filename"], _rb)
                    st.session_state.pop("_masters_synced", None)
                    st.session_state[f"_saved_{b['file_key']}"] = True
                    st.rerun()
                else:
                    st.error("Failed to restore backup.")
