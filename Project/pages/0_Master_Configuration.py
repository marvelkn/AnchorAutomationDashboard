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
    apply_theme, page_header, section_label, kpi_card,
    GOLD, GOLD_DIM, SURFACE, BORDER, TEXT_PRI, TEXT_SEC, GREEN, RED, AMBER
)
from utils.backup_manager import rotate_backups, get_available_backups, restore_backup

st.set_page_config(page_title="Global Settings — BTN Anchor", page_icon="⚙️", layout="wide")
apply_theme()

page_header("⚙️", "Global Settings", "Upload and manage your Master Reference Files")

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
        st.error(f"⚠️ Could not connect to Neon: {_eng_err}")
        _engine_ok = False
        _engine = None

    st.markdown(
        """<div style="background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);
        border-radius:10px;padding:12px 16px;font-size:0.85rem;color:#34D399;margin-bottom:22px;">
        ☁️ <b>Cloud Mode Active</b> — Master files are persisted in <b>Neon (PostgreSQL)</b> and
        survive app restarts. Uploaded files are also cached locally for pipeline compatibility.
        </div>""",
        unsafe_allow_html=True,
    )
else:
    _engine = None
    _engine_ok = False
    st.markdown(
        """<div style="background:rgba(240,190,72,.08);border:1px solid rgba(240,190,72,.25);
        border-radius:10px;padding:12px 16px;font-size:0.85rem;color:#c8a033;margin-bottom:22px;">
        📌 These master files are saved permanently on the server and used automatically by all
        Processing modules. After your first upload, the system auto-updates them — you never need to
        re-upload unless the reference data changes.
        </div>""",
        unsafe_allow_html=True,
    )

# ── Paths (always needed for local cache / download) ──────────────────────────
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")
os.makedirs(MASTER_DIR, exist_ok=True)

PATH_MID  = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON  = os.path.join(MASTER_DIR, "master_monitoring.xlsx")
BACKUP_DIR = os.path.join(MASTER_DIR, "backup_uploads")
os.makedirs(BACKUP_DIR, exist_ok=True)

# ── On cloud mode: sync all masters from Neon → local disk (once per session) ─
# Gated by session_state so it only runs on the very first load of each browser
# session — NOT on every button click / file-uploader interaction rerun.
# A manual force-refresh can clear the flag via st.session_state.pop("_masters_synced").
if cloud_mode and _engine_ok and not st.session_state.get("_masters_synced"):
    sync_all_masters_to_disk(_engine, PATH_MID, PATH_CARD, PATH_MON)
    st.session_state["_masters_synced"] = True


# ── Helpers ───────────────────────────────────────────────────────────────────

def save_master(uploaded_file, dest_path: str, prefix: str, file_key: str, orig_filename: str):
    """
    Save an uploaded master file to disk (always) and to Neon (when cloud mode).
    Rotates local backup before overwriting.
    Returns True on success, False if no file provided.
    """
    if uploaded_file is None:
        return False

    content = uploaded_file.getvalue()

    # Rotate existing local file before overwrite
    if os.path.exists(dest_path):
        rotate_backups(dest_path, BACKUP_DIR, prefix=prefix, extension=".xlsx")

    # Write local cache
    with open(dest_path, "wb") as f:
        f.write(content)

    # Write to Neon (cloud mode)
    if cloud_mode and _engine_ok:
        ok = save_master_to_db(_engine, file_key, orig_filename, content)
        if not ok:
            st.warning(f"⚠️ Local save succeeded but Neon upload failed for `{orig_filename}`.")

    return True


def _neon_info(file_key: str) -> dict | None:
    """Return Neon metadata for a file_key, or None if not stored / not cloud mode."""
    if not cloud_mode or not _engine_ok:
        return None
    try:
        info_map = list_master_files(_engine)
        return info_map.get(file_key)
    except Exception:
        return None


def status_badge(path: str, file_key: str) -> str:
    """
    In cloud mode: show Neon status (falls back to local if Neon has it).
    In local mode: show local file status.
    """
    if cloud_mode and _engine_ok:
        info = _neon_info(file_key)
        if info:
            sz_kb = (info["size_bytes"] or 0) // 1024
            return (
                f'<span class="status-badge ok">☁️ Synced to Neon · {sz_kb} KB</span>'
            )
        # Not in Neon yet; check local disk
        if os.path.exists(path):
            sz = os.path.getsize(path) // 1024
            return f'<span class="status-badge ok">💾 Local only · {sz} KB</span>'
        return '<span class="status-badge err">❌ Not Configured</span>'

    # Local mode
    if os.path.exists(path):
        sz = os.path.getsize(path) // 1024
        return f'<span class="status-badge ok">✅ Configured · {sz} KB</span>'
    return '<span class="status-badge err">❌ Not Configured</span>'


def last_modified_line(path: str, file_key: str) -> str | None:
    """Return last-modified timestamp — prefer Neon updated_at in cloud mode."""
    if cloud_mode and _engine_ok:
        info = _neon_info(file_key)
        if info:
            return info.get("updated_at")
    if os.path.exists(path):
        mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=_LOCAL_TZ)
        return mtime.strftime("%d %b %Y, %H:%M")
    return None


def get_download_bytes(path: str, file_key: str) -> bytes | None:
    """
    Return file bytes for the download button.
    Prefer Neon content in cloud mode; fall back to local file.
    """
    if cloud_mode and _engine_ok:
        _, content = load_master_from_db(_engine, file_key)
        if content:
            return content
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    return None


def is_configured(path: str, file_key: str) -> bool:
    """True if the file is available (Neon or disk)."""
    if cloud_mode and _engine_ok:
        info = _neon_info(file_key)
        if info:
            return True
    return os.path.exists(path)


# ─── File card renderer ───────────────────────────────────────────────────────

def render_master_card(
    col,
    title: str,
    icon: str,
    path: str,
    file_key: str,
    prefix: str,
    orig_filename: str,
    uploader_label: str,
    uploader_key: str,
    btn_key: str,
    btn_label: str,
    backup_prefix: str,
):
    with col:
        mod = last_modified_line(path, file_key)
        mod_tag = (
            f'<div style="font-size:0.72rem;color:{TEXT_SEC};margin-top:6px;">🕐 Updated: {mod}</div>'
            if mod else ""
        )
        st.markdown(
            f"""<div class="config-card">
                <h3>{icon} {title}</h3>
                {status_badge(path, file_key)}
                {mod_tag}
            </div>""",
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        # Download button
        dl_bytes = get_download_bytes(path, file_key)
        if dl_bytes:
            st.download_button(
                "⬇️ Download Current",
                dl_bytes,
                file_name=orig_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{uploader_key}",
                width="stretch",
            )

        # ── Success banner (survives the rerun after save) ──────────────────
        success_key = f"_saved_{file_key}"
        if st.session_state.pop(success_key, False):
            st.success(
                f"✅ **{orig_filename}** uploaded and saved successfully!",
                icon="✅",
            )

        up_file = st.file_uploader(uploader_label, type=["xlsx"], key=uploader_key)
        if st.button(btn_label, key=btn_key, type="primary", width="stretch"):
            if up_file:
                with st.spinner(f"Saving {orig_filename}…"):
                    ok = save_master(up_file, path, prefix, file_key, orig_filename)
                if ok:
                    # Invalidate local sync cache so next full load re-syncs
                    st.session_state.pop("_masters_synced", None)
                    # Set flag BEFORE rerun so success banner renders on next cycle
                    st.session_state[success_key] = True
                    st.rerun()
            else:
                st.warning("Please upload a file first.")

        # Rollback section
        with st.expander("🕒 Version History & Rollback", expanded=False):
            backups = get_available_backups(BACKUP_DIR, prefix=backup_prefix, extension=".xlsx")
            if not backups:
                st.caption("No versions available for rollback.")
            else:
                for b in backups:
                    c1, c2 = st.columns([3, 1])
                    c1.write(f"**Version {b['version']}** ({b['timestamp']})")
                    if c2.button("Restore", key=f"restore_{backup_prefix}_{b['version']}"):
                        with st.spinner("Restoring…"):
                            restored = restore_backup(b["path"], path)
                        if restored:
                            # If cloud mode, also push restored file to Neon
                            if cloud_mode and _engine_ok:
                                with open(path, "rb") as _f:
                                    _rb = _f.read()
                                save_master_to_db(_engine, file_key, orig_filename, _rb)
                            st.session_state.pop("_masters_synced", None)
                            st.session_state[f"_saved_{file_key}"] = True
                            st.rerun()
                        else:
                            st.error("Failed to restore backup.")


# ─── Render the three cards ───────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

render_master_card(
    col=col1,
    title="ALL MID Master",
    icon="🧹",
    path=PATH_MID,
    file_key="master_mid",
    prefix="master_mid",
    orig_filename="master_mid.xlsx",
    uploader_label="Upload ALL_MID_UPDATED.xlsx",
    uploader_key="up_mid",
    btn_key="btn_mid",
    btn_label="💾 Save MID Master",
    backup_prefix="master_mid",
)

render_master_card(
    col=col2,
    title="Card Share Master",
    icon="💳",
    path=PATH_CARD,
    file_key="master_card",
    prefix="master_card",
    orig_filename="master_card_share.xlsx",
    uploader_label="Upload CARD_SHARE_MERCHANT_ANCHOR.xlsx",
    uploader_key="up_card",
    btn_key="btn_card",
    btn_label="💾 Save Card Share Master",
    backup_prefix="master_card",
)

render_master_card(
    col=col3,
    title="Monitoring Master",
    icon="📅",
    path=PATH_MON,
    file_key="master_mon",
    prefix="master_mon",
    orig_filename="master_monitoring.xlsx",
    uploader_label="Upload Monitoring Weekly Anchor.xlsx",
    uploader_key="up_mon",
    btn_key="btn_mon",
    btn_label="💾 Save Monitoring Master",
    backup_prefix="master_mon",
)

# ─── Summary strip ────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
section_label("Configuration Status Summary")
s1, s2, s3 = st.columns(3)
s1.metric("MID Master",        "✅ Ready" if is_configured(PATH_MID,  "master_mid")  else "❌ Missing")
s2.metric("Card Share Master", "✅ Ready" if is_configured(PATH_CARD, "master_card") else "❌ Missing")
s3.metric("Monitoring Master", "✅ Ready" if is_configured(PATH_MON,  "master_mon")  else "❌ Missing")

# ─── Quick Actions ────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
section_label("Quick Actions")

qa1, qa2 = st.columns(2)
with qa1:
    st.page_link(
        "pages/00_Automated_Pipeline.py",
        label="**🚀 Go to Automated Pipeline**",
        icon="🚀",
        help="Navigate to the ETL pipeline to run the end-to-end data refresh.",
    )
with qa2:
    db_exists = os.path.exists(os.path.join(BASE_DIR, "database", "staging.db"))
    has_neon  = cloud_mode and bool(os.getenv("DATABASE_URL"))
    if db_exists or has_neon:
        st.page_link(
            "pages/4_Dashboard.py",
            label="**📊 View Analytics Dashboard**",
            icon="📈",
            help="Jump straight to the analytics and ML insights dashboard.",
        )
