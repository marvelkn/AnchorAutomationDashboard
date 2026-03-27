import streamlit as st
import os
import sys
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
from utils.theme import (
    apply_theme, page_header, section_label, kpi_card,
    GOLD, GOLD_DIM, SURFACE, BORDER, TEXT_PRI, TEXT_SEC, GREEN, RED, AMBER
)

st.set_page_config(page_title="Global Settings — BTN Anchor", page_icon="⚙️", layout="wide")
apply_theme()

page_header("⚙️", "Global Settings", "Upload and manage your Master Reference Files")

st.markdown(
    """<div style="background:rgba(240,190,72,.08);border:1px solid rgba(240,190,72,.25);
    border-radius:10px;padding:12px 16px;font-size:0.85rem;color:#c8a033;margin-bottom:22px;">
    📌 These master files are saved permanently on the server and used automatically by all
    Processing modules. After your first upload, the system auto-updates them — you never need to
    re-upload unless the reference data changes.
    </div>""",
    unsafe_allow_html=True,
)

MASTER_DIR = os.path.join(BASE_DIR, "data", "master")
os.makedirs(MASTER_DIR, exist_ok=True)

PATH_MID  = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON  = os.path.join(MASTER_DIR, "master_monitoring.xlsx")

def save_master(uploaded_file, dest_path):
    if uploaded_file is not None:
        with open(dest_path, "wb") as f:
            f.write(uploaded_file.getvalue())
        return True
    return False

def status_badge(path):
    if os.path.exists(path):
        sz  = os.path.getsize(path) // 1024
        return f'<span class="status-badge ok">✅ Configured · {sz} KB</span>'
    return '<span class="status-badge err">❌ Not Configured</span>'

col1, col2, col3 = st.columns(3)

# ─── MID ──────────────────────────────────────────────────────────────────────
with col1:
    st.markdown(
        f"""<div class="config-card">
            <h3>🧹 ALL MID Master</h3>
            {status_badge(PATH_MID)}
        </div>""",
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if os.path.exists(PATH_MID):
        with open(PATH_MID, "rb") as f:
            st.download_button(
                "⬇️ Download Current",
                f,
                file_name="master_mid.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_mid",
                use_container_width=True,
            )
    up_mid = st.file_uploader("Upload ALL_MID_UPDATED.xlsx", type=["xlsx"], key="up_mid")
    if st.button("💾 Save MID Master", key="btn_mid", type="primary", use_container_width=True):
        if up_mid and save_master(up_mid, PATH_MID):
            st.success("✅ Saved!")
            st.rerun()
        elif not up_mid:
            st.warning("Please upload a file first.")

# ─── Card Share ───────────────────────────────────────────────────────────────
with col2:
    st.markdown(
        f"""<div class="config-card">
            <h3>💳 Card Share Master</h3>
            {status_badge(PATH_CARD)}
        </div>""",
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if os.path.exists(PATH_CARD):
        with open(PATH_CARD, "rb") as f:
            st.download_button(
                "⬇️ Download Current",
                f,
                file_name="master_card_share.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_card",
                use_container_width=True,
            )
    up_card = st.file_uploader("Upload CARD_SHARE_MERCHANT_ANCHOR.xlsx", type=["xlsx"], key="up_card")
    if st.button("💾 Save Card Share Master", key="btn_card", type="primary", use_container_width=True):
        if up_card and save_master(up_card, PATH_CARD):
            st.success("✅ Saved!")
            st.rerun()
        elif not up_card:
            st.warning("Please upload a file first.")

# ─── Monitoring ───────────────────────────────────────────────────────────────
with col3:
    st.markdown(
        f"""<div class="config-card">
            <h3>📅 Monitoring Master</h3>
            {status_badge(PATH_MON)}
        </div>""",
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if os.path.exists(PATH_MON):
        with open(PATH_MON, "rb") as f:
            st.download_button(
                "⬇️ Download Current",
                f,
                file_name="master_monitoring.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_mon",
                use_container_width=True,
            )
    up_mon = st.file_uploader("Upload Monitoring Weekly Anchor.xlsx", type=["xlsx"], key="up_mon")
    if st.button("💾 Save Monitoring Master", key="btn_mon", type="primary", use_container_width=True):
        if up_mon and save_master(up_mon, PATH_MON):
            st.success("✅ Saved!")
            st.rerun()
        elif not up_mon:
            st.warning("Please upload a file first.")

# ─── Summary strip ────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
section_label("Configuration Status Summary")
s1, s2, s3 = st.columns(3)
s1.metric("MID Master",        "✅ Ready" if os.path.exists(PATH_MID)  else "❌ Missing")
s2.metric("Card Share Master", "✅ Ready" if os.path.exists(PATH_CARD) else "❌ Missing")
s3.metric("Monitoring Master", "✅ Ready" if os.path.exists(PATH_MON)  else "❌ Missing")
