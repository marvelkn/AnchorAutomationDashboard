import streamlit as st
import os
import shutil

st.set_page_config(page_title="Master Configuration", page_icon="⚙️", layout="wide")

st.title("⚙️ Master Configuration")
st.markdown("""
Upload your **Master Reference Files** here. 
These files will be **saved permanently** on the server and used automatically by the Processing modules. 
Whenever you process new data in the other pages, these master files will be **automatically updated and saved behind the scenes**, so you never have to re-upload them again!
""")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")
os.makedirs(MASTER_DIR, exist_ok=True)

PATH_MID = os.path.join(MASTER_DIR, "master_mid.xlsx")
PATH_CARD = os.path.join(MASTER_DIR, "master_card_share.xlsx")
PATH_MON = os.path.join(MASTER_DIR, "master_monitoring.xlsx")

def save_master(uploaded_file, dest_path):
    if uploaded_file is not None:
        with open(dest_path, "wb") as f:
            f.write(uploaded_file.getvalue())
        return True
    return False

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("🧹 ALL MID Master")
    if os.path.exists(PATH_MID):
        st.success("✅ Master MID Configured")
        with open(PATH_MID, "rb") as f:
            st.download_button("⬇️ Download Current", f, file_name="master_mid.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_mid")
    else:
        st.error("❌ Not Configured")
        
    up_mid = st.file_uploader("Upload ALL_MID_UPDATED.xlsx", type=["xlsx"], key="up_mid")
    if st.button("Save MID Master", key="btn_mid") and up_mid:
        if save_master(up_mid, PATH_MID):
            st.success("Saved!")
            st.rerun()

with col2:
    st.subheader("💳 Card Share Master")
    if os.path.exists(PATH_CARD):
        st.success("✅ Master Card Share Configured")
        with open(PATH_CARD, "rb") as f:
            st.download_button("⬇️ Download Current", f, file_name="master_card_share.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_card")
    else:
        st.error("❌ Not Configured")
        
    up_card = st.file_uploader("Upload CARD_SHARE_MERCHANT_ANCHOR.xlsx", type=["xlsx"], key="up_card")
    if st.button("Save Card Share Master", key="btn_card") and up_card:
        if save_master(up_card, PATH_CARD):
            st.success("Saved!")
            st.rerun()

with col3:
    st.subheader("📅 Monitoring Master")
    if os.path.exists(PATH_MON):
        st.success("✅ Master Monitoring Configured")
        with open(PATH_MON, "rb") as f:
            st.download_button("⬇️ Download Current", f, file_name="master_monitoring.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_mon")
    else:
        st.error("❌ Not Configured")
        
    up_mon = st.file_uploader("Upload Monitoring Weekly Anchor.xlsx", type=["xlsx"], key="up_mon")
    if st.button("Save Monitoring Master", key="btn_mon") and up_mon:
        if save_master(up_mon, PATH_MON):
            st.success("Saved!")
            st.rerun()
