import streamlit as st
import sqlite3
import pandas as pd
import os
import sys

# ── Theme ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
from utils.theme import apply_theme, theme_toggle_sidebar, get_palette

# Set page config for the entire app
st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()

# ── Sidebar: Theme toggle + divider ──────────────────────────────────────────
theme_toggle_sidebar()
st.sidebar.markdown("<hr style='border-color:#2B4470;margin:6px 0;'>", unsafe_allow_html=True)


# ── Navigation  ──────────────────────────────────────────────────────────────
# Dashboard is the FIRST page so users see visuals immediately.
# Processing and Configuration come after.
try:
    pg = st.navigation({
        "📊 ANALYTICS": [
            st.Page("pages/4_Dashboard.py", title="Dashboard", icon="📈", default=True),
        ],
        "📁 PROCESSING": [
            st.Page("pages/1_MID_Cleaner.py",          title="ALL MID Cleaner",      icon="🧹"),
            st.Page("pages/2_Card_Share_Processor.py", title="Card Share Processor", icon="💳"),
            st.Page("pages/3_Monitoring_Processor.py", title="Monitoring Weekly",    icon="📅"),
        ],
        "⚙️ SETTINGS": [
            st.Page("pages/0_Master_Configuration.py", title="Master Configuration", icon="⚙️"),
        ],
    })
    pg.run()
except AttributeError:
    st.error("Please update Streamlit to >= 1.36 to use native navigation.")
