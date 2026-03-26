import streamlit as st
import sqlite3
import pandas as pd
import os

# Set page config for the entire app
st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (BTN Colors)
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background-color: #1F3864;
    }
    [data-testid="stSidebar"] * {
        color: white;
    }
    .main-header {
        color: #1F3864;
        font-family: 'Inter', sans-serif;
    }
    .kpi-card {
        background-color: #F8F9FA;
        border-left: 5px solid #F2C94C;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    hr {
        border-bottom: 2px solid #2F80ED;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# NAVIGATION SETUP
# -------------------------------------------------------------
# If using Streamlit >= 1.36.0, we use st.navigation
# Older versions will just auto-load from the `pages/` directory.
# But we can also manually implement a navigation via sidebar if needed.
# Since Streamlit 2024 is available, st.navigation is preferred.

try:
    pg = st.navigation({
        "⚙️ CONFIGURATION": [
            st.Page("pages/0_Master_Configuration.py", title="Global Settings", icon="⚙️")
        ],
        "📁 PROCESSING": [
            st.Page("pages/1_MID_Cleaner.py", title="Page 1: ALL MID Cleaner", icon="🧹"),
            st.Page("pages/2_Card_Share_Processor.py", title="Page 2: Card Share", icon="💳"),
            st.Page("pages/3_Monitoring_Processor.py", title="Page 3: Monitoring Weekly", icon="📅")
        ],
        "📊 VISUALIZATIONS": [
            st.Page("pages/4_Dashboard.py", title="Page 4: Dashboard", icon="📈")
        ]
    })
    pg.run()
except AttributeError:
    # Fallback for Streamlit < 1.36
    st.error("Please update Streamlit to >= 1.36 to use native navigation, or ensure the app is run from the root directory so `pages/` auto-loader kicks in.")
    st.markdown("Navigate using the sidebar if files are configured correctly.")
