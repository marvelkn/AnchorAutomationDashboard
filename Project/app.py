import streamlit as st
import os
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils.theme import apply_theme, theme_toggle_sidebar, get_palette

st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()

# ── Paths ──────────────────────────────────────────────────────────────────────
LOGO_PATH = os.path.join(BASE_DIR, "static", "btn_logo.png")
DB_PATH   = os.path.join(BASE_DIR, "database", "staging.db")
db_exists = os.path.exists(DB_PATH)
p         = get_palette()

# ── DB status helpers ──────────────────────────────────────────────────────────
if db_exists:
    _mtime   = datetime.fromtimestamp(os.path.getmtime(DB_PATH))
    _age_h   = (datetime.now().timestamp() - _mtime.timestamp()) / 3600
    _size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    if _age_h < 24:
        _db_clr, _db_dot, _db_lbl = p["GREEN"], "🟢", "Fresh"
    elif _age_h < 72:
        _db_clr, _db_dot, _db_lbl = p["AMBER"], "🟡", "Aging"
    else:
        _db_clr, _db_dot, _db_lbl = p["RED"],   "🔴", "Stale"
    _db_sub = f"{_size_mb:.0f} MB · {_mtime.strftime('%d %b, %H:%M')}"
else:
    _db_clr, _db_dot, _db_lbl, _db_sub = p["RED"], "🔴", "Not Found", "Upload staging.db"

# ── Navigation registry ────────────────────────────────────────────────────────
# MUST run before st.page_link() — page_link looks up URL metadata from the
# registry that st.navigation() populates. pg.run() is called at the very end.
try:
    if not db_exists:
        pg = st.navigation({
            "REQUIRED ACTION": [
                st.Page("pages/00_Automated_Pipeline.py", title="Upload Database First", icon=":material/warning:", default=True),
            ],
            "SETTINGS": [
                st.Page("pages/0_Master_Configuration.py", title="Global Settings", icon=":material/settings:"),
            ],
        })
    else:
        pg = st.navigation({
            "ANALYTICS": [
                st.Page("pages/4_Dashboard.py", title="Dashboard", icon=":material/bar_chart:", default=True),
            ],
            "PIPELINE ORCHESTRATION": [
                st.Page("pages/00_Automated_Pipeline.py", title="Automated Pipeline", icon=":material/rocket_launch:"),
            ],
            "DATA MANAGEMENT": [
                st.Page("pages/01_Data_Editor.py", title="Master Records Editor", icon=":material/edit_document:"),
            ],
            "SETTINGS": [
                st.Page("pages/0_Master_Configuration.py", title="Global Settings", icon=":material/settings:"),
            ],
        })
except AttributeError:
    pg = None

# ── Sidebar CSS ────────────────────────────────────────────────────────────────
# Strategy:
#   • Hide the auto-generated stSidebarNav entirely (routing still works via
#     st.navigation() — the visible widget and the router are independent).
#   • Rebuild nav links manually with st.page_link() at the BOTTOM of the
#     with st.sidebar: block so Python source order controls visual order:
#       1. Brand header  (top)
#       2. Controls      (env / db / theme)
#       3. Nav links     (bottom)
st.markdown(f"""
<style>
/* ① Brand Header — sticky, expands to full sidebar width */
.sidebar-brand-header {{
    position: sticky !important;
    top: 0 !important;
    z-index: 100 !important;
    background: #172B4D !important;
    padding: 1.6rem 1.25rem 1rem 1.25rem !important;
    border-bottom: 1px solid {p['BORDER']} !important;
    box-sizing: border-box !important;
    margin: -1rem -1rem 0 -1rem !important;
    width: calc(100% + 2rem) !important;
}}

/* ② Controls strip */
.sb-controls {{
    padding: 0.75rem 0 0.5rem 0 !important;
    border-bottom: 1px solid {p['BORDER']} !important;
    margin-bottom: 0.25rem !important;
}}
.sb-controls .stSelectbox label {{
    font-size: 0.65rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    color: {p['TEXT_SEC']} !important;
    font-weight: 700 !important;
}}
.sb-controls .stSelectbox > div > div {{
    font-size: 0.82rem !important;
    padding: 5px 10px !important;
    min-height: 34px !important;
}}
.sb-controls .stToggle label {{
    font-size: 0.8rem !important;
    color: {p['TEXT_SEC']} !important;
}}

/* ③ HIDE the auto-generated nav widget — routing still works via st.navigation() */
[data-testid="stSidebarNav"] {{
    display: none !important;
}}

/* ④ Remove Streamlit's default top-padding on user content */
section[data-testid="stSidebarUserContent"] {{
    padding-top: 0 !important;
}}

/* ⑤ Custom nav section (built with st.page_link) */
.custom-nav {{
    padding: 0.5rem 0 1rem 0;
    border-top: 1px solid {p['BORDER']};
    margin-top: 0.5rem;
}}
.custom-nav-group {{
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-weight: 700;
    color: {p['TEXT_SEC']};
    opacity: 0.75;
    margin: 0.9rem 0.8rem 0.3rem 0.8rem;
}}

/* Style st.page_link() to match native nav link appearance */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] {{
    border-radius: 8px !important;
    margin: 0.1rem 0.8rem !important;
    padding: 0 !important;
    transition: background 0.15s !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"]:hover {{
    background: rgba(240,190,72,0.10) !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a {{
    color: #E8EDF5 !important;
    text-decoration: none !important;
    font-size: 0.88rem !important;
    padding: 0.45rem 0.8rem !important;
    display: flex !important;
    align-items: center !important;
    gap: 0.5rem !important;
    border-radius: 8px !important;
    width: 100% !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a:hover {{
    background: rgba(240,190,72,0.10) !important;
}}
/* Active page highlight */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a[aria-current="page"] {{
    background: rgba(43,68,112,0.60) !important;
    font-weight: 700 !important;
    border-left: 3px solid {p['GOLD']} !important;
    color: {p['GOLD']} !important;
}}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:

    # ══ SECTION A: BRAND HEADER ════════════════════════════════════════════════
    import base64
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        brand_icon_html = f'<img src="data:image/png;base64,{img_b64}" width="130" style="margin-bottom:6px; display:block;">'
    else:
        brand_icon_html = '<div style="font-size:1.15rem;font-weight:900;color:#E8EDF5;letter-spacing:-0.02em;margin-bottom:4px;">🏦 BTN Anchor</div>'

    st.markdown(
        f"""<div class="sidebar-brand-header">
            {brand_icon_html}
            <div style="font-size:0.67rem;color:{p['TEXT_SEC']};text-transform:uppercase;letter-spacing:0.09em;">
                Merchant Intelligence Platform
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ══ SECTION B: CONTROLS (env + db status + theme) ══════════════════════════
    st.markdown('<div class="sb-controls">', unsafe_allow_html=True)

    env_mode = st.selectbox(
        "Environment",
        options=["PRODUCTION", "STAGING"],
        index=0 if st.session_state.get("env_mode", "PRODUCTION") == "PRODUCTION" else 1,
        key="env_selector",
        help="Switch between Production and Staging environments.",
    )
    st.session_state["env_mode"] = env_mode

    st.markdown(
        f"""<div style="background:{p['SURFACE']};border:1px solid {p['BORDER']};
                border-left:3px solid {_db_clr};border-radius:7px;
                padding:6px 10px;margin:4px 0 6px 0;
                display:flex;align-items:center;gap:8px;">
          <span style="font-size:0.85rem;flex-shrink:0;">{_db_dot}</span>
          <div>
            <div style="font-size:0.73rem;font-weight:700;color:{_db_clr};line-height:1.2;">{_db_lbl}</div>
            <div style="font-size:0.65rem;color:{p['TEXT_SEC']};margin-top:1px;">{_db_sub}</div>
          </div>
        </div>""",
        unsafe_allow_html=True,
    )

    theme_toggle_sidebar()
    st.markdown('</div>', unsafe_allow_html=True)  # close .sb-controls

    # ══ SECTION C: NAVIGATION (manually rebuilt, at the bottom) ════════════════
    # stSidebarNav is hidden via CSS; st.navigation() below still handles routing.
    # st.page_link() here renders inside stSidebarUserContent in Python order.
    st.markdown('<div class="custom-nav">', unsafe_allow_html=True)

    if db_exists:
        st.markdown('<div class="custom-nav-group">Analytics</div>', unsafe_allow_html=True)
        st.page_link("pages/4_Dashboard.py",           label="Dashboard",            icon=":material/bar_chart:")

        st.markdown('<div class="custom-nav-group">Pipeline Orchestration</div>', unsafe_allow_html=True)
        st.page_link("pages/00_Automated_Pipeline.py", label="Automated Pipeline",   icon=":material/rocket_launch:")

        st.markdown('<div class="custom-nav-group">Data Management</div>', unsafe_allow_html=True)
        st.page_link("pages/01_Data_Editor.py",        label="Master Records Editor",icon=":material/edit_document:")

        st.markdown('<div class="custom-nav-group">Settings</div>', unsafe_allow_html=True)
        st.page_link("pages/0_Master_Configuration.py",label="Global Settings",      icon=":material/settings:")
    else:
        st.markdown('<div class="custom-nav-group">Required Action</div>', unsafe_allow_html=True)
        st.page_link("pages/00_Automated_Pipeline.py", label="Upload Database First", icon=":material/warning:")

        st.markdown('<div class="custom-nav-group">Settings</div>', unsafe_allow_html=True)
        st.page_link("pages/0_Master_Configuration.py",label="Global Settings",       icon=":material/settings:")

    st.markdown('</div>', unsafe_allow_html=True)  # close .custom-nav


# ── Run the registered page ────────────────────────────────────────────────────
if pg is not None:
    pg.run()
else:
    st.error("Please update Streamlit to >= 1.36 to use native navigation.")


