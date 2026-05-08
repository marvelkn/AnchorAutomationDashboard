import streamlit as st
import os
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils.theme import apply_theme, theme_toggle_sidebar, get_palette, _nav_css

st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon=os.path.join(BASE_DIR, "static", "btn_logo.png"),
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session State Defaults — single initialization point ─────────────────────
_DEFAULTS = {
    "theme_mode":       "light",
    "editor_key":       0,
    "_masters_synced":  False,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

apply_theme()

# ── Paths ──────────────────────────────────────────────────────────────────────
LOGO_PATH = os.path.join(BASE_DIR, "static", "btn_logo.png")
DB_PATH   = os.path.join(BASE_DIR, "database", "staging.db")
db_exists = os.path.exists(DB_PATH)
neon_exists = os.getenv("DATABASE_URL") is not None
data_exists = db_exists or neon_exists
p         = get_palette()

# ── DB status helpers ──────────────────────────────────────────────────────────
if neon_exists:
    _db_clr, _db_lbl = p["BLUE_ACC"], "Neon Connected"
    _db_sub = "Cloud Database Active"
elif db_exists:
    _mtime   = datetime.fromtimestamp(os.path.getmtime(DB_PATH))
    _age_h   = (datetime.now().timestamp() - _mtime.timestamp()) / 3600
    _size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    if _age_h < 24:
        _db_clr, _db_lbl = p["GREEN"], "Fresh"
    elif _age_h < 72:
        _db_clr, _db_lbl = p["AMBER"], "Aging"
    else:
        _db_clr, _db_lbl = p["RED"],   "Stale"
    _db_sub = f"{_size_mb:.0f} MB · {_mtime.strftime('%d %b, %H:%M')}"
else:
    _db_clr, _db_lbl, _db_sub = p["RED"], "Not Found", "Upload data to Neon or Staging"

_db_dot = (f'<span style="display:inline-block;width:var(--size-dot,8px);height:var(--size-dot,8px);border-radius:50%;'
           f'background:{_db_clr};margin-right:5px;vertical-align:middle;"></span>')

# ── Navigation registry ────────────────────────────────────────────────────────
# MUST run before st.page_link() — page_link looks up URL metadata from the
# registry that st.navigation() populates. pg.run() is called at the very end.
try:
    if not data_exists:
        pg = st.navigation({
            "REQUIRED ACTION": [
                st.Page("pages/00_Automated_Pipeline.py", title="Get Started / Connect Cloud", icon=":material/rocket_launch:", default=True),
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
                st.Page("pages/05_PM_Manager.py", title="PM Manager", icon=":material/group:"),
            ],
            "SETTINGS": [
                st.Page("pages/0_Master_Configuration.py", title="Global Settings", icon=":material/settings:"),
            ],
        })
except AttributeError:
    pg = None

# ── Sidebar CSS — injected from utils/theme._nav_css() (single source of truth) ─
st.markdown(_nav_css(p), unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:

    # ══ SECTION A: BRAND HEADER ════════════════════════════════════════════════
    import base64
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        brand_icon_html = (
            f'<a href="/" style="display:block;text-decoration:none;" title="Go to Dashboard">'
            f'<img src="data:image/png;base64,{img_b64}" style="display:block;width:clamp(40px,7vw,58px);height:auto;">'
            f'</a>'
        )
    else:
        brand_icon_html = '<div class="logo-mark">BTN // ANCHOR</div>'

    st.markdown(
        f"""<div class="sidebar-brand-header">
            <div style="display:flex;gap:10px;align-items:center;">
                <div style="flex-shrink:0;">{brand_icon_html}</div>
                <div>
                    <div class="logo-mark">Anchor</div>
                    <div class="logo-sub">Automation Analytics</div>
                </div>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ══ SECTION B: CONTROLS (theme toggle) ════════════════════════════════════
    st.markdown('<div class="sb-controls">', unsafe_allow_html=True)
    theme_toggle_sidebar()
    st.markdown('</div>', unsafe_allow_html=True)  # close .sb-controls

    # ══ SECTION C: NAVIGATION (manually rebuilt, at the bottom) ════════════════
    # stSidebarNav is hidden via CSS; st.navigation() below still handles routing.
    st.markdown('<div class="custom-nav">', unsafe_allow_html=True)

    if data_exists:
        st.markdown('<div class="custom-nav-group">Analytics</div>', unsafe_allow_html=True)
        st.page_link("pages/4_Dashboard.py",           label="Dashboard",            icon=":material/bar_chart:")

        st.markdown('<div class="custom-nav-group">Pipeline Orchestration</div>', unsafe_allow_html=True)
        st.page_link("pages/00_Automated_Pipeline.py", label="Automated Pipeline",   icon=":material/rocket_launch:")

        st.markdown('<div class="custom-nav-group">Data Management</div>', unsafe_allow_html=True)
        st.page_link("pages/01_Data_Editor.py",        label="Master Records Editor",icon=":material/edit_document:")
        st.page_link("pages/05_PM_Manager.py",         label="PM Manager",           icon=":material/group:")

        st.markdown('<div class="custom-nav-group">Settings</div>', unsafe_allow_html=True)
        st.page_link("pages/0_Master_Configuration.py",label="Global Settings",      icon=":material/settings:")
    else:
        st.markdown('<div class="custom-nav-group">Required Action</div>', unsafe_allow_html=True)
        st.page_link("pages/00_Automated_Pipeline.py", label="Upload Database First", icon=":material/warning:")

        st.markdown('<div class="custom-nav-group">Settings</div>', unsafe_allow_html=True)
        st.page_link("pages/0_Master_Configuration.py",label="Global Settings",       icon=":material/settings:")

    st.markdown('</div>', unsafe_allow_html=True)  # close .custom-nav

    # ══ SECTION D: STATUS STRIP (pinned to bottom) ═════════════════════════════
    st.markdown('<div class="sb-status-strip">', unsafe_allow_html=True)

    # ── Card 1: Neon Cloud DB connection ──────────────────────────────────────
    if neon_exists:
        _neon_clr, _neon_dot = p["BLUE_ACC"], "☁️"
        _neon_lbl, _neon_sub = "Connected", "Cloud DB Active"
    else:
        _neon_clr, _neon_dot = p["RED"], "🔴"
        _neon_lbl, _neon_sub = "Not Connected", "Set DATABASE_URL env var"

    st.markdown(
        f"""<div class="db-info" style="border-left-color:{_neon_clr}; margin-bottom:8px;">
          <div class="db-label">Neon Database</div>
          <div class="db-status" style="color:{_neon_clr};">{_neon_dot} {_neon_lbl}</div>
          <div class="db-meta">{_neon_sub}</div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Card 2: Staging DB / local data source (only when Neon is not active) ──
    if not neon_exists:
        st.markdown(
            f"""<div class="db-info" style="border-left-color:{_db_clr};">
              <div class="db-label">Database Status</div>
              <div class="db-status" style="color:{_db_clr};">{_db_dot} {_db_lbl}</div>
              <div class="db-meta">{_db_sub}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)  # close .sb-status-strip


# ── Run the registered page ────────────────────────────────────────────────────
if pg is not None:
    pg.run()
else:
    st.error("Please update Streamlit to >= 1.36 to use native navigation.")


