import streamlit as st
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils.theme import apply_theme, theme_toggle_sidebar, get_palette, _nav_css


def _render_mobile_nav(neon_connected: bool):
    """Render the fixed bottom navigation bar (mobile only).

    Visible only at ≤768px — gated entirely by the `.st-key-mobile_nav` CSS in
    utils/theme._make_css(). Mirrors the sidebar page registry but uses short,
    phone-friendly labels. Must be rendered last in the DOM (after pg.run) so
    the CSS can lift it out of flow with position:fixed. The sidebar nav stays
    available as the secondary (drawer) surface.
    """
    if neon_connected:
        items = [
            ("pages/4_Dashboard.py",            "Dashboard", ":material/bar_chart:"),
            ("pages/00_Automated_Pipeline.py",  "Pipeline",  ":material/rocket_launch:"),
            ("pages/01_Data_Editor.py",         "Records",   ":material/edit_document:"),
            ("pages/05_PM_Manager.py",          "PM",        ":material/group:"),
            ("pages/0_Master_Configuration.py", "Settings",  ":material/settings:"),
        ]
    else:
        items = [
            ("pages/00_Automated_Pipeline.py",  "Connect",  ":material/warning:"),
            ("pages/0_Master_Configuration.py", "Settings", ":material/settings:"),
        ]
    with st.container(key="mobile_nav"):
        cols = st.columns(len(items))
        for col, (path, label, icon) in zip(cols, items):
            col.page_link(path, label=label, icon=icon)

st.set_page_config(
    page_title="BTN Anchor Dashboard",
    page_icon=os.path.join(BASE_DIR, "static", "btn_logo.png"),
    layout="wide",
    # "auto" → sidebar expanded on desktop, collapsed on phones/tablets so the
    # content (and the new fixed bottom nav) is immediately visible on mobile.
    initial_sidebar_state="auto",
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
# Neon (cloud) is the only data backend the app supports — the local SQLite
# fallback was removed (see plan act-as-a-senior-glistening-lovelace.md).
neon_exists = os.getenv("DATABASE_URL") is not None
data_exists = neon_exists
p         = get_palette()

# ── Navigation registry ────────────────────────────────────────────────────────
# MUST run before st.page_link() — page_link looks up URL metadata from the
# registry that st.navigation() populates. pg.run() is called at the very end.
try:
    if not data_exists:
        pg = st.navigation({
            "REQUIRED ACTION": [
                st.Page("pages/00_Automated_Pipeline.py", title="Connect to Neon", icon=":material/rocket_launch:", default=True),
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
        # Streamlit's markdown sanitizer forces target="_blank" on every <a>,
        # which would launch the dashboard in a new tab. Using href="javascript:void(0)"
        # neutralizes that target and routes the click through an onclick handler
        # that navigates the top-level window in place.
        brand_icon_html = (
            f'<a href="javascript:void(0)" '
            f'onclick="window.top.location.href=\'/\'; return false;" '
            f'style="display:block;text-decoration:none;cursor:pointer;" '
            f'title="Go to Dashboard">'
            f'<img src="data:image/png;base64,{img_b64}" '
            f'style="display:block;width:clamp(40px,7vw,58px);height:auto;">'
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
        st.page_link("pages/00_Automated_Pipeline.py", label="Connect to Neon",      icon=":material/warning:")

        st.markdown('<div class="custom-nav-group">Settings</div>', unsafe_allow_html=True)
        st.page_link("pages/0_Master_Configuration.py",label="Global Settings",       icon=":material/settings:")

    st.markdown('</div>', unsafe_allow_html=True)  # close .custom-nav

    # ══ SECTION D: STATUS STRIP (pinned to bottom) ═════════════════════════════
    st.markdown('<div class="sb-status-strip">', unsafe_allow_html=True)

    # ── Neon Cloud DB connection card (only DB status the app cares about) ───
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

    st.markdown('</div>', unsafe_allow_html=True)  # close .sb-status-strip


# ── Run the registered page ────────────────────────────────────────────────────
if pg is not None:
    # ── Mobile bottom navigation bar (≤768px; CSS-gated in utils/theme) ──────────
    # Rendered BEFORE pg.run() so it still appears even when a page calls
    # st.stop() early. position:fixed lifts it out of flow regardless of DOM
    # order, so placement here has no visual cost. Hidden on desktop via the
    # .st-key-mobile_nav rule; the sidebar stays as the secondary drawer nav.
    _render_mobile_nav(data_exists)
    pg.run()
else:
    st.error("Please update Streamlit to >= 1.36 to use native navigation.")


