import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# PALETTES
# ──────────────────────────────────────────────────────────────────────────────
_DARK = dict(
    BG          = "#0D1520",
    SURFACE     = "#1A2538",
    SURFACE2    = "#1F2E45",
    BORDER      = "#2B4470",
    TEXT_PRI    = "#EDF1F7",
    TEXT_SEC    = "#A3B5CC",
    NAVY        = "#1B2F5E",
    NAVY2       = "#2B4470",
    GOLD        = "#F0BE48",
    GOLD_DIM    = "#C8A033",
    GREEN       = "#34D399",
    RED         = "#F87171",
    AMBER       = "#FBBF24",
    BLUE_ACC    = "#60A5FA",
    SIDEBAR_BG  = "linear-gradient(180deg,#172B4D 0%,#0D1520 100%)",
    ALERT_BG    = "rgba(26,37,56,0.85)",
    DROPDOWN_BG = "#1F2E45",
    SCROLLBAR   = "#2B4470",
)

_LIGHT = dict(
    BG          = "#F8F6F1",
    SURFACE     = "#FFFFFF",
    SURFACE2    = "#F0ECE3",
    BORDER      = "#D6CFC2",
    TEXT_PRI    = "#1A1A2E",
    TEXT_SEC    = "#596778",
    NAVY        = "#1B2F5E",
    NAVY2       = "#2B4470",
    GOLD        = "#A06C06",
    GOLD_DIM    = "#8A5C05",
    GREEN       = "#16A34A",
    RED         = "#DC2626",
    AMBER       = "#CA8A04",
    BLUE_ACC    = "#2563EB",
    SIDEBAR_BG  = "linear-gradient(180deg,#172B4D 0%,#0D1520 100%)",
    ALERT_BG    = "rgba(248,246,241,0.9)",
    DROPDOWN_BG = "#FFFFFF",
    SCROLLBAR   = "#D6CFC2",
)


def _palette():
    """Return the active palette dict based on session_state."""
    mode = st.session_state.get("theme_mode", "dark")
    return _DARK if mode == "dark" else _LIGHT


def get_palette():
    return _palette()


def is_dark():
    return st.session_state.get("theme_mode", "dark") == "dark"


# ──────────────────────────────────────────────────────────────────────────────
# STATIC DARK defaults (used as module-level imports by other pages)
# Pages that want theme-aware colours should call get_palette() instead.
# ──────────────────────────────────────────────────────────────────────────────
NAVY     = "#1B2F5E"
NAVY2    = "#2B4470"
GOLD     = "#F0BE48"
GOLD_DIM = "#C8A033"
BG       = "#0D1520"
SURFACE  = "#1A2538"
BORDER   = "#2B4470"
TEXT_PRI = "#EDF1F7"
TEXT_SEC = "#A3B5CC"
GREEN    = "#34D399"
RED      = "#F87171"
AMBER    = "#FBBF24"
BLUE_ACC = "#60A5FA"

CLUSTER_COLORS = {
    "PREMIUM": "#22C55E",
    "REGULER": "#3B82F6",
    "PASIF":   "#EF4444",
}

PAYMENT_COLORS = {
    "DEBIT ON US":   "#1B2F5E",
    "DEBIT OFF US":  "#3B82F6",
    "CREDIT OFF US": "#F59E0B",
    "QRIS ON US":    "#22C55E",
    "QRIS OFF US":   "#6EE7B7",
}


# ──────────────────────────────────────────────────────────────────────────────
# CSS GENERATOR
# ──────────────────────────────────────────────────────────────────────────────
def _make_css(p: dict) -> str:
    BG         = p["BG"]
    SURFACE    = p["SURFACE"]
    SURFACE2   = p["SURFACE2"]
    BORDER     = p["BORDER"]
    TEXT_PRI   = p["TEXT_PRI"]
    TEXT_SEC   = p["TEXT_SEC"]
    NAVY       = p["NAVY"]
    NAVY2      = p["NAVY2"]
    GOLD       = p["GOLD"]
    GOLD_DIM   = p["GOLD_DIM"]
    GREEN      = p["GREEN"]
    RED        = p["RED"]
    AMBER      = p["AMBER"]
    BLUE_ACC   = p["BLUE_ACC"]
    SIDEBAR_BG = p["SIDEBAR_BG"]
    ALERT_BG   = p["ALERT_BG"]
    DROP_BG    = p["DROPDOWN_BG"]
    SCROLL     = p["SCROLLBAR"]

    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

*, *::before, *::after {{ box-sizing: border-box; }}
html, body {{
    font-family: 'Inter', -apple-system, sans-serif;
    background-color: {BG} !important;
    color: {TEXT_PRI} !important;
}}
/* Theme text — targeted selectors so we don't break Glide data grid */
.stApp, [data-testid="stAppViewContainer"],
[data-testid="block-container"],
[data-testid="stMarkdown"],
[data-testid="stText"],
.stMarkdown, .stText, p, h1, h2, h3, h4, h5, h6, span, li, label {{
    color: {TEXT_PRI} !important;
    font-family: 'Inter', -apple-system, sans-serif;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: {SIDEBAR_BG} !important;
    border-right: 1px solid {NAVY2} !important;
}}
[data-testid="stSidebar"] * {{ color: #E8EDF5 !important; }}
[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a {{
    border-radius: 8px; transition: background 0.2s;
}}
[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a:hover {{
    background: rgba(240,190,72,0.12) !important;
}}

/* ── Main background ── */
[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"],
.stApp,
[data-testid="block-container"] {{
    background-color: {BG} !important;
}}
[data-testid="stHeader"] {{
    background: {BG} !important;
    border-bottom: 1px solid {BORDER};
}}

/* ── Metric cards ── */
[data-testid="metric-container"] {{
    background: {SURFACE} !important;
    border: 1px solid {BORDER} !important;
    border-radius: 12px !important;
    padding: 16px !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.2) !important;
}}
[data-testid="metric-container"] label {{
    color: {TEXT_SEC} !important; font-size: 0.78rem !important;
}}
[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    color: {TEXT_PRI} !important; font-size: 1.2rem !important; font-weight: 700 !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    background: {SURFACE} !important;
    border-radius: 12px; border: 1px solid {BORDER}; padding: 4px; gap: 2px;
}}
[data-testid="stTabs"] [data-baseweb="tab"] {{
    background: transparent !important; color: {TEXT_SEC} !important;
    border-radius: 9px !important; font-weight: 500 !important;
    font-size: 0.85rem !important; padding: 8px 16px !important; transition: all 0.2s;
}}
[data-testid="stTabs"] [aria-selected="true"] {{
    background: linear-gradient(135deg, {NAVY2}, {NAVY}) !important;
    color: {GOLD} !important; font-weight: 700 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.4) !important;
}}
[data-testid="stTabs"] [data-baseweb="tab-highlight"] {{ display: none !important; }}

/* ── Buttons ── */
[data-testid="stButton"] > button[kind="primary"] {{
    background: linear-gradient(135deg, {GOLD_DIM}, {GOLD}) !important;
    color: {NAVY} !important; border: none !important; font-weight: 700 !important;
    border-radius: 8px !important; transition: all 0.2s;
    box-shadow: 0 2px 8px rgba(184,134,11,0.35) !important;
}}
[data-testid="stButton"] > button[kind="primary"]:hover {{
    transform: translateY(-1px); box-shadow: 0 4px 14px rgba(184,134,11,0.45) !important;
}}
[data-testid="stButton"] > button:not([kind="primary"]) {{
    background: {SURFACE} !important; color: {TEXT_PRI} !important;
    border: 1px solid {BORDER} !important; border-radius: 8px !important;
}}

/* ── Inputs & Selects ── */
[data-testid="stTextInput"] input,
[data-baseweb="select"] > div:first-child,
[data-baseweb="input"] input {{
    background: {SURFACE} !important; color: {TEXT_PRI} !important;
    border-color: {BORDER} !important; border-radius: 8px !important;
}}
[data-baseweb="popover"] [data-baseweb="menu"] {{
    background: {DROP_BG} !important; border: 1px solid {BORDER} !important;
}}
[data-baseweb="option"] {{ color: {TEXT_PRI} !important; background: {DROP_BG} !important; }}
[data-baseweb="option"]:hover {{ background: {NAVY2} !important; color: #E8EDF5 !important; }}

/* ── File uploader ── */
[data-testid="stFileUploader"] {{
    background: {SURFACE} !important; border: 2px dashed {BORDER} !important;
    border-radius: 12px !important; padding: 8px !important;
}}
[data-testid="stFileUploader"]:hover {{ border-color: {GOLD_DIM} !important; }}

/* ── Data tables (Glide Data Editor) ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {BORDER} !important; border-radius: 10px !important; overflow: hidden;
}}
[data-testid="stDataFrame"] > div {{
    background: {SURFACE} !important;
}}

/* ── Expanders ── */
[data-testid="stExpander"] {{
    background: {SURFACE} !important; border: 1px solid {BORDER} !important;
    border-radius: 10px !important;
}}

/* ── Alerts ── */
[data-testid="stAlert"] {{
    background: {ALERT_BG} !important; border-radius: 10px !important;
    border-left-width: 4px !important; color: {TEXT_PRI} !important;
}}

/* ── Divider ── */
hr {{ border-color: {BORDER} !important; opacity: 0.5; }}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: {BG}; }}
::-webkit-scrollbar-thumb {{ background: {SCROLL}; border-radius: 3px; }}

/* ── Radio / Checkbox / Toggle ── */
[data-testid="stRadio"] label {{ color: {TEXT_PRI} !important; }}
[data-testid="stCheckbox"] label {{ color: {TEXT_PRI} !important; }}
[data-testid="stToggle"] label {{ color: {TEXT_PRI} !important; }}

/* ── Multiselect chips ── */
[data-testid="stMultiSelect"] span {{ color: {TEXT_PRI} !important; }}
[data-testid="stMultiSelect"] [data-baseweb="tag"] {{
    background: {GOLD_DIM} !important; color: #fff !important;
    border-radius: 16px !important;
}}

/* ── Slider ── */
[data-testid="stSlider"] label {{ color: {TEXT_PRI} !important; }}
[data-testid="stSlider"] [data-testid="stTickBarMin"],
[data-testid="stSlider"] [data-testid="stTickBarMax"] {{ color: {TEXT_SEC} !important; }}

/* ── Selectbox label ── */
[data-testid="stSelectbox"] label {{ color: {TEXT_PRI} !important; }}

/* ── Expander summary ── */
[data-testid="stExpander"] summary span {{ color: {TEXT_PRI} !important; }}

/* ── Download button ── */
[data-testid="stDownloadButton"] > button {{
    background: {SURFACE} !important; color: {TEXT_PRI} !important;
    border: 1px solid {BORDER} !important; border-radius: 8px !important;
}}

/* ── Spinner ── */
[data-testid="stSpinner"] {{ color: {TEXT_SEC} !important; }}

/* ════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   ════════════════════════════════════════════════════ */

.page-header {{
    display: flex; align-items: center; gap: 12px;
    padding: 18px 0 16px 0;
    border-bottom: 2px solid {GOLD_DIM}; margin-bottom: 24px;
}}
.page-header h1 {{
    font-size: 1.65rem; font-weight: 800;
    background: linear-gradient(90deg, {TEXT_PRI}, {GOLD});
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0;
}}
.page-header .subtitle {{
    font-size: 0.82rem; color: {TEXT_SEC}; margin-top: 3px;
}}

.section-label {{
    font-size: 0.78rem; font-weight: 700; letter-spacing: 0.08em;
    text-transform: uppercase; color: {GOLD_DIM};
    border-left: 3px solid {GOLD}; padding-left: 10px; margin: 22px 0 12px 0;
}}

.kpi-card {{
    background: linear-gradient(135deg, {SURFACE} 0%, {SURFACE2} 100%);
    border: 1px solid {BORDER}; border-radius: 14px; padding: 20px 18px;
    box-shadow: 0 4px 18px rgba(0,0,0,.25);
    transition: transform 0.2s, box-shadow 0.2s;
    position: relative; overflow: hidden; text-align: center;
}}
.kpi-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 22px rgba(0,0,0,.35); }}
.kpi-card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, {GOLD_DIM}, {GOLD});
}}
.kpi-card .kpi-val {{
    font-size: 1.75rem; font-weight: 800; color: {GOLD};
    line-height: 1.1; margin-bottom: 5px;
}}
.kpi-card .kpi-lbl {{
    font-size: 0.74rem; color: {TEXT_SEC}; text-transform: uppercase; letter-spacing: 0.06em;
}}
.kpi-card.danger::before  {{ background: linear-gradient(90deg, #7f1d1d, {RED}); }}
.kpi-card.danger .kpi-val {{ color: {RED}; }}
.kpi-card.success::before {{ background: linear-gradient(90deg, #14532d, {GREEN}); }}
.kpi-card.success .kpi-val {{ color: {GREEN}; }}
.kpi-card.accent::before  {{ background: linear-gradient(90deg, #1e3a8a, {BLUE_ACC}); }}
.kpi-card.accent .kpi-val {{ color: {BLUE_ACC}; }}

.tab-desc {{
    background: {SURFACE2}; border-left: 4px solid {GOLD_DIM};
    padding: 10px 16px; border-radius: 8px;
    font-size: 0.85rem; color: {TEXT_SEC}; margin-bottom: 18px;
}}

.filter-pill {{
    display: inline-block; background: rgba(184,134,11,.12);
    border: 1px solid {GOLD_DIM}; border-radius: 20px; padding: 4px 14px;
    font-size: 0.78rem; color: {GOLD}; margin-bottom: 14px; font-weight: 600;
}}

.status-badge {{
    display: inline-block; border-radius: 6px;
    padding: 3px 10px; font-size: 0.75rem; font-weight: 600;
}}
.status-badge.ok   {{ background: rgba(34,197,94,.15);  color: {GREEN}; border: 1px solid rgba(34,197,94,.3); }}
.status-badge.err  {{ background: rgba(239,68,68,.15);   color: {RED};   border: 1px solid rgba(239,68,68,.3); }}
.status-badge.warn {{ background: rgba(245,158,11,.15);  color: {AMBER}; border: 1px solid rgba(245,158,11,.3); }}

.config-card {{
    background: linear-gradient(135deg, {SURFACE}, {SURFACE2});
    border: 1px solid {BORDER}; border-radius: 14px; padding: 22px 20px;
    height: 100%; box-shadow: 0 4px 14px rgba(0,0,0,.25);
    position: relative; overflow: hidden;
}}
.config-card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, {GOLD_DIM}, {GOLD});
}}
.config-card h3 {{ font-size: 1rem; font-weight: 700; color: {TEXT_PRI}; margin: 0 0 12px 0; }}

/* Status strip card */
.status-strip {{
    background: {SURFACE}; border: 1px solid {BORDER};
    border-radius: 12px; padding: 14px 18px;
    display: flex; align-items: center; gap: 10px;
}}
.status-strip .ss-icon {{ font-size: 1.4rem; }}
.status-strip .ss-label {{
    font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; color: {TEXT_SEC};
}}
.status-strip .ss-value {{
    font-size: 0.9rem; font-weight: 700; color: {TEXT_PRI}; margin-top: 2px;
}}
.status-strip.ok   {{ border-left: 4px solid {GREEN}; }}
.status-strip.err  {{ border-left: 4px solid {RED}; }}
.status-strip.warn {{ border-left: 4px solid {AMBER}; }}
</style>
"""


def apply_theme():
    """Inject the active-mode CSS into the page."""
    p = _palette()
    st.markdown(_make_css(p), unsafe_allow_html=True)


def theme_toggle_sidebar():
    """
    Render a compact dark/light toggle switch in the sidebar.
    Uses st.sidebar.toggle for a modern switch control.
    """
    mode = st.session_state.get("theme_mode", "dark")
    is_dark_mode = (mode == "dark")
    new_val = st.sidebar.toggle(
        "🌙 Dark Mode",
        value=is_dark_mode,
        key="theme_switch",
    )
    if new_val != is_dark_mode:
        st.session_state["theme_mode"] = "dark" if new_val else "light"
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# HELPER COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def page_header(icon: str, title: str, subtitle: str = ""):
    sub_html = f'<div class="subtitle">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f"""<div class="page-header">
            <span style="font-size:2rem;">{icon}</span>
            <div><h1>{title}</h1>{sub_html}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def section_label(text: str):
    st.markdown(f'<div class="section-label">{text}</div>', unsafe_allow_html=True)


def kpi_card(value: str, label: str, kind: str = "default") -> str:
    cls = f"kpi-card {kind}" if kind != "default" else "kpi-card"
    return f'<div class="{cls}"><div class="kpi-val">{value}</div><div class="kpi-lbl">{label}</div></div>'


def kpi_row(cards: list):
    inner = "".join(f'<div style="flex:1;">{c}</div>' for c in cards)
    st.markdown(
        f'<div style="display:flex;gap:12px;margin-bottom:20px;">{inner}</div>',
        unsafe_allow_html=True,
    )


def tab_desc(text: str):
    st.markdown(f'<div class="tab-desc">{text}</div>', unsafe_allow_html=True)


def filter_pill(text: str):
    st.markdown(f'<div class="filter-pill">🔹 {text}</div>', unsafe_allow_html=True)


def status_card(icon: str, label: str, value: str, kind: str = "ok") -> str:
    """Single status card HTML. kind: ok | err | warn"""
    return f"""<div class="status-strip {kind}">
        <div class="ss-icon">{icon}</div>
        <div><div class="ss-label">{label}</div><div class="ss-value">{value}</div></div>
    </div>"""


def apply_plotly_theme(fig):
    """Apply active palette colours to a Plotly figure."""
    p = _palette()
    fig.update_layout(
        font=dict(family="Inter, sans-serif", color=p["TEXT_PRI"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=False, color=p["TEXT_SEC"], linecolor=p["BORDER"], zerolinecolor=p["BORDER"])
    fig.update_yaxes(gridcolor=p["BORDER"], color=p["TEXT_SEC"], linecolor=p["BORDER"], zerolinecolor=p["BORDER"])
    return fig
