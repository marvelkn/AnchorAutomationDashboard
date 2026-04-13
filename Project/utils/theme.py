import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# PALETTES
# ──────────────────────────────────────────────────────────────────────────────
_DARK = dict(
    BG          = "#0c0e14",
    SURFACE     = "#12151e",
    SURFACE2    = "#1a1e2b",
    BORDER      = "#2a2f44",
    TEXT_PRI    = "#e2e6f3",
    TEXT_SEC    = "#8890b0",
    NAVY        = "#12151e",
    NAVY2       = "#222738",
    GOLD        = "#f5a623",
    GOLD_DIM    = "#d4900f",
    GREEN       = "#26de81",
    RED         = "#ff5252",
    AMBER       = "#ffc152",
    BLUE_ACC    = "#4b7bec",
    SIDEBAR_BG  = "#12151e",
    ALERT_BG    = "rgba(26,30,43,0.9)",
    DROPDOWN_BG = "#1a1e2b",
    SCROLLBAR   = "#353c58",
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
NAVY     = "#12151e"
NAVY2    = "#222738"
GOLD     = "#f5a623"
GOLD_DIM = "#d4900f"
BG       = "#0c0e14"
SURFACE  = "#12151e"
BORDER   = "#2a2f44"
TEXT_PRI = "#e2e6f3"
TEXT_SEC = "#8890b0"
GREEN    = "#26de81"
RED      = "#ff5252"
AMBER    = "#ffc152"
BLUE_ACC = "#4b7bec"

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
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');

/* ══════════════════════════════════════════════════════════════════════════
   LAYER 1 — CSS CUSTOM PROPERTIES (single source of truth for all colors)
   Python re-injects this block on every theme toggle. Every downstream
   var() reference auto-updates — no hardcoded hex in rules below.
   ══════════════════════════════════════════════════════════════════════════ */
:root {{
    /* Streamlit native var overrides — ensures native components inherit theme */
    --background-color:            {BG};
    --secondary-background-color:  {SURFACE};
    --text-color:                  {TEXT_PRI};
    --primary-color:               {GOLD};

    /* BTN brand palette tokens */
    --btn-bg:        {BG};
    --btn-surface:   {SURFACE};
    --btn-surface2:  {SURFACE2};
    --btn-border:    {BORDER};
    --btn-text-pri:  {TEXT_PRI};
    --btn-text-sec:  {TEXT_SEC};
    --btn-navy:      {NAVY};
    --btn-navy2:     {NAVY2};
    --btn-gold:      {GOLD};
    --btn-gold-dim:  {GOLD_DIM};
    --btn-green:     {GREEN};
    --btn-red:       {RED};
    --btn-amber:     {AMBER};
    --btn-blue:      {BLUE_ACC};
    --btn-sidebar:   {SIDEBAR_BG};
    --btn-alert-bg:  {ALERT_BG};
    --btn-dropdown:  {DROP_BG};
    --btn-scroll:    {SCROLL};

    /* Extended reference palette tokens */
    --btn-text3:      #545c7e;
    --btn-bg4:        #222738;
    --btn-border2:    #353c58;
    --btn-amber-dim:  rgba(245,166,35,0.12);
    --btn-teal:       #00cec9;
    --btn-purple:     #a29bfe;
    --btn-font-mono:  'JetBrains Mono', monospace;
}}

/* ══════════════════════════════════════════════════════════════════════════
   LAYER 2 — STRUCTURAL RULES  (var() only — zero hardcoded hex)
   ══════════════════════════════════════════════════════════════════════════ */

*, *::before, *::after {{ box-sizing: border-box; }}
html, body {{
    font-family: 'Space Grotesk', -apple-system, sans-serif;
    background-color: var(--btn-bg) !important;
    color: var(--btn-text-pri) !important;
}}

.stApp, [data-testid="stAppViewContainer"],
[data-testid="block-container"],
[data-testid="stMarkdown"],
[data-testid="stText"],
.stMarkdown, .stText, p, h1, h2, h3, h4, h5, h6, span, li, label {{
    color: var(--btn-text-pri) !important;
    font-family: 'Space Grotesk', -apple-system, sans-serif;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: var(--btn-sidebar) !important;
    border-right: 1px solid var(--btn-navy2) !important;
}}
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {{
    color: #E8EDF5;  /* intentionally fixed — sidebar is always dark navy */
}}

/* ── Main background ── */
[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"],
.stApp,
[data-testid="block-container"] {{
    background-color: var(--btn-bg) !important;
}}
/* ── Reduce top dead-space (safe — preserves all native Streamlit controls) ── */
/* 1. Shrink the Streamlit toolbar height but keep it visible so that the
      stSidebarCollapseButton and the mobile hamburger remain reachable.
      We only reduce its minimum height and zero out its internal padding;
      we NEVER use display:none here. */
[data-testid="stHeader"] {{
    min-height: 0 !important;
    height: auto !important;
    padding: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
}}
/* Keep the deploy/menu toolbar icon visible but collapsed to minimum space */
[data-testid="stToolbar"] {{
    padding: 0 !important;
    min-height: 0 !important;
}}
/* 2. Reduce main content top padding now that the toolbar is slim */
[data-testid="stMainBlockContainer"] {{
    padding-top: 1rem !important;
}}
/* 3. Reduce sidebar top dead-space while keeping stSidebarCollapseButton
      fully visible and in normal document flow (never hidden or moved). */
[data-testid="stSidebarHeader"] {{
    padding-top: 0.25rem !important;
    padding-bottom: 0.25rem !important;
    min-height: unset !important;
}}
/* Collapse only the purely decorative spacer that sits ABOVE the collapse
   button — but do NOT hide the button itself. */
[data-testid="stLogoSpacer"] {{
    height: 0 !important;
    min-height: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
}}
/* Guarantee the sidebar collapse/expand button is always visible & clickable */
[data-testid="stSidebarCollapseButton"] {{
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    position: relative !important;
    z-index: 999 !important;
    pointer-events: all !important;
}}
/* ── Fix phantom sidebar scrollbar ── */
/* ROOT CAUSE 1: Streamlit ships ~6rem padding-bottom on the user-content
   section. When stSidebarHeader shrinks (our fix above), this bottom pad
   becomes the sole driver pushing height past 100vh → scrollbar appears.
   We zero it here. The sidebar itself still uses overflow-y: auto so it
   WILL scroll if you ever add enough links to genuinely need it. */
section[data-testid="stSidebarUserContent"] {{
    padding-bottom: 0 !important;
}}
/* ROOT CAUSE 2: stSidebarNav is hidden (display:none), but its sibling
   margin-bottom can still contribute to scroll height in some Streamlit
   versions. Zero it out safely. */
[data-testid="stSidebarNav"] {{
    margin-bottom: 0 !important;
    padding-bottom: 0 !important;
}}
/* ROOT CAUSE 3: The bottom status strip (.sb-status-strip) uses
   margin-top: auto which can inflate the flex container height.
   Pair it with an explicit bottom padding of 0 on the sidebar itself. */
[data-testid="stSidebar"] > div:first-child {{
    padding-bottom: 0 !important;
}}

/* ── Metric cards ── */
[data-testid="metric-container"] {{
    background: var(--btn-surface) !important;
    border: 1px solid var(--btn-border) !important;
    border-radius: 12px !important;
    padding: 16px !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.2) !important;
}}
[data-testid="metric-container"] label {{
    color: var(--btn-text-sec) !important; font-size: 0.78rem !important;
}}
[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    color: var(--btn-text-pri) !important; font-size: 1.2rem !important; font-weight: 700 !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    background: var(--btn-surface) !important;
    border-radius: 6px; border: 1px solid var(--btn-border); padding: 4px; gap: 2px;
}}
[data-testid="stTabs"] [data-baseweb="tab"] {{
    background: transparent !important; color: var(--btn-text-sec) !important;
    border-radius: 4px !important; font-weight: 500 !important;
    font-size: 0.85rem !important; padding: 8px 16px !important; transition: all 0.15s;
    font-family: 'Space Grotesk', sans-serif !important;
}}
[data-testid="stTabs"] [aria-selected="true"] {{
    background: var(--btn-amber-dim) !important;
    color: var(--btn-gold) !important; font-weight: 700 !important;
    border-bottom: 2px solid var(--btn-gold) !important;
}}
[data-testid="stTabs"] [data-baseweb="tab-highlight"] {{ display: none !important; }}

/* ── Buttons ── */
[data-testid="stButton"] > button[kind="primary"] {{
    background: linear-gradient(135deg, var(--btn-gold-dim), var(--btn-gold)) !important;
    color: var(--btn-navy) !important; border: none !important; font-weight: 700 !important;
    border-radius: 8px !important; transition: all 0.2s;
    box-shadow: 0 2px 8px rgba(184,134,11,0.35) !important;
}}
[data-testid="stButton"] > button[kind="primary"]:hover {{
    transform: translateY(-1px); box-shadow: 0 4px 14px rgba(184,134,11,0.45) !important;
}}
[data-testid="stButton"] > button:not([kind="primary"]) {{
    background: var(--btn-surface) !important; color: var(--btn-text-pri) !important;
    border: 1px solid var(--btn-border) !important; border-radius: 8px !important;
}}

/* ── Inputs & Selects ── */
[data-testid="stTextInput"] input,
[data-baseweb="select"] > div:first-child,
[data-baseweb="input"] input {{
    background: var(--btn-surface2) !important; color: var(--btn-text-pri) !important;
    border-color: var(--btn-border2) !important; border-radius: 6px !important;
    font-family: var(--btn-font-mono) !important;
}}
[data-testid="stTextInput"] input:focus,
[data-baseweb="input"] input:focus {{ border-color: var(--btn-gold) !important; }}
[data-baseweb="popover"] [data-baseweb="menu"] {{
    background: var(--btn-dropdown) !important; border: 1px solid var(--btn-border) !important;
}}
[data-baseweb="option"] {{ color: var(--btn-text-pri) !important; background: var(--btn-dropdown) !important; }}
[data-baseweb="option"]:hover {{ background: var(--btn-bg4) !important; color: var(--btn-text-pri) !important; }}

/* ── File uploader ── */
[data-testid="stFileUploader"] {{
    background: var(--btn-surface) !important; border: 2px dashed var(--btn-border) !important;
    border-radius: 12px !important; padding: 8px !important;
}}
[data-testid="stFileUploader"]:hover {{ border-color: var(--btn-gold-dim) !important; }}

/* ── Data tables ── */
[data-testid="stDataFrame"] {{
    border: 1px solid var(--btn-border) !important; border-radius: 6px !important; overflow: hidden;
}}
[data-testid="stDataFrame"] > div {{ background: var(--btn-surface) !important; }}
[data-testid="stDataFrame"] tr:hover td {{ background: var(--btn-bg4) !important; }}

/* ── Expanders ── */
[data-testid="stExpander"] {{
    background: var(--btn-surface) !important; border: 1px solid var(--btn-border) !important;
    border-radius: 10px !important;
}}

/* ── Alerts ── */
[data-testid="stAlert"] {{
    background: var(--btn-alert-bg) !important; border-radius: 10px !important;
    border-left-width: 4px !important; color: var(--btn-text-pri) !important;
}}

/* ── Divider / Scrollbar ── */
hr {{ border-color: var(--btn-border) !important; opacity: 0.5; }}
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: var(--btn-bg); }}
::-webkit-scrollbar-thumb {{ background: var(--btn-scroll); border-radius: 3px; }}

/* ── Form controls ── */
[data-testid="stRadio"] label, [data-testid="stCheckbox"] label,
[data-testid="stToggle"] label, [data-testid="stSlider"] label,
[data-testid="stSelectbox"] label, [data-testid="stMultiSelect"] span {{
    color: var(--btn-text-pri) !important;
}}
[data-testid="stSlider"] [data-testid="stTickBarMin"],
[data-testid="stSlider"] [data-testid="stTickBarMax"] {{ color: var(--btn-text-sec) !important; }}
[data-testid="stMultiSelect"] [data-baseweb="tag"] {{
    background: var(--btn-gold-dim) !important; color: #fff !important; border-radius: 16px !important;
}}
[data-testid="stExpander"] summary span {{ color: var(--btn-text-pri) !important; }}
[data-testid="stDownloadButton"] > button {{
    background: var(--btn-surface) !important; color: var(--btn-text-pri) !important;
    border: 1px solid var(--btn-border) !important; border-radius: 8px !important;
}}
[data-testid="stSpinner"] {{ color: var(--btn-text-sec) !important; }}

/* ════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES  (var() only)
   ════════════════════════════════════════════════════ */

.page-header {{
    display: flex; align-items: center; gap: 14px;
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-left: 3px solid var(--btn-gold); border-radius: 6px;
    padding: 16px 20px; margin-bottom: 24px;
}}
.page-header h1 {{
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.4rem; font-weight: 700; color: var(--btn-text-pri); margin: 0;
}}
.page-header .subtitle {{
    font-family: var(--btn-font-mono); font-size: 0.78rem;
    color: var(--btn-text3); margin-top: 3px;
}}

.section-label {{
    font-size: 0.72rem; font-weight: 600; letter-spacing: 2px;
    text-transform: uppercase; color: var(--btn-text3);
    font-family: var(--btn-font-mono); margin: 20px 0 10px 0;
}}

.kpi-card {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 6px; padding: 16px;
    position: relative; overflow: hidden;
}}
.kpi-card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--btn-gold);
}}
.kpi-card .kpi-val {{
    font-size: 1.6rem; font-weight: 700; font-family: var(--btn-font-mono);
    color: var(--btn-text-pri); line-height: 1.15; margin-bottom: 6px;
}}
.kpi-card .kpi-lbl {{
    font-size: 10px; color: var(--btn-text3);
    text-transform: uppercase; letter-spacing: 1.5px; font-family: var(--btn-font-mono);
}}
.kpi-card.danger::before  {{ background: var(--btn-red); }}
.kpi-card.success::before {{ background: var(--btn-green); }}
.kpi-card.accent::before  {{ background: var(--btn-blue); }}

.tab-desc {{
    background: var(--btn-surface2); border-left: 4px solid var(--btn-gold-dim);
    padding: 10px 16px; border-radius: 8px;
    font-size: 0.85rem; color: var(--btn-text-sec); margin-bottom: 18px;
}}

.filter-pill {{
    display: inline-block; background: rgba(184,134,11,.12);
    border: 1px solid var(--btn-gold-dim); border-radius: 20px; padding: 4px 14px;
    font-size: 0.78rem; color: var(--btn-gold); margin-bottom: 14px; font-weight: 600;
}}

.status-badge {{
    display: inline-block; border-radius: 6px; padding: 3px 10px;
    font-size: 0.75rem; font-weight: 600;
}}
.status-badge.ok   {{ background: rgba(34,197,94,.15);  color: var(--btn-green); border: 1px solid rgba(34,197,94,.3); }}
.status-badge.err  {{ background: rgba(239,68,68,.15);   color: var(--btn-red);   border: 1px solid rgba(239,68,68,.3); }}
.status-badge.warn {{ background: rgba(245,158,11,.15);  color: var(--btn-amber); border: 1px solid rgba(245,158,11,.3); }}

.config-card {{
    background: linear-gradient(135deg, var(--btn-surface), var(--btn-surface2));
    border: 1px solid var(--btn-border); border-radius: 14px; padding: 22px 20px;
    height: 100%; box-shadow: 0 4px 14px rgba(0,0,0,.25);
    position: relative; overflow: hidden;
}}
.config-card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--btn-gold-dim), var(--btn-gold));
}}
.config-card h3 {{ font-size: 1rem; font-weight: 700; color: var(--btn-text-pri); margin: 0 0 12px 0; }}

.status-strip {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 12px; padding: 14px 18px;
    display: flex; align-items: center; gap: 10px;
}}
.status-strip .ss-icon {{ font-size: 1.4rem; }}
.status-strip .ss-label {{
    font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--btn-text-sec);
}}
.status-strip .ss-value {{
    font-size: 0.9rem; font-weight: 700; color: var(--btn-text-pri); margin-top: 2px;
}}
.status-strip.ok   {{ border-left: 4px solid var(--btn-green); }}
.status-strip.err  {{ border-left: 4px solid var(--btn-red); }}
.status-strip.warn {{ border-left: 4px solid var(--btn-amber); }}

/* ── Pipeline Stepper ── */
.pipeline-stepper {{
    display: flex; align-items: flex-start; gap: 0; margin: 24px 0 28px 0;
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 14px; padding: 20px 24px; overflow-x: auto;
}}
.step-item {{
    display: flex; flex-direction: column; align-items: center;
    flex: 1; min-width: 110px; position: relative;
}}
.step-item:not(:last-child)::after {{
    content: ''; position: absolute; top: 20px; left: calc(50% + 22px);
    right: calc(-50% + 22px); height: 2px; background: var(--btn-border); z-index: 0;
}}
.step-item.complete:not(:last-child)::after {{ background: var(--btn-green); }}
.step-item.active:not(:last-child)::after   {{ background: var(--btn-gold-dim); }}
.step-circle {{
    width: 40px; height: 40px; border-radius: 50%; border: 2px solid var(--btn-border);
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem; font-weight: 700; background: var(--btn-surface2);
    color: var(--btn-text-sec); position: relative; z-index: 1; transition: all 0.25s;
}}
.step-item.complete .step-circle {{
    background: var(--btn-green); border-color: var(--btn-green); color: #fff;
}}
.step-item.active .step-circle {{
    background: linear-gradient(135deg, var(--btn-gold-dim), var(--btn-gold));
    border-color: var(--btn-gold); color: var(--btn-navy);
    box-shadow: 0 0 0 4px rgba(240,190,72,0.2);
}}
.step-label {{
    margin-top: 8px; font-size: 0.72rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.06em;
    color: var(--btn-text-sec); text-align: center; line-height: 1.3;
}}
.step-item.active .step-label  {{ color: var(--btn-gold); }}
.step-item.complete .step-label {{ color: var(--btn-green); }}

/* ── Info Chip ── */
.info-chip {{
    display: inline-flex; align-items: center; gap: 5px;
    padding: 3px 10px; border-radius: 20px; font-size: 0.73rem;
    font-weight: 700; letter-spacing: 0.05em;
}}
.info-chip.production {{ background: rgba(52,211,153,.12); color: var(--btn-green); border: 1px solid rgba(52,211,153,.3); }}
.info-chip.staging    {{ background: rgba(251,191,36,.12);  color: var(--btn-amber); border: 1px solid rgba(251,191,36,.3); }}
.info-chip.neutral    {{ background: var(--btn-surface2); color: var(--btn-text-sec); border: 1px solid var(--btn-border); }}

/* ── Prerequisite Checklist ── */
.prereq-card {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 12px; padding: 16px 18px; margin-bottom: 14px;
}}
.prereq-row {{
    display: flex; align-items: center; gap: 10px;
    padding: 6px 0; border-bottom: 1px solid var(--btn-border);
    font-size: 0.88rem; color: var(--btn-text-pri);
}}
.prereq-row:last-child {{ border-bottom: none; }}
.prereq-row .prereq-icon {{ font-size: 1.1rem; min-width: 24px; }}

/* ── Stale Data Banner ── */
.stale-banner {{
    background: rgba(251,191,36,0.08); border: 1px solid rgba(251,191,36,0.3);
    border-left: 4px solid var(--btn-amber); border-radius: 10px;
    padding: 10px 16px; margin-bottom: 16px;
    display: flex; align-items: flex-start; gap: 10px;
    font-size: 0.84rem; color: var(--btn-amber);
}}
.stale-banner strong {{ color: var(--btn-amber); }}

/* ── Sidebar App Header ── */
.sidebar-app-header {{
    padding: 16px 12px 14px 12px;
    border-bottom: 1px solid rgba(43,68,112,0.7);
    margin-bottom: 10px;
}}
.sidebar-app-header .app-name {{
    font-size: 1.05rem; font-weight: 800; color: #E8EDF5;
    line-height: 1.2;
}}
.sidebar-app-header .app-sub {{
    font-size: 0.7rem; color: #7B96BC; margin-top: 2px; letter-spacing: 0.04em;
}}

/* ── Stats grid & Stat cards ── */
.stats-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:20px; }}
.stat-card {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 6px; padding: 16px; position: relative; overflow: hidden;
}}
.stat-card::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
.stat-card.amber::before  {{ background: var(--btn-gold); }}
.stat-card.blue::before   {{ background: var(--btn-blue); }}
.stat-card.green::before  {{ background: var(--btn-green); }}
.stat-card.purple::before {{ background: var(--btn-purple); }}
.stat-card.red::before    {{ background: var(--btn-red); }}
.stat-label {{
    font-size: 10px; color: var(--btn-text3); text-transform: uppercase;
    letter-spacing: 1.5px; font-weight: 600; font-family: var(--btn-font-mono); margin-bottom: 8px;
}}
.stat-value {{ font-size: 22px; font-family: var(--btn-font-mono); font-weight: 700; color: var(--btn-text-pri); }}
.stat-meta  {{ font-size: 10px; color: var(--btn-text3); margin-top: 4px; font-family: var(--btn-font-mono); }}

/* ── Card container ── */
.card {{ background: var(--btn-surface); border: 1px solid var(--btn-border); border-radius: 6px; overflow: hidden; margin-bottom: 16px; }}
.card-header {{ padding: 14px 18px; border-bottom: 1px solid var(--btn-border); display: flex; align-items: center; gap: 10px; }}
.card-title  {{ font-size: 13px; font-weight: 700; color: var(--btn-text-pri); font-family: 'Space Grotesk', sans-serif; }}
.card-subtitle {{ font-size: 11px; color: var(--btn-text3); margin-top: 2px; font-family: var(--btn-font-mono); }}
.card-body   {{ padding: 16px 18px; }}
.card-actions {{ margin-left: auto; display: flex; gap: 6px; }}

/* ── Badges ── */
.badge {{ display: inline-flex; align-items: center; padding: 2px 7px; border-radius: 3px; font-size: 10px; font-family: var(--btn-font-mono); font-weight: 600; }}
.badge-amber {{ background: var(--btn-amber-dim); color: var(--btn-gold); }}
.badge-green {{ background: rgba(38,222,129,0.1); color: var(--btn-green); }}
.badge-blue  {{ background: rgba(75,123,236,0.12); color: var(--btn-blue); }}
.badge-red   {{ background: rgba(255,82,82,0.1); color: var(--btn-red); }}
.badge-gray  {{ background: var(--btn-bg4); color: var(--btn-text3); }}

/* ── Section title / sub ── */
.section-title {{ font-size: 15px; font-weight: 700; color: var(--btn-text-pri); margin-bottom: 4px; font-family: 'Space Grotesk', sans-serif; }}
.section-sub   {{ font-size: 11px; color: var(--btn-text3); margin-bottom: 16px; font-family: var(--btn-font-mono); }}

/* ── Table value helpers ── */
td.num-val  {{ color: var(--btn-blue); text-align: right; font-family: var(--btn-font-mono); }}
td.date-val {{ color: var(--btn-teal); font-family: var(--btn-font-mono); }}
td.null-val {{ color: var(--btn-text3); font-style: italic; }}

/* ── Dot-spin loading ── */
.dot-spin {{
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--btn-gold); display: inline-block;
    animation: dot-pulse 0.8s infinite;
}}
@keyframes dot-pulse {{ 0%,100%{{opacity:.2;}} 50%{{opacity:1;}} }}
</style>
"""


def _nav_css(p: dict) -> str:
    """
    Returns the CSS block that hides Streamlit's auto-generated sidebar nav and
    styles the custom st.page_link()-based nav built in app.py.
    Consolidated here so it is maintained in one place only.
    """
    GOLD   = p["GOLD"]
    BORDER = p["BORDER"]
    TEXT_SEC = p["TEXT_SEC"]
    return f"""
<style>
/* ── Brand Header — sticky, full sidebar width ── */
.sidebar-brand-header {{
    position: sticky !important; top: 0 !important; z-index: 100 !important;
    background: #0c0e14 !important; padding: 1.6rem 1.25rem 1rem 1.25rem !important;
    border-bottom: 1px solid {BORDER} !important; box-sizing: border-box !important;
    margin: -1rem -1rem 0 -1rem !important; width: calc(100% + 2rem) !important;
}}

/* ── Controls strip ── */
.sb-controls {{
    padding: 0.75rem 0 0.5rem 0 !important;
    border-bottom: 1px solid {BORDER} !important; margin-bottom: 0.25rem !important;
}}
.sb-controls .stSelectbox label {{
    font-size: 9px !important; text-transform: uppercase !important;
    letter-spacing: 2px !important; color: #545c7e !important; font-weight: 600 !important;
    font-family: 'JetBrains Mono', monospace !important;
}}
.sb-controls .stSelectbox > div > div {{
    font-size: 0.82rem !important; padding: 5px 10px !important; min-height: 34px !important;
    border-color: {BORDER} !important;
}}
.sb-controls .stSelectbox > div > div:focus-within {{ border-color: {GOLD} !important; }}
.sb-controls .stToggle label {{ font-size: 0.8rem !important; color: {TEXT_SEC} !important; }}

/* ── Hide the auto-generated nav widget — routing still works via st.navigation() ── */
[data-testid="stSidebarNav"] {{ display: none !important; }}

/* ── Remove Streamlit's default top-padding on user content ── */
section[data-testid="stSidebarUserContent"] {{ padding-top: 0 !important; }}

/* ── Status strip — pinned to bottom of sidebar ── */
.sb-status-strip {{
    position: sticky !important; bottom: 0 !important;
    padding: 0.6rem 0 0.25rem 0 !important;
    border-top: 1px solid {BORDER} !important;
    background: #0c0e14 !important;
    margin-top: auto !important;
}}

/* ── Custom nav section (built with st.page_link) ── */
.custom-nav {{
    padding: 0.5rem 0 1rem 0;
    border-top: 1px solid {BORDER}; margin-top: 0.5rem;
}}
.custom-nav-group {{
    font-family: 'JetBrains Mono', monospace; font-size: 9px;
    text-transform: uppercase; letter-spacing: 2px;
    font-weight: 600; color: #545c7e;
    margin: 0.9rem 0.8rem 0.3rem 0.8rem;
}}

/* ── Style st.page_link() to match native nav link appearance ── */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] {{
    border-radius: 6px !important; margin: 0.1rem 0.8rem !important;
    padding: 0 !important; transition: background 0.15s !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"]:hover {{
    background: rgba(245,166,35,0.07) !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a {{
    color: #8890b0 !important; text-decoration: none !important;
    font-size: 0.85rem !important; padding: 0.45rem 0.8rem !important;
    display: flex !important; align-items: center !important;
    gap: 0.5rem !important; border-radius: 6px !important; width: 100% !important;
    font-family: 'Space Grotesk', sans-serif !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a:hover {{
    background: rgba(245,166,35,0.07) !important; color: #e2e6f3 !important;
}}
/* Active page highlight — amber accent */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a[aria-current="page"] {{
    background: rgba(245,166,35,0.12) !important; font-weight: 700 !important;
    border-left: 3px solid {GOLD} !important; color: {GOLD} !important;
}}

/* ── Sidebar logo-mark / logo-sub ── */
.logo-mark {{
    font-family: 'JetBrains Mono', monospace; font-size: 11px;
    color: {GOLD}; letter-spacing: 2px; font-weight: 700;
}}
.logo-sub {{
    font-size: 10px; color: #545c7e; margin-top: 2px;
    font-family: 'JetBrains Mono', monospace;
}}

/* ── DB info card ── */
.db-info {{
    background: #1a1e2b; border-radius: 6px; padding: 10px 12px;
    font-family: 'JetBrains Mono', monospace; font-size: 10px;
    border-left: 3px solid {GOLD};
}}
.db-info .db-label {{ color: #545c7e; margin-bottom: 3px; font-size: 9px; letter-spacing: 1px; text-transform: uppercase; }}
.db-info .db-status {{ font-size: 11px; font-weight: 700; line-height: 1.3; }}
.db-info .db-meta {{ color: #8890b0; font-size: 9px; margin-top: 2px; }}
</style>
"""


def apply_theme():
    """Inject the active-mode CSS into the page."""
    p = _palette()
    st.markdown(_make_css(p), unsafe_allow_html=True)
    _render_global_pipeline_status()

@st.fragment(run_every="5s")
def _render_global_pipeline_status():
    import os
    import json
    _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    STATUS_FILE = os.path.join(_BASE, "database", "pipeline_status.json")
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                data = json.load(f)
            if data.get("status") == "running":
                st.sidebar.info(f"⏳ **ETL Pipeline Running:**\\n{data.get('message', 'Processing...')}")
        except:
            pass


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


def section_header(icon: str, title: str, subtitle: str = "", accent_color: str = None):
    """
    Large, visually distinct section header for separating major data blocks.
    Renders a gradient card with icon, bold title, and optional subtitle.
    """
    p     = _palette()
    color = accent_color or p["GOLD"]
    surf  = p["SURFACE"]
    surf2 = p["SURFACE2"]
    txt   = p["TEXT_PRI"]
    txt2  = p["TEXT_SEC"]
    sub_html = (
        f'<div style="font-size:0.8rem;color:{txt2};margin-top:3px;">{subtitle}</div>'
        if subtitle else ""
    )
    html = (
        '<div style="display:flex;align-items:center;gap:14px;'
        'margin:28px 0 14px 0;padding:14px 20px;'
        f'background:linear-gradient(135deg,{surf} 0%,{surf2} 100%);'
        f'border-radius:12px;border-left:4px solid {color};'
        'box-shadow:0 2px 10px rgba(0,0,0,.15);">'
        f'<span style="font-size:1.8rem;line-height:1;">{icon}</span>'
        '<div>'
        f'<div style="font-size:1.05rem;font-weight:800;color:{txt};letter-spacing:0.03em;">{title}</div>'
        f'{sub_html}'
        '</div>'
        '</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def styled_divider():
    """Gradient horizontal rule — replaces plain st.markdown('---')."""
    p = _palette()
    st.markdown(
        f'<div style="height:1px;background:linear-gradient(90deg,{p["GOLD_DIM"]},transparent);'
        f'margin:22px 0 8px 0;"></div>',
        unsafe_allow_html=True,
    )


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


def badge_html(text: str, variant: str = "amber") -> str:
    """Return badge HTML. variant: amber | green | blue | red | gray"""
    return f'<span class="badge badge-{variant}">{text}</span>'


def card_wrap(title: str, subtitle: str = "") -> str:
    """Return opening HTML for a card container with header. Close with </div></div>."""
    sub = f'<div class="card-subtitle">{subtitle}</div>' if subtitle else ""
    return (
        f'<div class="card">'
        f'<div class="card-header"><div>'
        f'<div class="card-title">{title}</div>{sub}'
        f'</div></div>'
        f'<div class="card-body">'
    )


def apply_plotly_theme(fig):
    """Apply active palette colours to a Plotly figure."""
    p = _palette()
    fig.update_layout(
        font=dict(family="Space Grotesk, sans-serif", color=p["TEXT_PRI"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=False, color=p["TEXT_SEC"], linecolor=p["BORDER"], zerolinecolor=p["BORDER"])
    fig.update_yaxes(gridcolor=p["BORDER"], color=p["TEXT_SEC"], linecolor=p["BORDER"], zerolinecolor=p["BORDER"])
    return fig


# ──────────────────────────────────────────────────────────────────────────────
# NEW HELPER COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def pipeline_stepper(steps: list, current_step: int):
    """
    Render a horizontal visual stepper.
    steps: list of (icon, label) tuples
    current_step: 0-based index of the active step (-1 = not started, len(steps) = all done)
    """
    items = []
    for i, (icon, label) in enumerate(steps):
        if i < current_step:
            state = "complete"
            circle_content = "✓"
        elif i == current_step:
            state = "active"
            circle_content = icon
        else:
            state = "pending"
            circle_content = str(i + 1)
        items.append(
            f'<div class="step-item {state}">'
            f'<div class="step-circle">{circle_content}</div>'
            f'<div class="step-label">{label}</div>'
            f'</div>'
        )
    st.markdown(
        f'<div class="pipeline-stepper">{" ".join(items)}</div>',
        unsafe_allow_html=True,
    )


def info_chip(label: str, kind: str = "neutral") -> str:
    """
    Return HTML for a small pill chip.
    kind: 'production' | 'staging' | 'neutral'
    """
    icons = {"production": "🟢", "staging": "🟡", "neutral": "⚪"}
    icon = icons.get(kind, "⚪")
    return f'<span class="info-chip {kind}">{icon} {label}</span>'


def stale_data_banner(db_path: str = None, threshold_hours: int = 24):
    """
    Show a stale-data notice banner if the staging.db is older than threshold_hours.
    Always shows if db_path is None (data came from Excel fallback).
    """
    import os
    from datetime import datetime
    p = _palette()

    is_stale = True
    age_str = "unknown age"

    if db_path and os.path.exists(db_path):
        mtime = os.path.getmtime(db_path)
        age_h = (datetime.now().timestamp() - mtime) / 3600
        is_stale = age_h > threshold_hours
        if is_stale:
            if age_h >= 24:
                age_str = f"{age_h/24:.0f} day(s) ago"
            else:
                age_str = f"{age_h:.1f} hour(s) ago"

    if is_stale:
        st.markdown(
            f"""
<div class="stale-banner">
  <span style="font-size:1.3rem;">⚠️</span>
  <div>
    <strong>You are viewing cached data</strong> — last updated {age_str}.<br>
    <span style="color:{p['TEXT_SEC']};font-size:0.82rem;">
      For the most current analytics, upload a new <code>staging.db</code> in
      <b>🚀 Automated Pipeline</b> or refresh the Master Excel files in <b>⚙️ Global Settings</b>.
    </span>
  </div>
</div>""",
            unsafe_allow_html=True,
        )
    return is_stale
