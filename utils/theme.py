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
    # primary button: gold gradient in dark mode
    PRIMARY_BTN_BG  = "linear-gradient(135deg, #d4900f, #f5a623)",
    PRIMARY_BTN_FG  = "#0c0e14",
    NAV_ACTIVE      = "#f5a623",
    NAV_ACTIVE_BG   = "rgba(245,166,35,0.12)",
)

_LIGHT = dict(
    # Plan §4.3 — refreshed light palette: cooler off-white BG (less yellow cast),
    # slightly more saturated SURFACE2 so cards/active filters pop, higher-contrast
    # BORDER (was disappearing), and navy-tinted black instead of pure black for
    # text — pure #000 on light is harsh and out of brand. GREEN/RED/AMBER/BLUE_ACC
    # now match the unified status system (Healthy/Alert/Watch/Info).
    BG          = "#FAFBFC",   # cooler off-white canvas
    SURFACE     = "#FFFFFF",   # white widgets
    SURFACE2    = "#EEF4FE",   # slightly more saturated blue tint
    BORDER      = "#E1E7F2",   # higher-contrast neutral border
    TEXT_PRI    = "#0F1B33",   # navy-tinted near-black, brand-aligned
    TEXT_SEC    = "#5C6680",   # navy-tinted secondary text
    NAVY        = "#0F2552",   # --btn-navy
    NAVY2       = "#DDE8FE",   # --btn-blue-100
    GOLD        = "#FFBF1A",   # --btn-gold
    GOLD_DIM    = "#E9A800",   # --btn-gold-600
    GREEN       = "#10B981",   # unified status: healthy
    RED         = "#EF4444",   # unified status: alert
    AMBER       = "#F59E0B",   # unified status: watch
    BLUE_ACC    = "#3B82F6",   # unified status: info / neutral
    SIDEBAR_BG  = "#FFFFFF",   # white sidebar
    ALERT_BG    = "rgba(250,251,252,0.9)",
    DROPDOWN_BG = "#FFFFFF",
    SCROLLBAR   = "#D1D9E6",   # cooler, matches new BORDER
    # Primary button: subtle gradient on light mode mirrors the gold-gradient
    # in dark mode and lifts CTAs above flat secondary buttons (plan §4.3).
    PRIMARY_BTN_BG  = "linear-gradient(135deg, #1B59F8, #3D7AFE)",
    PRIMARY_BTN_FG  = "#FFFFFF",
    NAV_ACTIVE      = "#1B59F8",
    NAV_ACTIVE_BG   = "rgba(27,89,248,0.10)",
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

# Tier colors — aligned to the unified status system (plan §4.3).
# ELITE keeps gold (special/highest tier); PREMIUM/REGULER/PASIF/DORMANT now
# use the same exact shades as SUCCESS/INFO/DANGER/neutral so semantics are
# consistent across cluster pies, KPI bars, status chips, and growth arrows.
CLUSTER_COLORS = {
    "ELITE":   "#F59E0B",   # gold (= WARNING shade — special, attention-grabbing)
    "PREMIUM": "#10B981",   # = SUCCESS (healthy, top performers)
    "REGULER": "#3B82F6",   # = INFO    (neutral, the standard tier)
    "PASIF":   "#EF4444",   # = DANGER  (at-risk, action required)
    "DORMANT": "#9CA3AF",   # neutral gray (inactive, no signal)
}

# Unified semantic status colors — plan §4.3 single source of truth.
# Use these everywhere for status chips, KPI accent bars, growth arrows,
# gauge thresholds. Replaces three previously-conflicting red/green shades.
SUCCESS    = "#10B981"   # healthy
WARNING    = "#F59E0B"   # watch
DANGER     = "#EF4444"   # alert
INFO       = "#3B82F6"   # info / neutral

# PM palette — for per-item left-accent cards (STYLING_GUIDE.md §1)
PM_PALETTE = ['#2F80ED', '#9B59B6', '#F39C12', '#1ABC9C', '#E67E22', '#16A085']

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
    BG             = p["BG"]
    SURFACE        = p["SURFACE"]
    SURFACE2       = p["SURFACE2"]
    BORDER         = p["BORDER"]
    TEXT_PRI       = p["TEXT_PRI"]
    TEXT_SEC       = p["TEXT_SEC"]
    NAVY           = p["NAVY"]
    NAVY2          = p["NAVY2"]
    GOLD           = p["GOLD"]
    GOLD_DIM       = p["GOLD_DIM"]
    GREEN          = p["GREEN"]
    RED            = p["RED"]
    AMBER          = p["AMBER"]
    BLUE_ACC       = p["BLUE_ACC"]
    SIDEBAR_BG     = p["SIDEBAR_BG"]
    ALERT_BG       = p["ALERT_BG"]
    DROP_BG        = p["DROPDOWN_BG"]
    SCROLL         = p["SCROLLBAR"]
    PRIMARY_BTN_BG = p["PRIMARY_BTN_BG"]
    PRIMARY_BTN_FG = p["PRIMARY_BTN_FG"]

    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

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

    /* Extended reference palette tokens — all theme-aware */
    --btn-text3:      {TEXT_SEC};
    --btn-bg4:        {SURFACE2};
    --btn-border2:    {BORDER};
    --btn-amber-dim:  rgba(245,166,35,0.12);
    --btn-teal:       #00cec9;
    --btn-purple:     #a29bfe;
    --btn-font-mono:  'JetBrains Mono', monospace;

    /* Unified semantic status tokens — plan §4.3 single source of truth.
       Same shades flow into Python via SUCCESS/WARNING/DANGER/INFO module
       constants and into CLUSTER_COLORS so PREMIUM=success, PASIF=danger, etc. */
    --color-success:  #10B981;   /* healthy */
    --color-warning:  #F59E0B;   /* watch   */
    --color-danger:   #EF4444;   /* alert   */
    --color-info:     #3B82F6;   /* info / neutral */

    /* ── Elevation tokens — plan §4.3 ──
       Layered card shadows tint with navy (matches new TEXT_PRI #0F1B33) so
       elevation feels brand-aligned rather than generic gray. */
    --shadow-card:      0 1px 2px rgba(15,27,51,0.04), 0 4px 12px rgba(15,27,51,0.04);
    --shadow-elevated:  0 2px 4px rgba(15,27,51,0.06), 0 12px 24px rgba(15,27,51,0.06);
    --shadow-popover:   0 8px 24px rgba(15,27,51,0.10), 0 2px 6px rgba(15,27,51,0.05);

    /* ── Type scale — 8 size tokens ── */
    --fs-2xs:   0.65rem;
    --fs-xs:    0.75rem;
    --fs-sm:    0.82rem;
    --fs-base:  0.92rem;
    --fs-md:    1.0rem;
    --fs-lg:    1.15rem;
    --fs-xl:    1.5rem;
    /* KPI sizes use clamp() so long currency strings (e.g. "Rp 1,926.1 M")
       gracefully shrink in narrow / 5-column layouts instead of wrapping
       to two lines and stretching the card height.
         min  = floor on small viewports
         pref = scales with viewport width
         max  = the headline size on wide screens                             */
    --fs-kpi:    clamp(1.6rem, 2.4vw, 2.0rem);   /* per-tab metric boxes      */
    --fs-kpi-lg: clamp(1.8rem, 2.8vw, 2.4rem);   /* page-level hero strip     */

    /* ── KPI typography refinements ── */
    --kpi-letter-spacing: -0.02em;
    --kpi-line-height:    1.05;

    /* ── Font weights — 5 weight tokens ── */
    --fw-regular:  400;
    --fw-medium:   500;
    --fw-semibold: 600;
    --fw-bold:     700;
    --fw-black:    900;

    /* ── Responsive spacing tokens — overridden per breakpoint ──
       Using vars instead of hardcoded px lets every component respond
       to viewport width by cascading a single token override. */
    --card-pad-x:  1.5rem;
    --card-pad-y:  1.375rem;
    --grid-gap:    0.875rem;
    --section-gap: 1rem;
    --size-dot:    8px;

    /* ── Mobile touch-target baseline ──
       Minimum hit area for any interactive control. 44px = Apple HIG /
       48dp Material floor. Overrides cascade per breakpoint below. */
    --touch-min:   44px;
    /* Height reserved at page bottom for the fixed mobile nav bar (Phase 1).
       0 on desktop; set to the bar height inside the ≤768px breakpoint. */
    --mobile-nav-h: 0px;
}}

/* ══════════════════════════════════════════════════════════════════════════
   LAYER 2 — STRUCTURAL RULES  (var() only — zero hardcoded hex)
   ══════════════════════════════════════════════════════════════════════════ */

*, *::before, *::after {{ box-sizing: border-box; }}
html, body {{
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background-color: var(--btn-bg) !important;
    color: var(--btn-text-pri) !important;
}}

.stApp, [data-testid="stAppViewContainer"],
[data-testid="block-container"],
[data-testid="stMarkdown"],
[data-testid="stText"],
.stMarkdown, .stText, p, h1, h2, h3, h4, h5, h6, span, li, label {{
    color: var(--btn-text-pri) !important;
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: var(--btn-sidebar) !important;
    border-right: 1px solid var(--btn-navy2) !important;
}}
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {{
    color: {TEXT_PRI};
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
/* 3. Collapse the native header to zero height; overflow:visible lets the
      absolutely-positioned collapse button float over the brand header. */
[data-testid="stSidebarHeader"] {{
    padding: 0 !important;
    min-height: 0 !important;
    height: 0 !important;
    overflow: visible !important;
    position: relative !important;
    z-index: 200 !important;
}}
/* Hide decorative spacer entirely */
[data-testid="stLogoSpacer"] {{
    display: none !important;
}}
/* Float the collapse button to the top-right, visually aligned with the brand
   header row below it (React state untouched — DOM node not moved). */
[data-testid="stSidebarCollapseButton"] {{
    position: absolute !important;
    top: 16px !important;
    right: 8px !important;
    z-index: 201 !important;
    display: flex !important;
    align-items: center !important;
    visibility: visible !important;
    opacity: 1 !important;
    pointer-events: all !important;
}}
/* ── Fix phantom sidebar scrollbar ── */
/* ROOT CAUSE: Streamlit's own stylesheet ships ~96px padding-bottom on
   stSidebarUserContent and stSidebar's inner wrapper using a more specific
   rule than a bare attribute selector. We match specificity by prefixing
   the tag name (div) so our !important actually wins the cascade. */
div[data-testid="stSidebarUserContent"] {{
    padding-bottom: 0 !important;
    margin-bottom: 0 !important;
}}
div[data-testid="stSidebarNav"] {{
    margin-bottom: 0 !important;
    padding-bottom: 0 !important;
}}
/* The outer flex wrapper inside stSidebar also carries a default
   padding-bottom that compounds the overflow — zero it here. */
[data-testid="stSidebar"] > div:first-child {{
    padding-bottom: 0 !important;
    margin-bottom: 0 !important;
}}
/* Belt-and-suspenders: target the direct stSidebar scroll container */
.stSidebar > div, [data-testid="stSidebar"] > div {{
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
    color: var(--btn-text-sec) !important; font-size: var(--fs-xs) !important;
}}
[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    color: var(--btn-text-pri) !important; font-size: var(--fs-lg) !important; font-weight: 700 !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    background: var(--btn-surface) !important;
    border-radius: 6px; border: 1px solid var(--btn-border); padding: 4px; gap: 2px;
}}
[data-testid="stTabs"] [data-baseweb="tab"] {{
    background: transparent !important; color: var(--btn-text-sec) !important;
    border-radius: 4px !important; font-weight: 500 !important;
    font-size: var(--fs-sm) !important; padding: 8px 16px !important; transition: all 0.15s;
    font-family: 'Roboto', sans-serif !important;
}}
[data-testid="stTabs"] [aria-selected="true"] {{
    background: var(--btn-amber-dim) !important;
    color: var(--btn-gold) !important; font-weight: 700 !important;
    border-bottom: 2px solid var(--btn-gold) !important;
}}
[data-testid="stTabs"] [data-baseweb="tab-highlight"] {{ display: none !important; }}

/* ── Buttons ──
   Plan §4.3 — primary button uses a gradient via PRIMARY_BTN_BG (solid hex
   in dark mode is replaced; light mode now matches the gold-gradient pattern).
   Shadows use the unified elevation tokens so dark-mode (gold) and light-mode
   (blue) buttons each get a brand-aligned glow rather than a hardcoded blue tint. */
[data-testid="stButton"] > button[kind="primary"] {{
    background: {PRIMARY_BTN_BG} !important;
    color: {PRIMARY_BTN_FG} !important; border: none !important; font-weight: 700 !important;
    border-radius: 10px !important; font-family: 'Roboto', sans-serif !important;
    transition: all 120ms cubic-bezier(0.2,0.7,0.2,1);
    box-shadow: var(--shadow-card) !important;
}}
[data-testid="stButton"] > button[kind="primary"]:hover {{
    filter: brightness(0.95); box-shadow: var(--shadow-elevated) !important;
    transform: translateY(-1px);
}}
[data-testid="stButton"] > button[kind="primary"]:active {{
    transform: scale(0.98);
}}
[data-testid="stButton"] > button:not([kind="primary"]) {{
    background: var(--btn-surface) !important; color: var(--btn-text-pri) !important;
    border: 1px solid var(--btn-border) !important; border-radius: 10px !important;
    font-family: 'Roboto', sans-serif !important;
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
/* Only the cosmetic wrapper border — let Streamlit's native theme handle
   inner cell backgrounds so the table correctly follows the light/dark toggle. */
[data-testid="stDataFrame"] {{
    border: 1px solid var(--btn-border) !important; border-radius: 6px !important; overflow: hidden;
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
/* stExpander header colour intentionally left to Streamlit's native theme vars */
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
    font-family: 'Roboto', sans-serif;
    font-size: 1.4rem; font-weight: 700; color: var(--btn-text-pri); margin: 0;
}}
.page-header .subtitle {{
    font-family: var(--btn-font-mono); font-size: 0.78rem;
    color: var(--btn-text3); margin-top: 3px;
}}

.section-label {{
    font-size: 0.78rem; font-weight: 800; letter-spacing: 1.5px;
    text-transform: uppercase; color: var(--btn-text-pri);
    font-family: var(--btn-font-mono); margin: 24px 0 10px 0;
    padding-left: 10px; border-left: 3px solid var(--btn-gold);
}}

.kpi-card {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 20px; padding: var(--card-pad-y) var(--card-pad-x);
    box-shadow: var(--shadow-card);                 /* plan §4.3 — layered navy-tinted */
    position: relative; overflow: hidden;
    display: flex; flex-direction: column; gap: 10px;
    transition: box-shadow .15s ease, transform .15s ease;
    min-width: 0;  /* let inner value span shrink inside flex parents */
}}
.kpi-card:hover {{
    box-shadow: var(--shadow-elevated);
    transform: translateY(-1px);
}}
.kpi-card .kpi-val {{
    /* Headline KPI value. The Python-side helper (kpi_card) inspects len(value)
       and appends a `kpi-val--lg` / `kpi-val--xl` modifier for medium / long
       strings, so trillion-range currency formats downsize gracefully instead
       of being clipped. nowrap stays — digits must not wrap mid-number. */
    font-size: var(--fs-kpi); font-weight: var(--fw-bold);
    font-family: 'Roboto', sans-serif;
    color: var(--btn-text-pri); line-height: var(--kpi-line-height);
    font-variant-numeric: tabular-nums; letter-spacing: var(--kpi-letter-spacing);
    white-space: nowrap;
    max-width: 100%;
}}
/* Length-aware downscale: medium (~9-12 chars) and long (13+ chars) strings.
   Applied to both per-tab and hero variants; the .hero overrides below pick up
   slightly larger sizes so the page-level strip stays visually dominant. */
.kpi-card .kpi-val--lg {{
    font-size: clamp(1.2rem, 2.0vw, 1.7rem);
    letter-spacing: 0;
}}
.kpi-card .kpi-val--xl {{
    font-size: clamp(1.0rem, 1.6vw, 1.4rem);
    letter-spacing: -0.5px;
}}
.kpi-card .kpi-lbl {{
    /* Bumped 13px → fs-xs (0.75rem) and weight 500 → 600 so the label reads
       cleanly under the larger headline value. */
    font-size: var(--fs-xs); font-weight: var(--fw-semibold);
    color: var(--btn-text-sec);
    font-family: 'Roboto', sans-serif;
    text-transform: uppercase; letter-spacing: 1.2px;
}}
.kpi-card.danger  {{ border-top: 3px solid var(--color-danger); }}
.kpi-card.success {{ border-top: 3px solid var(--color-success); }}
.kpi-card.accent  {{ border-top: 3px solid var(--color-info); }}

/* KPI delta + sparkline (plan U3) — direction-aware change indicator and a
   micro inline-SVG trend so each card answers "good or bad, and moving?". */
.kpi-card .kpi-foot {{
    display: flex; align-items: center; justify-content: space-between;
    gap: 8px; margin-top: 2px;
}}
.kpi-card .kpi-delta {{
    font-size: var(--fs-xs); font-weight: var(--fw-bold);
    font-variant-numeric: tabular-nums; white-space: nowrap;
    display: inline-flex; align-items: center; gap: 3px;
}}
.kpi-card .kpi-delta.up   {{ color: var(--color-success); }}
.kpi-card .kpi-delta.down {{ color: var(--color-danger); }}
.kpi-card .kpi-delta.flat {{ color: var(--btn-text-sec); }}
.kpi-card .kpi-spark {{ display: block; height: 26px; flex: 0 0 auto; }}

/* Hero KPI card — page-level strip at the top of the dashboard. Larger
   headline value than per-tab cards so the eye lands here first.
   Padding compacted (1.25x -> 1.0x) to reclaim ~50px of vertical space. */
.kpi-card.hero {{
    padding: var(--card-pad-y) var(--card-pad-x);
    box-shadow: var(--shadow-elevated);
}}
.kpi-card.hero .kpi-val {{
    font-size: var(--fs-kpi-lg);
    font-weight: var(--fw-bold);
}}
.kpi-card.hero .kpi-val--lg {{
    font-size: clamp(1.4rem, 2.4vw, 2.0rem);
}}
.kpi-card.hero .kpi-val--xl {{
    font-size: clamp(1.2rem, 2.0vw, 1.7rem);
}}
.kpi-card.hero .kpi-lbl {{
    font-size: var(--fs-sm);
    margin-top: 6px;
}}

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
    background: var(--btn-surface);
    border: 1px solid var(--btn-border); border-radius: 20px; padding: 24px;
    height: 100%; box-shadow: 0 5px 20px rgba(0,0,0,0.05);
    position: relative; overflow: hidden;
}}
.config-card::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--btn-gold-dim), var(--btn-gold));
}}
.config-card h3 {{ font-size: 1rem; font-weight: 700; color: var(--btn-text-pri); margin: 0 0 12px 0; font-family: 'Roboto', sans-serif; }}

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
/* ── Unified metric box style — single source of truth ──
   `.stat-card` (Card Share / Health Alerts / Anomaly tabs) and `.kpi-card`
   (Weekly tab + new Daily Briefing) now share the SAME visual treatment:
   layered navy-tinted shadow, hover lift, fs-kpi headline value with
   tabular-nums and tight tracking. The colour-strip on `.stat-card`
   stays — it's a useful at-a-glance cue. */
.stats-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:var(--grid-gap); margin-bottom:22px; }}
.stat-card {{
    background: var(--btn-surface); border: 1px solid var(--btn-border);
    border-radius: 20px; padding: var(--card-pad-y) var(--card-pad-x);
    box-shadow: var(--shadow-card);
    position: relative; overflow: hidden;
    display: flex; flex-direction: column; gap: 8px;
    transition: box-shadow .15s ease, transform .15s ease;
}}
.stat-card:hover {{
    box-shadow: var(--shadow-elevated);
    transform: translateY(-1px);
}}
/* 3-px top accent bar (was 2-px) so the colour cue actually reads at a glance */
.stat-card::before {{ content:''; position:absolute; top:0; left:0; right:0; height:3px; }}
.stat-card.amber::before  {{ background: var(--color-warning); }}
.stat-card.blue::before   {{ background: var(--color-info); }}
.stat-card.green::before  {{ background: var(--color-success); }}
.stat-card.purple::before {{ background: var(--btn-purple); }}
.stat-card.red::before    {{ background: var(--color-danger); }}

.stat-label {{
    font-size: var(--fs-xs); color: var(--btn-text-sec); text-transform: uppercase;
    letter-spacing: 1.5px; font-weight: var(--fw-semibold);
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    margin-bottom: 4px;
}}
.stat-value {{
    /* Mirror of .kpi-card .kpi-val so all metric boxes look identical. */
    font-size: var(--fs-kpi); font-weight: var(--fw-bold);
    color: var(--btn-text-pri);
    line-height: var(--kpi-line-height);
    letter-spacing: var(--kpi-letter-spacing);
    font-variant-numeric: tabular-nums;
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.stat-meta {{
    font-size: var(--fs-xs); color: var(--btn-text-sec); margin-top: 4px;
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}}

/* ── Card container ── */
.card {{ background: var(--btn-surface); border: 1px solid var(--btn-border); border-radius: 20px; box-shadow: 0 5px 20px rgba(0,0,0,0.05); overflow: hidden; margin-bottom: 16px; }}
.card-header {{ padding: 16px 24px; border-bottom: 1px solid var(--btn-border); display: flex; align-items: center; gap: 10px; }}
.card-title  {{ font-size: var(--fs-base); font-weight: var(--fw-bold); color: var(--btn-text-pri); font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
.card-subtitle {{ font-size: var(--fs-xs); color: var(--btn-text3); margin-top: 2px; font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
.card-body   {{ padding: 20px 24px; }}
.card-actions {{ margin-left: auto; display: flex; gap: 6px; }}

/* ── Badges ── */
.badge {{ display: inline-flex; align-items: center; padding: 2px 7px; border-radius: 3px; font-size: 10px; font-family: var(--btn-font-mono); font-weight: 600; }}
.badge-amber {{ background: var(--btn-amber-dim); color: var(--btn-gold); }}
.badge-green {{ background: rgba(38,222,129,0.1); color: var(--btn-green); }}
.badge-blue  {{ background: rgba(75,123,236,0.12); color: var(--btn-blue); }}
.badge-red   {{ background: rgba(255,82,82,0.1); color: var(--btn-red); }}
.badge-gray  {{ background: var(--btn-bg4); color: var(--btn-text3); }}

/* ── Section title / sub ── */
.section-title {{ font-size: var(--fs-md); font-weight: var(--fw-bold); color: var(--btn-text-pri); margin-bottom: 4px; font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
.section-sub   {{ font-size: var(--fs-xs); color: var(--btn-text3); margin-bottom: 16px; font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}

/* ── Table value helpers ── */
td.num-val  {{ color: var(--btn-blue); text-align: right; font-family: var(--btn-font-mono); }}
td.date-val {{ color: var(--btn-teal); font-family: var(--btn-font-mono); }}
td.null-val {{ color: var(--btn-text3); font-style: italic; }}

/* ── KPI typography system ── */
.kpi-label {{
    font-size: var(--fs-2xs);
    font-weight: var(--fw-bold);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--btn-text-sec);
    margin-bottom: 6px;
}}
.kpi-value {{
    font-size: var(--fs-kpi);
    font-weight: var(--fw-bold);                 /* plan §4.3: 700, not black 900 */
    color: var(--btn-text-pri);
    line-height: var(--kpi-line-height);
    letter-spacing: var(--kpi-letter-spacing);  /* tighter tracking on the headline number */
    font-variant-numeric: tabular-nums;          /* stable column widths in KPI rows */
}}
.kpi-meta {{
    font-size: var(--fs-xs);
    color: var(--btn-text-sec);
    margin-top: 4px;
}}

/* ── Aggregate stat strip (horizontal KPI row with dividers) ── */
.agg-strip {{
    display: flex;
    border: 1px solid var(--btn-border);
    border-radius: 14px;
    overflow: hidden;
    margin: 12px 0 18px;
}}
.agg-strip-item {{
    flex: 1;
    text-align: center;
    padding: 18px 12px;
    border-right: 1px solid var(--btn-border);
}}
.agg-strip-item:last-child {{ border-right: none; }}
@media (max-width: 640px) {{
    .agg-strip {{ flex-direction: column; }}
    .agg-strip-item {{ border-right: none; border-bottom: 1px solid var(--btn-border); }}
    .agg-strip-item:last-child {{ border-bottom: none; }}
}}

/* ── Dot-spin loading ── */
.dot-spin {{
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--btn-gold); display: inline-block;
    animation: dot-pulse 0.8s infinite;
}}
@keyframes dot-pulse {{ 0%,100%{{opacity:.2;}} 50%{{opacity:1;}} }}

/* ══════════════════════════════════════════════════════════════════════════
   RESPONSIVE / MOBILE  — injected last so !important overrides inline styles
   ══════════════════════════════════════════════════════════════════════════ */

/* ── Tablet (≤ 900px): wrap Streamlit columns so wide grids compress cleanly ── */
@media (max-width: 900px) {{
    :root {{
        --card-pad-x: 1.1rem;
        --grid-gap:   0.75rem;
    }}
    [data-testid="stHorizontalBlock"] {{
        flex-wrap: wrap !important;
    }}
    [data-testid="stColumn"] {{
        min-width: min(100%, 260px) !important;
        flex: 1 1 260px !important;
    }}
    .kpi-card.hero .kpi-val {{ font-size: clamp(1.4rem, 2vw, 1.8rem) !important; }}
    .page-header h1 {{ font-size: clamp(1.1rem, 2.5vw, 1.4rem); }}
}}

/* ── Tablet / large phone (≤ 768px): 2-col stat grids, smaller cards ── */
@media (max-width: 768px) {{
    :root {{
        --card-pad-x: 0.9rem;
        --card-pad-y: 0.75rem;
        --grid-gap:   0.625rem;
    }}
    .stats-grid {{ grid-template-columns: repeat(2, 1fr) !important; }}
    .pipeline-stepper {{ flex-wrap: wrap; gap: 8px; overflow-x: visible; }}
    .step-item {{ min-width: 80px !important; }}
    .stat-value  {{ font-size: 18px; }}
    .kpi-card .kpi-val {{ font-size: clamp(1.2rem, 3.5vw, 1.6rem) !important; }}
    .card-body   {{ padding: 14px 16px; }}
    .card-header {{ padding: 12px 16px; }}
    .section-label {{ font-size: 0.72rem; }}
    .tab-desc {{ padding: 8px 12px; font-size: 0.8rem; }}
    /* 4-col KPI strip → 2×2 grid at tablet */
    [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"]:nth-child(4)) > [data-testid="stColumn"] {{
        flex: 1 1 calc(50% - var(--grid-gap)) !important;
        min-width: calc(50% - var(--grid-gap)) !important;
    }}
    /* Tab buttons: smaller text */
    [data-testid="stTabs"] [data-baseweb="tab"] {{
        font-size: 0.78rem !important;
        padding: 6px 10px !important;
    }}
}}

/* ── Phone (≤ 640px): stack ALL Streamlit columns to full width ── */
@media (max-width: 640px) {{
    :root {{
        --card-pad-x: 0.75rem;
        --card-pad-y: 0.625rem;
    }}
    [data-testid="stHorizontalBlock"] {{
        flex-direction: column !important;
    }}
    [data-testid="stColumn"] {{
        width: 100% !important;
        flex: 1 1 100% !important;
        min-width: 0 !important;
    }}
    .kpi-card .kpi-val {{ font-size: clamp(1.3rem, 5vw, 1.6rem) !important; }}
    /* Tab bar: horizontal scroll so 6+ tabs don't overflow or wrap */
    [data-testid="stTabs"] [data-baseweb="tab-list"] {{
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
        scrollbar-width: none !important;
        flex-wrap: nowrap !important;
    }}
    [data-testid="stTabs"] [data-baseweb="tab-list"]::-webkit-scrollbar {{ display: none; }}
    [data-testid="stTabs"] [data-baseweb="tab"] {{
        white-space: nowrap !important;
        font-size: 0.72rem !important;
        padding: 5px 9px !important;
        min-width: unset !important;
    }}
    /* Allow Plotly containers to shrink below their Python-set height */
    [data-testid="stPlotlyChart"] > div {{ min-height: 0 !important; }}
    [data-testid="stMainBlockContainer"] {{ padding-left: 0.75rem !important; padding-right: 0.75rem !important; }}
}}

/* ── Small phone (≤ 480px): single-column stat grids, compact everything ── */
@media (max-width: 480px) {{
    :root {{
        --card-pad-x: 0.6rem;
        --card-pad-y: 0.5rem;
        --grid-gap:   0.5rem;
        --size-dot:   6px;
    }}
    .stats-grid {{ grid-template-columns: 1fr !important; }}
    .step-item   {{ min-width: 60px !important; font-size: 0.7rem; }}
    .step-circle {{ width: 32px !important; height: 32px !important; font-size: 0.85rem !important; }}
    .step-label  {{ font-size: 0.65rem; }}
    .stat-value  {{ font-size: 16px; }}
    .kpi-card .kpi-val {{ font-size: clamp(1.1rem, 5vw, 1.4rem) !important; }}
    .card-body   {{ padding: 12px 14px; }}
    .card-header {{ padding: 10px 14px; }}
    .section-title {{ font-size: 13px; }}
    .prereq-row  {{ font-size: 0.82rem; }}
    .page-header {{ padding: 10px 12px; }}
    .page-header h1 {{ font-size: clamp(1rem, 5vw, 1.2rem); }}
    .filter-pill {{ font-size: 0.7rem; padding: 3px 8px; }}
    .status-badge {{ font-size: 0.7rem; padding: 3px 8px; }}
    .info-chip {{ font-size: 0.7rem; padding: 3px 8px; }}
    [data-testid="stPlotlyChart"] iframe {{ min-height: 160px !important; }}
    [data-testid="stMainBlockContainer"] {{ padding-left: 0.5rem !important; padding-right: 0.5rem !important; }}
}}

/* ── KPI hero strip — responsive flex grid ──
   5 cards collapse: 5-up (>1100px) -> 3-up (<=1100) -> 2-up (<=768) -> 1-up (<=480).
   min-width:0 is critical: without it, .kpi-val white-space:nowrap forces
   each item's min-content past the viewport -> horizontal scroll. */
.kpi-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 20px;
    width: 100%;
}}
.kpi-row-item {{
    flex: 1 1 calc(20% - 10px);
    min-width: 0;
    display: flex;
}}
.kpi-row-item > .kpi-card {{ flex: 1; min-width: 0; }}
@media (max-width: 1100px) {{
    .kpi-row-item {{ flex: 1 1 calc(33.333% - 8px); }}
}}
@media (max-width: 768px) {{
    .kpi-row {{ gap: 10px; }}
    .kpi-row-item {{ flex: 1 1 calc(50% - 5px); }}
}}
@media (max-width: 480px) {{
    .kpi-row {{ gap: 8px; }}
    .kpi-row-item {{ flex: 1 1 100%; }}
}}

/* ── Config stat grid — responsive 3→2→1 col ── */
.config-stat-grid {{ grid-template-columns: repeat(3, 1fr); gap: var(--grid-gap); }}
@media (max-width: 768px) {{
    .config-stat-grid {{ grid-template-columns: repeat(2, 1fr) !important; }}
}}
@media (max-width: 480px) {{
    .config-stat-grid {{ grid-template-columns: 1fr !important; }}
}}

/* ══════════════════════════════════════════════════════════════════════════
   DASHBOARD HEADER  — compact title
   ══════════════════════════════════════════════════════════════════════════ */

/* Compact dashboard page title (replaces ## H2 with smaller H3 to reclaim
   vertical space). Clamp() keeps it readable across breakpoints. */
.dashboard-page-title {{
    font-size: clamp(1.05rem, 1.6vw, 1.4rem) !important;
    font-weight: var(--fw-bold) !important;
    color: var(--btn-text-pri) !important;
    margin: 2px 0 8px 0 !important;
    line-height: 1.25 !important;
    letter-spacing: -0.01em;
}}

/* Eyebrow / kicker above the dashboard title — a small uppercase label that
   establishes a two-tier hierarchy (kicker + headline), matching the design
   system's .eyebrow pattern. Sits tight above .dashboard-page-title. */
.dashboard-page-eyebrow {{
    font-size: 0.7rem;
    font-weight: var(--fw-bold);
    letter-spacing: 0.13em;
    text-transform: uppercase;
    color: var(--btn-text-sec);
    margin: 4px 0 0 0;
    line-height: 1.1;
    font-family: 'Roboto', sans-serif;
}}

/* ── Passive freshness indicator (replaces the old NEW DATA button) ──
   Status pill aligned to the right of the dashboard page title. Three
   age-based variants. Non-interactive at every layer (pointer-events,
   cursor, no hover state) so it can never be mistaken for a button. */
.dashboard-header-row {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 4px;
}}

.fresh-chip {{
    display: inline-flex;
    align-items: center;
    height: 26px;
    padding: 0 12px 0 10px;
    border-radius: 999px;
    font: 500 12px/1 'Roboto', sans-serif;
    letter-spacing: 0.02em;
    color: rgba(0, 0, 0, 0.70);
    white-space: nowrap;
    pointer-events: none;
    user-select: none;
    cursor: default;
    border: 1px solid transparent;
}}

.fresh-chip__dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 8px;
    flex: 0 0 8px;
}}

.fresh-chip--fresh   {{ background: rgba(47, 234, 155, 0.10); }}
.fresh-chip--fresh   .fresh-chip__dot {{ background: #2FEA9B; }}

.fresh-chip--recent  {{ background: rgba(255, 191, 26, 0.10); }}
.fresh-chip--recent  .fresh-chip__dot {{ background: #FFBF1A; }}

.fresh-chip--stale   {{ background: rgba(229, 24, 55, 0.08); color: rgba(0, 0, 0, 0.80); }}
.fresh-chip--stale   .fresh-chip__dot {{ background: #E51837; }}

.fresh-chip--unknown {{ background: rgba(0, 0, 0, 0.04); color: rgba(0, 0, 0, 0.50); }}
.fresh-chip--unknown .fresh-chip__dot {{ background: #9098A3; }}

@media (max-width: 768px) {{
    .dashboard-header-row {{ flex-direction: column; align-items: flex-start; gap: 8px; }}
}}

/* ── Merchant alert tile — flatten [4,3,2] columns at <=768px ──
   The Health Alerts tab renders each merchant in a bordered container with
   a [4,3,2] split. Below tablet width, stack the three columns vertically
   so labels and the action link don't get squeezed unreadable. */
@media (max-width: 768px) {{
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {{
        flex: 1 1 100% !important;
        min-width: 100% !important;
        margin-bottom: 6px;
    }}
}}

/* ── Cluster card flex grid — already wraps via inline style; harden it
   against narrow viewports so cards don't squish below readable width. */
@media (max-width: 480px) {{
    .config-stat-grid + div [style*="display:flex"][style*="flex-wrap:wrap"] > div {{
        min-width: 100% !important;
    }}
}}

/* ══════════════════════════════════════════════════════════════════════════
   COMFORT BREAKPOINT — large desktop (>=1400px)
   On 13"+ laptops and external monitors, reclaim breathing room and bump
   the hero KPI hierarchy so the page doesn't feel cramped at width.
   ══════════════════════════════════════════════════════════════════════════ */
@media (min-width: 1400px) {{
    :root {{
        --card-pad-x:  1.75rem;
        --card-pad-y:  1.5rem;
        --grid-gap:    1.0rem;
    }}
    .kpi-card.hero .kpi-val {{
        font-size: clamp(2.1rem, 2.6vw, 2.7rem);
    }}
    .dashboard-page-title {{
        font-size: clamp(1.3rem, 1.5vw, 1.6rem) !important;
    }}
}}

/* ════════════════════════════════════════════════════════════════════════════
   MOBILE-FIRST OVERHAUL  (≤768px)
   Appended last so these rules win source-order ties against the older,
   retrofitted phone breakpoints above. Self-contained and reviewable as one
   block. Desktop (>768px) is never matched by anything here.
   ──────────────────────────────────────────────────────────────────────────
   PHASE 0 — Foundation: readable type floor, comfortable spacing, 44px targets.
   ════════════════════════════════════════════════════════════════════════════ */
@media (max-width: 768px) {{
    :root {{
        /* Typography floor — body ≈15px, smallest label ≈11.5px, so nothing
           on a phone needs pinch-zoom. Bumps the older 0.65–0.92rem scale. */
        --fs-2xs:  0.72rem;
        --fs-xs:   0.80rem;
        --fs-sm:   0.86rem;
        --fs-base: 0.95rem;
        --fs-md:   1.02rem;
        /* Comfortable card padding — overrides the ≤480px crush (was 0.5rem).
           Whitespace is the minimalism; cards must not feel cramped. */
        --card-pad-x: 1.05rem;
        --card-pad-y: 1.05rem;
        --grid-gap:   0.7rem;
        --touch-min:  44px;
    }}
    /* Body-copy floor */
    .stApp p, [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] li {{
        font-size: var(--fs-base);
        line-height: 1.55;
    }}
    /* Touch-target baseline — primary interactive controls ≥44px tall */
    [data-testid="stButton"] > button,
    [data-testid="stDownloadButton"] > button,
    [data-baseweb="select"] > div:first-child,
    [data-testid="stTextInput"] input,
    [data-testid="stNumberInput"] input,
    [data-baseweb="input"] input {{
        min-height: var(--touch-min) !important;
    }}
    /* Expander header — comfortable tap area */
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] details > summary {{
        min-height: var(--touch-min) !important;
        display: flex !important;
        align-items: center !important;
    }}
}}

/* ════════════════════════════════════════════════════════════════════════════
   PHASE 1 — Navigation: fixed bottom tab bar + discoverable dashboard tab strip.
   The bottom bar (keyed container `st-key-mobile_nav`, rendered in app.py) is
   hidden on desktop and revealed only inside the ≤768px breakpoint.
   ════════════════════════════════════════════════════════════════════════════ */

/* Hidden by default — desktop never shows the bottom bar. */
.st-key-mobile_nav {{ display: none; }}

@media (max-width: 768px) {{
    :root {{ --mobile-nav-h: 62px; }}

    /* ── Fixed bottom tab bar ─────────────────────────────────────────────── */
    .st-key-mobile_nav {{
        display: block !important;
        position: fixed !important;
        left: 0 !important; right: 0 !important; bottom: 0 !important;
        z-index: 999 !important;
        background: var(--btn-sidebar) !important;
        border-top: 1px solid var(--btn-border) !important;
        box-shadow: 0 -2px 14px rgba(15,27,51,0.07) !important;
        padding: 4px 4px calc(4px + env(safe-area-inset-bottom, 0px)) 4px !important;
    }}
    /* Keep the link row horizontal — overrides the global ≤640px column-stack */
    .st-key-mobile_nav [data-testid="stHorizontalBlock"] {{
        flex-direction: row !important;
        flex-wrap: nowrap !important;
        gap: 0 !important;
    }}
    .st-key-mobile_nav [data-testid="stColumn"] {{
        flex: 1 1 0 !important;
        min-width: 0 !important;
        width: auto !important;
    }}
    /* Each page link → icon stacked over a short label */
    .st-key-mobile_nav [data-testid="stPageLink"] {{ margin: 0 !important; }}
    .st-key-mobile_nav [data-testid="stPageLink"] a {{
        flex-direction: column !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 3px !important;
        padding: 6px 2px !important;
        min-height: 54px !important;
        border-radius: 10px !important;
        text-align: center !important;
    }}
    .st-key-mobile_nav [data-testid="stPageLink"] a p {{
        font-size: 10.5px !important;
        font-weight: 600 !important;
        line-height: 1.1 !important;
        margin: 0 !important;
        white-space: nowrap !important;
    }}
    .st-key-mobile_nav [data-testid="stIconMaterial"] {{
        font-size: 23px !important;
        width: 23px !important; height: 23px !important;
    }}
    /* Active page — BTN blue accent on icon + label + pill background */
    .st-key-mobile_nav [data-testid="stPageLink"] a[aria-current="page"] {{
        background: var(--btn-surface2) !important;
    }}
    .st-key-mobile_nav [data-testid="stPageLink"] a[aria-current="page"] p,
    .st-key-mobile_nav [data-testid="stPageLink"] a[aria-current="page"] [data-testid="stIconMaterial"] {{
        color: var(--btn-blue) !important;
    }}
    /* Reserve space so page content is never hidden behind the fixed bar */
    [data-testid="stMainBlockContainer"] {{
        padding-bottom: calc(var(--mobile-nav-h) + 18px) !important;
    }}

    /* ── Dashboard tab strip — discoverable horizontal scroller ───────────── */
    [data-testid="stTabs"] {{ position: relative !important; }}
    [data-testid="stTabs"] [data-baseweb="tab-list"] {{
        overflow-x: auto !important;
        flex-wrap: nowrap !important;
        scrollbar-width: none !important;
        -webkit-overflow-scrolling: touch !important;
        padding-right: 38px !important;   /* clear room under the fade/chevron */
    }}
    [data-testid="stTabs"] [data-baseweb="tab-list"]::-webkit-scrollbar {{ display: none; }}
    /* Restore a comfortable 44px touch target on every tab */
    [data-testid="stTabs"] [data-baseweb="tab"] {{
        min-height: 44px !important;
        padding: 0 14px !important;
        font-size: 0.82rem !important;
        white-space: nowrap !important;
    }}
    /* Right-edge fade + chevron — signals the strip scrolls horizontally */
    [data-testid="stTabs"]::after {{
        content: "\\203A";
        position: absolute !important;
        top: 0; right: 0;
        width: 40px; height: 54px;
        display: flex; align-items: center; justify-content: flex-end;
        padding-right: 10px;
        font-size: 22px; font-weight: 700; line-height: 1;
        color: var(--btn-text-sec);
        background: linear-gradient(to right, transparent, var(--btn-bg) 65%);
        pointer-events: none;
        z-index: 2;
    }}
}}

/* ════════════════════════════════════════════════════════════════════════════
   PHASE 2 — Layout: KPI "2 hero + 3 mini" strip + chart / table safety nets.
   ════════════════════════════════════════════════════════════════════════════ */
@media (max-width: 768px) {{
    /* ── KPI hero strip → 2 full-width heroes + a compact 3-up mini row ──
       The dashboard renders exactly 5 hero KPI cards, in this order:
         1 Merchants · 2 Sales Volume · 3 Transactions · 4 On-Us · 5 High Risk
       Surface the two most decision-relevant (Sales Volume, High Risk) as
       full-width heroes via flex `order`; compress the remaining three into a
       3-up row so the eye lands on what matters without a 5-card scroll tower. */
    .kpi-row {{ gap: 8px !important; }}
    .kpi-row-item {{ flex: 1 1 calc(33.333% - 6px) !important; }}
    .kpi-row-item:nth-child(2),
    .kpi-row-item:nth-child(5) {{ flex: 1 1 100% !important; }}
    .kpi-row-item:nth-child(2) {{ order: -2; }}
    .kpi-row-item:nth-child(5) {{ order: -1; }}
    /* Hero cards — confident headline */
    .kpi-row-item:nth-child(2) .kpi-card .kpi-val,
    .kpi-row-item:nth-child(5) .kpi-card .kpi-val {{
        font-size: clamp(1.7rem, 7vw, 2.2rem) !important;
    }}
    /* Mini cards — smaller headline + tighter box */
    .kpi-row-item:nth-child(1) .kpi-card,
    .kpi-row-item:nth-child(3) .kpi-card,
    .kpi-row-item:nth-child(4) .kpi-card {{
        padding: 12px 10px !important;
        gap: 4px !important;
    }}
    .kpi-row-item:nth-child(1) .kpi-card .kpi-val,
    .kpi-row-item:nth-child(3) .kpi-card .kpi-val,
    .kpi-row-item:nth-child(4) .kpi-card .kpi-val {{
        font-size: clamp(1.0rem, 4.6vw, 1.3rem) !important;
    }}

    /* ── Charts — never overflow the viewport ── */
    [data-testid="stPlotlyChart"],
    [data-testid="stPlotlyChart"] > div {{
        width: 100% !important;
        min-height: 0 !important;
    }}
    [data-testid="stPlotlyChart"] .plot-container,
    [data-testid="stPlotlyChart"] .svg-container {{ max-width: 100% !important; }}

    /* ── Wide data tables — kept inside the card with smooth touch scroll ── */
    [data-testid="stDataFrame"],
    [data-testid="stTable"] {{
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }}
}}

</style>
"""


def _nav_css(p: dict) -> str:
    """
    Returns the CSS block that hides Streamlit's auto-generated sidebar nav and
    styles the custom st.page_link()-based nav built in app.py.
    Consolidated here so it is maintained in one place only.
    """
    BORDER      = p["BORDER"]
    TEXT_SEC    = p["TEXT_SEC"]
    TEXT_PRI    = p["TEXT_PRI"]
    BG          = p["BG"]
    SIDEBAR_BG  = p["SIDEBAR_BG"]
    SURFACE2    = p["SURFACE2"]
    NAV_ACTIVE  = p["NAV_ACTIVE"]
    NAV_ACT_BG  = p["NAV_ACTIVE_BG"]
    return f"""
<style>
/* ── Sidebar background — white in light, dark in dark ── */
[data-testid="stSidebar"] {{
    background: {SIDEBAR_BG} !important;
    border-right: 1px solid {BORDER} !important;
}}

/* ── Brand Header — sticky, full sidebar width ── */
.sidebar-brand-header {{
    position: sticky !important; top: 0 !important; z-index: 100 !important;
    background: {SIDEBAR_BG} !important;
    padding: 22px 44px 18px 20px !important;
    box-sizing: border-box !important;
    margin: 0 -1rem 0 -1rem !important; width: calc(100% + 2rem) !important;
    display: flex !important; flex-direction: column !important; align-items: flex-start !important; gap: 4px !important;
    border-bottom: 1px solid {BORDER} !important;
}}
.sidebar-brand-header img {{
    width: min(110px, 45vw) !important;
    margin: 0 !important;
    display: block !important;
    flex-shrink: 0 !important;
}}

/* ── Controls strip ── */
.sb-controls {{
    padding: 0.5rem 0 0.25rem 0 !important;
    margin-bottom: 0.25rem !important;
}}
.sb-controls .stToggle label {{ font-size: 0.8rem !important; color: {TEXT_SEC} !important; font-family: 'Roboto', sans-serif !important; }}

/* ── Hide the auto-generated nav widget — routing still works via st.navigation() ── */
[data-testid="stSidebarNav"] {{ display: none !important; }}

/* ── Remove Streamlit's default top-padding on user content ── */
section[data-testid="stSidebarUserContent"] {{ padding-top: 0 !important; }}

/* ── Status strip — pinned to bottom of sidebar ── */
.sb-status-strip {{
    position: sticky !important; bottom: 0 !important;
    padding: 0.6rem 0 0.25rem 0 !important;
    border-top: 1px solid {BORDER} !important;
    background: {SIDEBAR_BG} !important;
    margin-top: auto !important;
}}

/* ── Custom nav section (built with st.page_link) ── */
.custom-nav {{
    padding: 0.5rem 0 1rem 0;
    border-top: 1px solid {BORDER}; margin-top: 0.5rem;
}}
.custom-nav-group {{
    font-family: 'Roboto', sans-serif; font-size: 10.5px;
    text-transform: uppercase; letter-spacing: 0.09em;
    font-weight: 700; color: {TEXT_SEC};
    margin: 0.9rem 0.9rem 0.3rem 0.9rem;
}}

/* ── Style st.page_link() — BTN Anchor nav item appearance ── */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] {{
    border-radius: 10px !important; margin: 0.1rem 0.6rem !important;
    padding: 0 !important; transition: background 120ms cubic-bezier(0.2,0.7,0.2,1) !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"]:hover {{
    background: rgba(0,0,0,0.03) !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a {{
    color: {TEXT_SEC} !important; text-decoration: none !important;
    font-size: 13.5px !important; font-weight: 500 !important;
    padding: 11px 14px !important;
    display: flex !important; align-items: center !important;
    gap: 0.5rem !important; border-radius: 10px !important; width: 100% !important;
    font-family: 'Roboto', sans-serif !important; letter-spacing: -0.01em !important;
}}
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a:hover {{
    background: rgba(0,0,0,0.03) !important; color: {TEXT_PRI} !important;
}}
/* Active page highlight — BTN blue in light, gold in dark ── */
[data-testid="stSidebarUserContent"] [data-testid="stPageLink"] a[aria-current="page"] {{
    background: {NAV_ACT_BG} !important; font-weight: 700 !important;
    color: {NAV_ACTIVE} !important;
}}

/* ── Sidebar logo-mark / logo-sub ── */
.logo-mark {{
    font-family: 'Roboto', sans-serif; font-size: 14px;
    color: {TEXT_PRI}; letter-spacing: -0.01em; font-weight: 700;
}}
.logo-sub {{
    font-size: 10.5px; color: {TEXT_SEC}; margin-top: 0;
    font-family: 'Roboto', sans-serif; letter-spacing: 0.04em;
    text-transform: uppercase; line-height: 1.3;
}}

/* ── DB info card ── */
.db-info {{
    background: {SURFACE2}; border-radius: 10px; padding: 10px 14px;
    font-family: 'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: var(--fs-xs);
    border-left: 3px solid {NAV_ACTIVE};
}}
.db-info .db-label {{ color: {TEXT_SEC}; margin-bottom: 3px; font-size: var(--fs-2xs); letter-spacing: 1px; text-transform: uppercase; }}
.db-info .db-status {{ font-size: var(--fs-xs); font-weight: var(--fw-bold); line-height: 1.3; }}
.db-info .db-meta {{ color: {TEXT_SEC}; font-size: var(--fs-2xs); margin-top: 2px; }}
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
        "Dark Mode",
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
        f'<div style="font-size:var(--fs-sm);color:{txt2};margin-top:3px;">{subtitle}</div>'
        if subtitle else ""
    )
    html = (
        '<div style="display:flex;align-items:center;gap:14px;'
        'margin:28px 0 14px 0;padding:14px 20px;'
        f'background:linear-gradient(135deg,{surf} 0%,{surf2} 100%);'
        f'border-radius:12px;border-left:4px solid {color};'
        'box-shadow:0 2px 10px rgba(0,0,0,.15);">'
        f'<span style="font-size:var(--fs-kpi);line-height:1;">{icon}</span>'
        '<div>'
        f'<div style="font-size:var(--fs-lg);font-weight:var(--fw-black);color:{txt};letter-spacing:-0.01em;font-family:\'Roboto\',-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;">{title}</div>'
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


def _sparkline_svg(values, *, stroke: str = "var(--btn-text-sec)",
                   width: int = 84, height: int = 26) -> str:
    """Inline-SVG micro line chart for a KPI card.

    Returns an empty string for fewer than 2 usable points so the caller can
    simply concatenate the result. preserveAspectRatio='none' lets the card
    stretch the chart to its column width without distorting stroke weight.
    """
    clean = []
    for v in (values or []):
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f == f:  # filter NaN
            clean.append(f)
    if len(clean) < 2:
        return ""
    lo, hi = min(clean), max(clean)
    span = (hi - lo) or 1.0
    pad = 2.0
    step = (width - 2 * pad) / (len(clean) - 1)
    pts = []
    for i, v in enumerate(clean):
        x = pad + i * step
        y = height - pad - (v - lo) / span * (height - 2 * pad)
        pts.append((x, y))
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    lx, ly = pts[-1]
    return (
        f'<svg class="kpi-spark" viewBox="0 0 {width} {height}" '
        f'preserveAspectRatio="none" aria-hidden="true">'
        f'<polyline points="{poly}" fill="none" stroke="{stroke}" '
        f'stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="2" fill="{stroke}"/></svg>'
    )


def kpi_card(value: str, label: str, kind: str = "default", *, hero: bool = False,
             delta: float = None, delta_good: str = "up", spark: list = None) -> str:
    """Render a single KPI box.

    `kind`:  "default" | "danger" | "success" | "accent" — paints the top accent bar.
    `hero`:  set True for the page-level KPI strip at the very top of Overview;
             uses a larger headline value (`--fs-kpi-lg`) and elevated shadow so
             the eye lands here first on page load.
    `delta`: optional period-over-period change as a percent. Renders an
             arrow + value, colored green/red by whether the move is favorable.
    `delta_good`: "up" or "down" — which direction of `delta` is a good outcome
             (e.g. risk count uses "down"). Defaults to "up".
    `spark`: optional list of numbers for a micro inline-SVG trend line.
    """
    classes = ["kpi-card"]
    if kind and kind != "default":
        classes.append(kind)
    if hero:
        classes.append("hero")
    cls = " ".join(classes)

    dir_cls = None
    delta_html = ""
    if delta is not None:
        try:
            d = float(delta)
        except (TypeError, ValueError):
            d = None
        if d is not None and d == d:
            if abs(d) < 0.05:
                dir_cls, arrow = "flat", "→"
            else:
                arrow = "▲" if d > 0 else "▼"
                dir_cls = "up" if (d > 0) == (delta_good == "up") else "down"
            delta_html = (
                f'<span class="kpi-delta {dir_cls}">{arrow} {abs(d):.1f}%</span>'
            )

    _stroke_map = {"up": "var(--color-success)", "down": "var(--color-danger)",
                   "flat": "var(--btn-text-sec)"}
    spark_html = (_sparkline_svg(spark, stroke=_stroke_map.get(dir_cls, "var(--btn-text-sec)"))
                  if spark else "")
    foot = (f'<div class="kpi-foot">{delta_html}{spark_html}</div>'
            if (delta_html or spark_html) else "")

    # Length-aware downscale: short values keep the default --fs-kpi(-lg),
    # medium (9-12 chars) shrink to --lg, long (13+) shrink to --xl. Prevents
    # the trillion-range currency strings ("Rp 2,094.1 M") from being clipped
    # by their container — see corresponding CSS in _make_css().
    _vlen = len(str(value))
    if _vlen <= 8:
        _size_cls = ""
    elif _vlen <= 12:
        _size_cls = " kpi-val--lg"
    else:
        _size_cls = " kpi-val--xl"

    return (f'<div class="{cls}"><div class="kpi-val{_size_cls}">{value}</div>'
            f'<div class="kpi-lbl">{label}</div>{foot}</div>')


def kpi_row(cards: list):
    """Render a horizontal KPI strip that wraps responsively.

    Mobile bug fix: previously used `display:flex` + `flex:1` inline with no
    wrap and no min-width:0. Since `.kpi-val` has `white-space:nowrap`, each
    card's min-content was the full string width, forcing the container past
    the viewport and producing horizontal scroll on phones. The `.kpi-row` /
    `.kpi-row-item` classes (in _make_css) set flex-wrap + min-width:0 + a
    per-breakpoint flex-basis, so 5 cards reflow to 3+2 / 2+2+1 / 1-per-row
    as the viewport narrows.
    """
    inner = "".join(f'<div class="kpi-row-item">{c}</div>' for c in cards)
    st.markdown(
        f'<div class="kpi-row">{inner}</div>',
        unsafe_allow_html=True,
    )


def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert a 6- or 8-digit hex color to an rgba() string.

    Plotly accepts named colors, 6-digit hex (#RRGGBB), and rgb()/rgba()
    strings — but NOT 8-digit hex (#RRGGBBAA) which CSS supports. So when
    we want a tinted fill in a plotly chart we must build rgba() from a
    6-digit hex + an alpha float.

    Examples:
        hex_to_rgba("#EF4444", 0.15)  -> "rgba(239,68,68,0.150)"
        hex_to_rgba("#10B981", 0.5)   -> "rgba(16,185,129,0.500)"
        hex_to_rgba("#10B98180")      -> "rgba(16,185,129,0.502)" (alpha from hex)
    """
    h = (hex_color or "").strip().lstrip("#")
    if len(h) == 8:
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        a = int(h[6:8], 16) / 255.0
    elif len(h) == 6:
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        a = max(0.0, min(1.0, float(alpha)))
    else:
        # Unrecognised — return the input unchanged so plotly's own validator
        # can produce a clearer error message than this helper would.
        return hex_color
    return f"rgba({r},{g},{b},{a:.3f})"


def tab_label_with_badge(label: str, count: int) -> str:
    """Plan §4.1 — add a numeric badge to a tab label so daily users see at a
    glance which tabs need attention. Streamlit's st.tabs() accepts plain text
    only (HTML is rendered as text), so the badge is unicode-only.

    Examples:
        tab_label_with_badge("Health Alerts", 5)  -> "Health Alerts  •  5"
        tab_label_with_badge("Anomaly", 0)        -> "Anomaly"
        tab_label_with_badge("Anomaly", None)     -> "Anomaly"
    """
    if not count:
        return label
    try:
        n = int(count)
    except (TypeError, ValueError):
        return label
    if n <= 0:
        return label
    # Thin-space + bullet + thin-space (explicit unicode escapes so the source
    # is unambiguous and there is no copy-paste hazard) reads as "label · count"
    # without the bullet looking like punctuation belonging to the label itself.
    THIN = " "  # THIN SPACE
    DOT  = "•"  # BULLET
    return f"{label}{THIN}{THIN}{DOT}{THIN}{THIN}{n}"


def tab_desc(text: str):
    st.markdown(f'<div class="tab-desc">{text}</div>', unsafe_allow_html=True)


def filter_pill(text: str):
    st.markdown(f'<div class="filter-pill">{text}</div>', unsafe_allow_html=True)


def portfolio_filter_bar(df_card, scope_key: str):
    """Merchant Group + Anchor Brand selectors for tabs that consume the
    portfolio filter. ``scope_key`` must be unique per tab ("t1", "t2", ...)
    so widget IDs don't collide; the user-facing selection is mirrored into
    shared session-state fields ``pf_group`` / ``pf_brand`` so state persists
    when the user switches between filter-aware tabs.

    Returns (sel_group, sel_brand) read from the shared model.
    """
    st.session_state.setdefault("pf_group", "ALL GROUPS")
    st.session_state.setdefault("pf_brand", "TOTAL PORTFOLIO")

    all_groups = ["ALL GROUPS"]
    if not df_card.empty and "MERCHANT_GROUP" in df_card.columns:
        all_groups += sorted(df_card["MERCHANT_GROUP"].dropna().unique().tolist())

    if st.session_state["pf_group"] not in all_groups:
        st.session_state["pf_group"] = "ALL GROUPS"

    group_widget_key = f"{scope_key}_pf_group"
    brand_widget_key = f"{scope_key}_pf_brand"

    def _sync_group():
        st.session_state["pf_group"] = st.session_state[group_widget_key]
        st.session_state["pf_brand"] = "TOTAL PORTFOLIO"

    def _sync_brand():
        st.session_state["pf_brand"] = st.session_state[brand_widget_key]

    f1, f2 = st.columns(2)
    with f1:
        st.selectbox(
            "Merchant Group",
            all_groups,
            index=all_groups.index(st.session_state["pf_group"]),
            key=group_widget_key,
            on_change=_sync_group,
        )

    sel_group = st.session_state["pf_group"]
    if sel_group != "ALL GROUPS" and not df_card.empty and "MERCHANT_ANCHOR" in df_card.columns:
        brands = (
            df_card[df_card["MERCHANT_GROUP"] == sel_group]["MERCHANT_ANCHOR"]
            .dropna().unique().tolist()
        )
        filtered_brands = ["TOTAL GROUP"] + sorted(brands)
    else:
        filtered_brands = ["TOTAL PORTFOLIO"]

    if st.session_state["pf_brand"] not in filtered_brands:
        st.session_state["pf_brand"] = filtered_brands[0]

    with f2:
        st.selectbox(
            "Merchant Brand (Anchor)",
            filtered_brands,
            index=filtered_brands.index(st.session_state["pf_brand"]),
            key=brand_widget_key,
            on_change=_sync_brand,
        )

    return st.session_state["pf_group"], st.session_state["pf_brand"]


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
        font=dict(family="Roboto, sans-serif", color=p["TEXT_PRI"]),
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
            circle_content = "&#10003;"
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
    dot_color = {"production": "#34D399", "staging": "#FBBF24", "neutral": "#8890b0"}
    color = dot_color.get(kind, "#8890b0")
    dot = (f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
           f'background:{color};margin-right:5px;vertical-align:middle;"></span>')
    return f'<span class="info-chip {kind}">{dot}{label}</span>'


def status_chip_html(label: str, kind: str = "ok") -> str:
    """
    Inline pill chip — STYLING_GUIDE.md §3.
    kind: 'ok' (green) | 'warn' (amber) | 'danger' (red)
    """
    color_map = {"ok": SUCCESS, "warn": WARNING, "danger": DANGER}
    c = color_map.get(kind, SUCCESS)
    return (
        f'<div style="display:inline-block;background:{c}22;border:1px solid {c};'
        f'border-radius:20px;padding:2px 9px;font-size:var(--fs-2xs);color:{c};'
        f'font-weight:var(--fw-bold);margin-top:6px;">{label}</div>'
    )


def left_accent_card(
    icon: str, name: str, count, sub_label: str,
    bar_label: str, bar_value: float, accent: str,
    chip_html: str = "",
) -> str:
    """
    Left-accent card HTML — STYLING_GUIDE.md §1.
    Returns a single card string; wrap multiple cards in:
      <div style="display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;">…</div>
    """
    p         = _palette()
    bar_color = SUCCESS if bar_value >= 80 else (WARNING if bar_value >= 50 else DANGER)
    bar_w     = min(bar_value, 100)
    return (
        f'<div style="flex:1;min-width:150px;border-left:5px solid {accent};'
        f'background:{accent}14;border-radius:0 14px 14px 0;padding:16px 18px;">'
        f'<div class="kpi-label" style="color:{accent};">{icon} {name}</div>'
        f'<div class="kpi-value" style="font-size:var(--fs-kpi);margin:6px 0 2px;">{count}</div>'
        f'<div class="kpi-meta" style="margin-bottom:10px;">{sub_label}</div>'
        f'<div class="kpi-meta" style="margin-bottom:3px;">'
        f'{bar_label}: <span style="color:{bar_color};font-weight:var(--fw-bold);">{bar_value:.0f}%</span></div>'
        f'<div style="height:4px;border-radius:2px;background:{p["BORDER"]};margin-bottom:8px;">'
        f'<div style="width:{bar_w:.1f}%;height:100%;border-radius:2px;background:{bar_color};"></div>'
        f'</div>'
        f'{chip_html}'
        f'</div>'
    )


def status_box(rate_pct: float, narrative: str) -> None:
    """
    Left-border AI insight / status box — STYLING_GUIDE.md §5.
    Renders directly via st.markdown.
    """
    p     = _palette()
    color = SUCCESS if rate_pct >= 100 else (WARNING if rate_pct >= 80 else DANGER)
    label = (
        "ON TRACK" if rate_pct >= 100
        else ("AT RISK" if rate_pct >= 80 else "CRITICAL — INTERVENTION REQUIRED")
    )
    dot = (f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
           f'background:{color};margin-right:6px;vertical-align:middle;"></span>')
    st.markdown(
        f'<div style="border-left:5px solid {color};background:{color}18;'
        f'border-radius:0 12px 12px 0;padding:16px 20px;margin-bottom:14px;">'
        f'<div class="kpi-label" style="color:{color};">{dot}STATUS: {label}</div>'
        f'<div style="font-size:var(--fs-sm);margin-top:8px;color:{p["TEXT_PRI"]};line-height:1.65;">'
        f'{narrative}</div></div>',
        unsafe_allow_html=True,
    )


def stale_data_banner(last_update: str | None = None, threshold_hours: int = 24):
    """
    Show a stale-data notice banner if the most recent pipeline run is older
    than threshold_hours. `last_update` is the LAST_DATA_UPDATE value pulled
    from Neon's app_metadata table (ISO-8601 or 'YYYY-MM-DD HH:MM:SS').

    Returns True iff a stale banner was rendered.
    """
    from datetime import datetime
    p = _palette()

    age_h = None
    if last_update and last_update != "Unknown":
        ts = None
        try:
            ts = datetime.fromisoformat(str(last_update).replace("Z", "+00:00"))
        except ValueError:
            try:
                ts = datetime.strptime(str(last_update), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts = None
        if ts is not None:
            now = datetime.now(ts.tzinfo) if ts.tzinfo else datetime.now()
            age_h = (now - ts).total_seconds() / 3600

    if age_h is None or age_h <= threshold_hours:
        return False

    age_str = f"{age_h/24:.0f} day(s) ago" if age_h >= 24 else f"{age_h:.1f} hour(s) ago"
    st.markdown(
        f"""
<div class="stale-banner">
  <span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#FBBF24;flex-shrink:0;margin-top:2px;"></span>
  <div>
    <strong>You are viewing cached data</strong> — last pipeline run {age_str}.<br>
    <span style="color:{p['TEXT_SEC']};font-size:var(--fs-sm);">
      Run a fresh ingest from the <b>Automated Pipeline</b> page to refresh.
    </span>
  </div>
</div>""",
        unsafe_allow_html=True,
    )
    return True
