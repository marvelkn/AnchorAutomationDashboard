"""
Dashboard page — Analytics hub.

Maps to pages/4_Dashboard.py from the Streamlit app.
Layout only (no callbacks here). All interactivity lives in callbacks/.

Tabs:
  0 — Overview  (KPI cards, PM coverage)
  1 — Card Share (monthly payment breakdown)
  2 — Weekly Monitoring
  3 — Segmentation (K-Means++)
  4 — Risk & Churn
"""
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table

from services.data_service import (
    load_card_share, load_card_monthly, load_monitoring_weekly, db_exists
)
from layouts.kpi_cards import kpi_card, kpi_row

dash.register_page(__name__, path="/", name="Dashboard", order=0)

# ── Colour palette (mirrors utils/theme.py _DARK) ────────────────────────────
GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"

PAYMENT_COLORS = {
    "DEBIT ON US":   "#1B2F5E",
    "DEBIT OFF US":  "#3B82F6",
    "CREDIT OFF US": "#F59E0B",
    "QRIS ON US":    "#22C55E",
    "QRIS OFF US":   "#10B981",
}
CLUSTER_COLORS = {
    "ELITE":   "#A855F7",
    "PREMIUM": "#22C55E",
    "REGULER": "#3B82F6",
    "PASIF":   "#EF4444",
    "DORMANT": "#6B7280",
}

# ── Empty figure (placeholder while callbacks load data) ─────────────────────
_EMPTY_FIG = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor":  "rgba(0,0,0,0)",
        "xaxis": {"visible": False},
        "yaxis": {"visible": False},
    }
}


# ── Global filter bar ─────────────────────────────────────────────────────────
def _filter_bar() -> dbc.Card:
    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Merchant Group", className="text-secondary small mb-1"),
                            dcc.Dropdown(
                                id="dd-group",
                                options=[{"label": "ALL GROUPS", "value": "ALL GROUPS"}],
                                value="ALL GROUPS",
                                clearable=False,
                                className="dash-dropdown-dark",
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Merchant Brand (Anchor)", className="text-secondary small mb-1"),
                            dcc.Dropdown(
                                id="dd-brand",
                                options=[{"label": "ALL BRANDS", "value": "ALL BRANDS"}],
                                value="ALL BRANDS",
                                clearable=False,
                                className="dash-dropdown-dark",
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        html.Div(id="filter-summary", className="text-secondary small pt-4"),
                        width=4,
                    ),
                ],
                align="end",
            ),
            className="py-2",
        ),
        className="mb-4",
        style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
    )


# ── Tab 0: Overview ───────────────────────────────────────────────────────────
def _tab_overview() -> dbc.Tab:
    return dbc.Tab(
        label="Overview",
        tab_id="tab-overview",
        children=[
            html.Div(id="overview-kpi-row", className="mt-3"),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("PM Coverage", className="text-warning"),
                                dbc.CardBody(html.Div(id="overview-pm-table")),
                            ],
                            style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Risk Summary", className="text-warning"),
                                dbc.CardBody(html.Div(id="overview-risk-summary")),
                            ],
                            style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
                        ),
                        width=6,
                    ),
                ],
                className="g-3 mt-1",
            ),
        ],
    )


# ── Tab 1: Card Share ─────────────────────────────────────────────────────────
def _tab_card_share() -> dbc.Tab:
    return dbc.Tab(
        label="Card Share",
        tab_id="tab-cardshare",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Year", className="text-secondary small mb-1"),
                            dcc.Dropdown(
                                id="dd-cs-year",
                                options=[{"label": "All", "value": "All"}],
                                value="All",
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Chart Style", className="text-secondary small mb-1"),
                            dbc.RadioItems(
                                id="radio-cs-style",
                                options=[
                                    {"label": "Stacked Bar", "value": "bar"},
                                    {"label": "Line Trend",  "value": "line"},
                                    {"label": "Both",        "value": "both"},
                                ],
                                value="bar",
                                inline=True,
                                className="text-light",
                            ),
                        ],
                        width=6,
                    ),
                ],
                className="mt-3 mb-3",
                align="end",
            ),
            # Three metric sections (TRANSACTION / SALES VOLUME / FBI)
            dbc.Accordion(
                [
                    _cs_section("TRANSACTION",  "cs-trx"),
                    _cs_section("SALES VOLUME", "cs-sv"),
                    _cs_section("FEE BASED INCOME", "cs-fbi"),
                ],
                always_open=True,
                active_item=["cs-trx", "cs-sv", "cs-fbi"],
            ),
            # Top merchants table
            html.H6("Top Merchants", className="text-warning mt-4 mb-2"),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Slider(id="slider-top-n", min=5, max=20, step=5, value=10,
                                   marks={5: "5", 10: "10", 15: "15", 20: "20"}),
                        width=4,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="dd-top-sort",
                            options=[
                                {"label": "Total SV",  "value": "TOTAL_SV"},
                                {"label": "Total TRX", "value": "TOTAL_TRX"},
                                {"label": "On-Us Ratio","value": "RASIO_ONUS"},
                            ],
                            value="TOTAL_SV",
                            clearable=False,
                        ),
                        width=3,
                    ),
                ],
                className="mb-2",
                align="center",
            ),
            html.Div(id="cs-top-merchants-table"),
        ],
    )


def _cs_section(title: str, item_id: str) -> dbc.AccordionItem:
    return dbc.AccordionItem(
        [
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id=f"chart-{item_id}-bar",  figure=_EMPTY_FIG, config={"displayModeBar": False}), width=6),
                    dbc.Col(dcc.Graph(id=f"chart-{item_id}-line", figure=_EMPTY_FIG, config={"displayModeBar": False}), width=6),
                ],
                className="g-3",
            ),
            dcc.Graph(id=f"chart-{item_id}-donut", figure=_EMPTY_FIG,
                      config={"displayModeBar": False}, style={"height": "280px"}),
        ],
        title=title,
        item_id=item_id,
    )


# ── Tab 2: Weekly Monitoring ──────────────────────────────────────────────────
def _tab_monitoring() -> dbc.Tab:
    return dbc.Tab(
        label="Weekly Monitoring",
        tab_id="tab-monitoring",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Year", className="text-secondary small mb-1"),
                            dcc.Dropdown(id="dd-mon-year", options=[], clearable=False),
                        ],
                        width=2,
                    ),
                    dbc.Col(
                        [
                            html.Label("PM", className="text-secondary small mb-1"),
                            dcc.Dropdown(id="dd-mon-pm", options=[], value="All PMs", clearable=False),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Metric (DIMENSI)", className="text-secondary small mb-1"),
                            dcc.Dropdown(
                                id="dd-mon-metric",
                                options=[
                                    {"label": "Volume (VOL)",       "value": "VOL"},
                                    {"label": "Transactions (TRX)", "value": "TRX"},
                                    {"label": "Fee Income (FBI)",   "value": "FBI"},
                                ],
                                value="VOL",
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        dbc.Button(
                            [html.I(className="bi bi-download me-2"), "Export CSV"],
                            id="btn-mon-export",
                            color="outline-secondary",
                            size="sm",
                            className="mt-4",
                        ),
                        width=2,
                    ),
                ],
                className="mt-3 mb-3",
                align="end",
            ),
            dcc.Download(id="download-mon-csv"),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="chart-mon-line",    figure=_EMPTY_FIG), width=8),
                    dbc.Col(dcc.Graph(id="chart-mon-heatmap", figure=_EMPTY_FIG), width=4),
                ],
                className="g-3 mb-3",
            ),
            html.Div(id="mon-matrix-table"),
        ],
    )


# ── Tab 3: Segmentation ───────────────────────────────────────────────────────
def _tab_segmentation() -> dbc.Tab:
    return dbc.Tab(
        label="Segmentation (K-Means++)",
        tab_id="tab-ml",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Number of Clusters (K)", className="text-secondary small mb-1"),
                            dcc.Slider(id="slider-k", min=3, max=5, step=1, value=3,
                                       marks={3: "3", 4: "4", 5: "5"}),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Filter by PM", className="text-secondary small mb-1"),
                            dcc.Dropdown(id="dd-ml-pm", options=[], multi=True, placeholder="All PMs"),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        dbc.Badge(id="badge-silhouette", color="info", className="mt-4 p-2"),
                        width=4,
                        className="text-end",
                    ),
                ],
                className="mt-3 mb-3",
                align="end",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="chart-ml-3d",      figure=_EMPTY_FIG), width=8),
                    dbc.Col(dcc.Graph(id="chart-ml-pie",     figure=_EMPTY_FIG), width=4),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="chart-ml-box",     figure=_EMPTY_FIG), width=6),
                    dbc.Col(dcc.Graph(id="chart-ml-pm-stack", figure=_EMPTY_FIG), width=6),
                ],
                className="g-3",
            ),
            html.Div(id="ml-segment-table", className="mt-3"),
        ],
    )


# ── Tab 4: Risk & Churn ───────────────────────────────────────────────────────
def _tab_risk() -> dbc.Tab:
    return dbc.Tab(
        label="Risk & Churn",
        tab_id="tab-risk",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Risk Tier", className="text-secondary small mb-1"),
                            dcc.Dropdown(
                                id="dd-risk-tier",
                                options=[
                                    {"label": "All",         "value": "ALL"},
                                    {"label": "High Risk",   "value": "HIGH RISK"},
                                    {"label": "Medium Risk", "value": "MEDIUM RISK"},
                                    {"label": "Stable",      "value": "STABLE"},
                                ],
                                value="ALL",
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                ],
                className="mt-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="chart-risk-pie",   figure=_EMPTY_FIG), width=5),
                    dbc.Col(dcc.Graph(id="chart-risk-gauge", figure=_EMPTY_FIG), width=7),
                ],
                className="g-3 mb-3",
            ),
            html.Div(id="risk-table", className="mb-4"),
            # Anomaly score bar chart (B3)
            html.H6("Isolation Forest Anomaly Scores", className="text-warning mt-3 mb-2"),
            dcc.Graph(id="chart-anomaly-scores", figure=_EMPTY_FIG, className="mb-4"),
            # Merchant drill-down
            dbc.Accordion(
                id="risk-drilldown-accordion",
                children=[],
                always_open=False,
            ),
        ],
    )


# ── AI Insights Accordion ─────────────────────────────────────────────────────
def _ai_insights() -> dbc.Accordion:
    return dbc.Accordion(
        [
            dbc.AccordionItem(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label(
                                        "Drop Threshold Alert (%)",
                                        className="text-secondary small mb-1",
                                    ),
                                    dcc.Slider(
                                        id="slider-churn-threshold",
                                        min=10, max=80, step=5, value=30,
                                        marks={10: "10", 30: "30", 50: "50", 80: "80"},
                                    ),
                                ],
                                width=6,
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(id="ai-churn-anomalies"),
                ],
                title="Silent Churn Anomaly Scanner",
                item_id="ai-churn",
            ),
            dbc.AccordionItem(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Select Merchant", className="text-secondary small mb-1"),
                                    dcc.Dropdown(
                                        id="dd-forecast-merchant",
                                        placeholder="Select merchant…",
                                        clearable=True,
                                        className="dash-dropdown-dark",
                                    ),
                                ],
                                width=6,
                            ),
                            dbc.Col(
                                html.Div(id="badge-forecast-method"),
                                width=6,
                                className="d-flex align-items-center",
                            ),
                        ],
                        className="mb-3",
                    ),
                    dcc.Graph(id="chart-forecast", figure=_EMPTY_FIG, style={"height": "380px"}),
                ],
                title="Deep Dive & Projection",
                item_id="ai-deepdive",
            ),
        ],
        always_open=True,
        active_item=["ai-churn", "ai-deepdive"],
        className="mb-4",
    )


# ── Page Layout ───────────────────────────────────────────────────────────────
def layout() -> html.Div:
    # Protect against missing DB (mirrors Streamlit's st.stop() guard)
    if not db_exists():
        return html.Div(
            dbc.Alert(
                [
                    html.I(className="bi bi-exclamation-triangle-fill me-2"),
                    "Database not found. Run the Automated Pipeline first to load data.",
                ],
                color="warning",
                className="mt-4",
            ),
            className="p-4",
        )

    # Load dropdown seed data (used to populate filter options on first render)
    df_card = load_card_share()
    df_mon  = load_monitoring_weekly()

    groups = (
        ["ALL GROUPS"] + sorted(df_card["MERCHANT_GROUP"].dropna().unique().tolist())
        if not df_card.empty and "MERCHANT_GROUP" in df_card.columns
        else ["ALL GROUPS"]
    )
    mon_years = (
        sorted(df_mon["YEAR"].dropna().unique().tolist(), reverse=True)
        if not df_mon.empty and "YEAR" in df_mon.columns
        else []
    )
    cs_years = (
        ["All"] + sorted(df_card["YEAR"].dropna().unique().tolist(), reverse=True)
        if not df_card.empty and "YEAR" in df_card.columns
        else ["All"]
    )

    return html.Div(
        [
            # Page header
            dbc.Row(
                [
                    dbc.Col(
                        html.H4(
                            "BTN Anchor Merchant Decision Intelligence",
                            className="text-warning fw-bold mb-0",
                        ),
                        width=10,
                    ),
                    dbc.Col(
                        html.Div(id="new-data-badge"),
                        width=2,
                        className="text-end",
                    ),
                ],
                className="mb-3",
                align="center",
            ),
            # Global KPI strip (unfiltered — whole portfolio)
            html.Div(id="global-kpi-row", className="mb-4"),
            # Global filter bar
            _filter_bar(),
            # AI Insights accordion
            _ai_insights(),
            # Main analytics tabs
            dbc.Tabs(
                [
                    _tab_overview(),
                    _tab_card_share(),
                    _tab_monitoring(),
                    _tab_segmentation(),
                    _tab_risk(),
                ],
                id="main-tabs",
                active_tab="tab-overview",
                className="mb-4",
            ),
            # Hidden stores for dropdown seeds (pre-populated server-side)
            dcc.Store(id="store-cs-years",  data=cs_years),
            dcc.Store(id="store-mon-years", data=mon_years),
            dcc.Store(id="store-groups",    data=groups),
        ],
        className="p-2",
    )
