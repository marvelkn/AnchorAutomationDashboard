"""
Card Share tab callbacks — year + group/brand filters → stacked bar, line, donut charts.
Maps to the Tab 1 logic in pages/4_Dashboard.py.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import callback, Input, Output, html, dash_table

from services.data_service import load_card_monthly, load_card_share

# BTN colour palette (consistent with dashboard.py)
GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
MUTED   = "#8A9BB5"

PAYMENT_COLORS = {
    "DEBIT ON US":   "#1B2F5E",
    "DEBIT OFF US":  "#3B82F6",
    "CREDIT OFF US": "#F59E0B",
    "QRIS ON US":    "#22C55E",
    "QRIS OFF US":   "#10B981",
}

_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=0, r=0, t=36, b=0),
)
_XAXIS = dict(showgrid=False, color=MUTED)
_YAXIS = dict(showgrid=True, gridcolor=BORDER, color=MUTED)

# Column sets for each metric section
_TRX_COLS = ["TRX_DEBIT_ONUS", "TRX_DEBIT_OFFUS", "TRX_CREDIT_OFFUS", "TRX_QRIS_ONUS", "TRX_QRIS_OFFUS"]
_SV_COLS  = ["SV_DEBIT_ONUS",  "SV_DEBIT_OFFUS",  "SV_CREDIT_OFFUS",  "SV_QRIS_ONUS",  "SV_QRIS_OFFUS"]
_FBI_COLS = ["FBI_DEBIT_ONUS", "FBI_DEBIT_OFFUS", "FBI_CREDIT_OFFUS", "FBI_QRIS_ONUS", "FBI_QRIS_OFFUS"]

_DISPLAY_NAMES = {
    "TRX_DEBIT_ONUS":   "DEBIT ON US",
    "TRX_DEBIT_OFFUS":  "DEBIT OFF US",
    "TRX_CREDIT_OFFUS": "CREDIT OFF US",
    "TRX_QRIS_ONUS":    "QRIS ON US",
    "TRX_QRIS_OFFUS":   "QRIS OFF US",
    "SV_DEBIT_ONUS":    "DEBIT ON US",
    "SV_DEBIT_OFFUS":   "DEBIT OFF US",
    "SV_CREDIT_OFFUS":  "CREDIT OFF US",
    "SV_QRIS_ONUS":     "QRIS ON US",
    "SV_QRIS_OFFUS":    "QRIS OFF US",
    "FBI_DEBIT_ONUS":   "DEBIT ON US",
    "FBI_DEBIT_OFFUS":  "DEBIT OFF US",
    "FBI_CREDIT_OFFUS": "CREDIT OFF US",
    "FBI_QRIS_ONUS":    "QRIS ON US",
    "FBI_QRIS_OFFUS":   "QRIS OFF US",
}


def _filter_df(df, sel_year, sel_group, sel_brand):
    if sel_group and sel_group != "ALL GROUPS" and "MERCHANT_GROUP" in df.columns:
        df = df[df["MERCHANT_GROUP"] == sel_group]
    if sel_brand and sel_brand != "ALL BRANDS" and "MERCHANT_BRAND" in df.columns:
        df = df[df["MERCHANT_BRAND"] == sel_brand]
    if sel_year and sel_year != "All" and "YEAR" in df.columns:
        df = df[df["YEAR"].astype(str) == str(sel_year)]
    return df


def _melt_to_long(df, value_cols, month_col="TRX_MONTH"):
    """Melt wide payment-type columns into long format for plotting."""
    present = [c for c in value_cols if c in df.columns]
    if not present or month_col not in df.columns:
        return pd.DataFrame(columns=["Month", "Type", "Value"])
    melted = df.melt(id_vars=[month_col], value_vars=present, var_name="Type", value_name="Value")
    melted = melted.rename(columns={month_col: "Month"})
    melted["Type"] = melted["Type"].map(_DISPLAY_NAMES).fillna(melted["Type"])
    return melted.groupby(["Month", "Type"], as_index=False)["Value"].sum()


def _bar_chart(long_df, title):
    if long_df.empty:
        return _empty()
    fig = px.bar(
        long_df, x="Month", y="Value", color="Type",
        barmode="stack", color_discrete_map=PAYMENT_COLORS,
        title=title,
    )
    fig.update_layout(height=320, legend=dict(orientation="h", y=-0.3), **_BASE)
    fig.update_xaxes(**_XAXIS)
    fig.update_yaxes(**_YAXIS)
    return fig


def _line_chart(df, value_col, title):
    if df.empty or value_col not in df.columns or "TRX_MONTH" not in df.columns:
        return _empty()
    agg = df.groupby("TRX_MONTH")[value_col].sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=agg["TRX_MONTH"], y=agg[value_col],
        mode="lines+markers+text",
        text=agg[value_col].apply(lambda v: f"{v/1e9:.1f}B" if v >= 1e9 else f"{v/1e6:.1f}M"),
        textposition="top center",
        line=dict(color=GOLD, width=2.5),
        marker=dict(color=GOLD, size=7),
    ))
    fig.update_layout(title=title, height=320, **_BASE)
    fig.update_xaxes(**_XAXIS)
    fig.update_yaxes(**_YAXIS)
    return fig


def _donut_chart(df, value_cols, title):
    present = [c for c in value_cols if c in df.columns]
    if not present or df.empty:
        return _empty()
    vals = df[present].sum()
    labels = [_DISPLAY_NAMES.get(c, c) for c in present]
    colors = [PAYMENT_COLORS.get(l, "#64748B") for l in labels]
    fig = px.pie(
        values=vals, names=labels, hole=0.6,
        color_discrete_sequence=colors, title=title,
    )
    fig.update_layout(height=280, **_BASE)
    return fig


def _empty():
    return {
        "layout": {
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor":  "rgba(0,0,0,0)",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
        }
    }


# ── TRANSACTION charts ─────────────────────────────────────────────────────────
@callback(
    Output("chart-cs-trx-bar",   "figure"),
    Output("chart-cs-trx-line",  "figure"),
    Output("chart-cs-trx-donut", "figure"),
    Input("dd-cs-year",          "value"),
    Input("store-filter-group",  "data"),
    Input("store-filter-brand",  "data"),
    Input("radio-cs-style",      "value"),
)
def update_trx_charts(sel_year, sel_group, sel_brand, chart_style):
    df = _filter_df(load_card_monthly(), sel_year, sel_group, sel_brand)
    long = _melt_to_long(df, _TRX_COLS)
    bar  = _bar_chart(long, "Transaction Mix (Stacked)")  if chart_style in ("bar", "both")  else _empty()
    line = _line_chart(df, "TOTAL_TRX", "Total Transaction Trend") if chart_style in ("line", "both") else _empty()
    donut = _donut_chart(df, _TRX_COLS, "YTD Transaction Mix")
    return bar, line, donut


# ── SALES VOLUME charts ────────────────────────────────────────────────────────
@callback(
    Output("chart-cs-sv-bar",   "figure"),
    Output("chart-cs-sv-line",  "figure"),
    Output("chart-cs-sv-donut", "figure"),
    Input("dd-cs-year",         "value"),
    Input("store-filter-group", "data"),
    Input("store-filter-brand", "data"),
    Input("radio-cs-style",     "value"),
)
def update_sv_charts(sel_year, sel_group, sel_brand, chart_style):
    df = _filter_df(load_card_monthly(), sel_year, sel_group, sel_brand)
    long = _melt_to_long(df, _SV_COLS)
    bar  = _bar_chart(long, "Sales Volume Mix (Stacked)")  if chart_style in ("bar", "both")  else _empty()
    line = _line_chart(df, "TOTAL_SV", "Total Sales Volume Trend") if chart_style in ("line", "both") else _empty()
    donut = _donut_chart(df, _SV_COLS, "YTD Sales Volume Mix")
    return bar, line, donut


# ── FEE BASED INCOME charts ────────────────────────────────────────────────────
@callback(
    Output("chart-cs-fbi-bar",   "figure"),
    Output("chart-cs-fbi-line",  "figure"),
    Output("chart-cs-fbi-donut", "figure"),
    Input("dd-cs-year",          "value"),
    Input("store-filter-group",  "data"),
    Input("store-filter-brand",  "data"),
    Input("radio-cs-style",      "value"),
)
def update_fbi_charts(sel_year, sel_group, sel_brand, chart_style):
    df = _filter_df(load_card_monthly(), sel_year, sel_group, sel_brand)
    long = _melt_to_long(df, _FBI_COLS)
    bar  = _bar_chart(long, "FBI Mix (Stacked)")  if chart_style in ("bar", "both")  else _empty()
    line = _line_chart(df, "TOTAL_FBI", "Total FBI Trend") if chart_style in ("line", "both") else _empty()
    donut = _donut_chart(df, _FBI_COLS, "YTD FBI Mix")
    return bar, line, donut


# ── Top merchants table ────────────────────────────────────────────────────────
@callback(
    Output("cs-top-merchants-table", "children"),
    Input("slider-top-n",           "value"),
    Input("dd-top-sort",            "value"),
    Input("store-filter-group",     "data"),
    Input("store-filter-brand",     "data"),
    Input("dd-cs-year",             "value"),
)
def update_top_merchants(top_n, sort_col, sel_group, sel_brand, sel_year):
    df = _filter_df(load_card_share(), sel_year, sel_group, sel_brand)
    if df.empty or sort_col not in df.columns:
        return html.P("No data available.", className="text-muted small")
    show_cols = ["MERCHANT_GROUP", "TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI", "RASIO_ONUS"]
    present = [c for c in show_cols if c in df.columns]
    top = df.nlargest(top_n or 10, sort_col)[present]
    return dash_table.DataTable(
        data=top.to_dict("records"),
        columns=[{"name": c, "id": c} for c in present],
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": NAVY, "color": GOLD, "fontWeight": "bold"},
        style_cell={"backgroundColor": SURFACE, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "0.8rem", "padding": "6px 12px"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#1E2E42"}],
    )
