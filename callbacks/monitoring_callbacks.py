"""
Weekly Monitoring tab callbacks.
Maps to Tab 2 logic in pages/4_Dashboard.py.
"""
import io
import pandas as pd
import plotly.express as px
from dash import callback, Input, Output, dcc, html, dash_table

from services.data_service import load_monitoring_weekly

GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
MUTED   = "#8A9BB5"

_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=0, r=0, t=36, b=0),
)

_WEEK_COLS = [f"W{str(i).zfill(2)}" for i in range(1, 53)]


def _filter(df, sel_year, sel_pm, sel_metric):
    if sel_year and "YEAR" in df.columns:
        df = df[df["YEAR"].astype(str) == str(sel_year)]
    if sel_pm and sel_pm != "All PMs" and "PM" in df.columns:
        df = df[df["PM"] == sel_pm]
    if sel_metric and "DIMENSI" in df.columns:
        df = df[df["DIMENSI"] == sel_metric]
    return df


@callback(
    Output("chart-mon-line",    "figure"),
    Output("chart-mon-heatmap", "figure"),
    Output("mon-matrix-table",  "children"),
    Input("dd-mon-year",        "value"),
    Input("dd-mon-pm",          "value"),
    Input("dd-mon-metric",      "value"),
    Input("store-filter-group", "data"),
)
def update_monitoring(sel_year, sel_pm, sel_metric, sel_group):
    df_raw = load_monitoring_weekly()
    if df_raw.empty:
        return _empty(), _empty(), html.P("No monitoring data.", className="text-muted small")

    df = _filter(df_raw, sel_year, sel_pm, sel_metric)
    if sel_group and sel_group != "ALL GROUPS" and "MERCHANT_GROUP" in df.columns:
        df = df[df["MERCHANT_GROUP"] == sel_group]

    week_cols = [c for c in _WEEK_COLS if c in df.columns]
    if df.empty or not week_cols:
        return _empty(), _empty(), html.P("No data for selected filters.", className="text-muted small")

    # ── Line trend (melt to long) ─────────────────────────────────────────────
    id_cols = [c for c in ["MERCHANT_GROUP", "PM", "DIMENSI"] if c in df.columns]
    long = df[id_cols + week_cols].melt(id_vars=id_cols, var_name="Week", value_name="Value")
    long["Value"] = pd.to_numeric(long["Value"], errors="coerce").fillna(0)
    agg = long.groupby(["Week", "MERCHANT_GROUP"] if "MERCHANT_GROUP" in id_cols else ["Week"],
                       as_index=False)["Value"].sum()
    color_col = "MERCHANT_GROUP" if "MERCHANT_GROUP" in agg.columns else None
    line_fig = px.line(
        agg, x="Week", y="Value",
        color=color_col,
        markers=True,
        title=f"{sel_metric or 'Metric'} Weekly Trend — {sel_year or 'All Years'}",
    )
    line_fig.update_layout(height=380, legend=dict(orientation="h", y=-0.3), **_BASE)
    line_fig.update_xaxes(showgrid=False, color=MUTED)
    line_fig.update_yaxes(showgrid=True, gridcolor=BORDER, color=MUTED)

    # ── Heatmap ───────────────────────────────────────────────────────────────
    if "MERCHANT_GROUP" in df.columns:
        heat = df.set_index("MERCHANT_GROUP")[week_cols].fillna(0)
    else:
        heat = df[week_cols].fillna(0)
    heat_fig = px.imshow(
        heat,
        color_continuous_scale="Blues",
        title="Weekly Performance Heatmap",
        aspect="auto",
    )
    heat_fig.update_layout(height=380, **_BASE)

    # ── Matrix DataTable ──────────────────────────────────────────────────────
    display_cols = id_cols + week_cols[:13]   # show W01-W13 to fit screen
    table_df = df[display_cols].copy()
    table = dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in table_df.columns],
        sort_action="native",
        page_action="native",
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": NAVY, "color": GOLD, "fontWeight": "bold",
                      "fontSize": "0.75rem"},
        style_cell={"backgroundColor": SURFACE, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "0.75rem", "padding": "4px 8px", "minWidth": "60px"},
    )

    return line_fig, heat_fig, table


@callback(
    Output("download-mon-csv", "data"),
    Input("btn-mon-export",    "n_clicks"),
    Input("dd-mon-year",       "value"),
    Input("dd-mon-pm",         "value"),
    Input("dd-mon-metric",     "value"),
    prevent_initial_call=True,
)
def export_csv(n_clicks, sel_year, sel_pm, sel_metric):
    if not n_clicks:
        return None
    df = _filter(load_monitoring_weekly(), sel_year, sel_pm, sel_metric)
    return dcc.send_data_frame(df.to_csv, "monitoring_export.csv", index=False)


def _empty():
    return {
        "layout": {
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor":  "rgba(0,0,0,0)",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
        }
    }
