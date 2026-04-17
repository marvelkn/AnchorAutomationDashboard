"""
Forecast & Churn Scanner callbacks.

B1 — Holt-Winters merchant forecast chart (ai-deepdive accordion).
B2 — Silent Churn Scanner table (ai-churn-anomalies).
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import callback, Output, Input, html, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from services.data_service import load_card_monthly
from services.ml_service import hw_forecast

GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
MUTED   = "#8A9BB5"
RED     = "#EF4444"
AMBER   = "#F59E0B"
PURPLE  = "#A855F7"

_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=0, r=0, t=36, b=0),
)


# ── B1a: Populate forecast merchant dropdown ──────────────────────────────────
@callback(
    Output("dd-forecast-merchant", "options"),
    Input("store-filter-group", "data"),
)
def populate_forecast_dropdown(sel_group):
    df = load_card_monthly()
    if df.empty:
        return []
    if sel_group and sel_group != "ALL GROUPS" and "MERCHANT_GROUP" in df.columns:
        df = df[df["MERCHANT_GROUP"] == sel_group]
    merchants = sorted(df["MERCHANT_GROUP"].dropna().unique())
    return [{"label": m, "value": m} for m in merchants]


# ── B1b: Holt-Winters forecast chart ─────────────────────────────────────────
@callback(
    Output("chart-forecast",       "figure"),
    Output("badge-forecast-method","children"),
    Input("dd-forecast-merchant",  "value"),
    Input("store-filter-group",    "data"),
)
def update_forecast_chart(merchant, sel_group):
    _placeholder_fig = go.Figure()
    _placeholder_fig.update_layout(
        **_BASE,
        annotations=[dict(
            text="Select a merchant to see forecast",
            showarrow=False,
            font=dict(color=MUTED, size=14),
            x=0.5, y=0.5, xref="paper", yref="paper",
        )],
    )

    if not merchant:
        return _placeholder_fig, ""

    df_monthly = load_card_monthly()
    if df_monthly.empty or "MERCHANT_GROUP" not in df_monthly.columns:
        return _placeholder_fig, dbc.Badge("No monthly data available", color="warning")

    df_m = df_monthly[df_monthly["MERCHANT_GROUP"] == merchant].copy()
    if df_m.empty:
        return _placeholder_fig, dbc.Badge("No data for this merchant", color="warning")

    df_m = df_m.sort_values("TRX_MONTH")
    monthly_sv = df_m.set_index("TRX_MONTH")["TOTAL_SV"]

    result = hw_forecast(monthly_sv, periods_ahead=12)

    fig = go.Figure()

    # Historical line
    fig.add_trace(go.Scatter(
        x=[str(d) for d in monthly_sv.index],
        y=monthly_sv.values.tolist(),
        mode="lines+markers",
        name="Historical SV",
        line=dict(color=GOLD, width=2),
        marker=dict(size=5),
    ))

    if result["success"]:
        forecast_vals = result["forecast"]
        try:
            last_date = pd.to_datetime(monthly_sv.index[-1])
        except Exception:
            last_date = pd.Timestamp.now()

        forecast_dates = [
            (last_date + pd.DateOffset(months=i + 1)).strftime("%Y-%m")
            for i in range(len(forecast_vals))
        ]

        # Bridge point + forecast line (dashed)
        last_x = str(monthly_sv.index[-1])
        fig.add_trace(go.Scatter(
            x=[last_x] + forecast_dates,
            y=[float(monthly_sv.iloc[-1])] + [float(v) for v in forecast_vals],
            mode="lines",
            name="Forecast",
            line=dict(color=PURPLE, width=2, dash="dash"),
        ))

        # ±15% confidence band
        upper = [v * 1.15 for v in forecast_vals]
        lower = [max(v * 0.85, 0) for v in forecast_vals]
        fig.add_trace(go.Scatter(
            x=forecast_dates + forecast_dates[::-1],
            y=upper + lower[::-1],
            fill="toself",
            fillcolor="rgba(168,85,247,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Confidence Band (±15%)",
            showlegend=True,
        ))

        method_label = result.get("method", "Holt-Winters")
        badge = dbc.Badge(f"Method: {method_label}", color="info", className="ms-2")
    else:
        badge = dbc.Badge("Insufficient data for forecast", color="warning", className="ms-2")

    fig.update_layout(
        **_BASE,
        height=360,
        xaxis_title="Month",
        yaxis_title="Sales Volume (SV)",
        legend=dict(orientation="h", y=1.12),
        xaxis=dict(gridcolor=BORDER),
        yaxis=dict(gridcolor=BORDER),
    )

    return fig, badge


# ── B2: Silent Churn Scanner ──────────────────────────────────────────────────
@callback(
    Output("ai-churn-anomalies",       "children"),
    Input("slider-churn-threshold",    "value"),
    Input("store-ml-result",           "data"),
)
def update_churn_scanner(threshold, ml_json):
    if not ml_json:
        return html.P("No ML data available yet.", className="text-muted small")

    df = pd.read_json(ml_json, orient="split")
    if df.empty or "SV_GROWTH_CLIPPED" not in df.columns:
        return html.P("No data.", className="text-muted small")

    pct     = (threshold or 30) / 100
    flagged = df[df["SV_GROWTH_CLIPPED"] <= -pct].copy()
    flagged = flagged.sort_values("SV_GROWTH_CLIPPED")

    if flagged.empty:
        return dbc.Alert(
            f"No merchants dropped more than {threshold}% this period.",
            color="success",
            className="mt-2",
        )

    flagged["Drop %"] = (flagged["SV_GROWTH_CLIPPED"] * 100).round(1).astype(str) + "%"
    cols    = [c for c in ["MERCHANT_GROUP", "PM", "Drop %", "RISK_SCORE", "CHURN_RISK"] if c in flagged.columns]
    display = flagged[cols].rename(columns={"MERCHANT_GROUP": "Merchant", "RISK_SCORE": "Risk Score"})

    return dash_table.DataTable(
        data=display.to_dict("records"),
        columns=[{"name": c, "id": c} for c in display.columns],
        sort_action="native",
        page_size=10,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": SURFACE, "color": GOLD, "fontWeight": "bold"},
        style_cell={"backgroundColor": NAVY, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "0.8rem", "padding": "6px 12px"},
        style_data_conditional=[
            {"if": {"filter_query": '{CHURN_RISK} = "HIGH RISK"'},
             "backgroundColor": "rgba(239,68,68,0.15)", "color": RED},
            {"if": {"filter_query": '{CHURN_RISK} = "MEDIUM RISK"'},
             "backgroundColor": "rgba(245,158,11,0.15)", "color": AMBER},
        ],
    )
