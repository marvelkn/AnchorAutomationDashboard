"""
Overview tab callbacks.

B4 — PM Coverage table (overview-pm-table).
B5 — Risk Summary KPI cards (overview-risk-summary).
"""
import pandas as pd
from dash import callback, Output, Input, html, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
RED     = "#EF4444"
AMBER   = "#F59E0B"
GREEN   = "#22C55E"
PURPLE  = "#A855F7"


# ── B4: PM Coverage table ─────────────────────────────────────────────────────
@callback(
    Output("overview-pm-table", "children"),
    Input("store-ml-result",    "data"),
)
def update_pm_table(ml_json):
    if not ml_json:
        return html.P("No data.", className="text-muted small")

    df = pd.read_json(ml_json, orient="split")
    if df.empty or "PM" not in df.columns:
        return html.P("No PM data available.", className="text-muted small")

    pm_stats = df.groupby("PM").agg(
        Merchants   =("MERCHANT_GROUP", "count"),
        Avg_Risk    =("RISK_SCORE",     "mean"),
        High_Risk   =("CHURN_RISK",     lambda x: (x == "HIGH RISK").sum()),
        Total_SV    =("TOTAL_SV",       "sum"),
    ).reset_index()

    pm_stats["Avg_Risk"]  = pm_stats["Avg_Risk"].round(1)
    pm_stats["Total_SV"]  = (pm_stats["Total_SV"] / 1e9).round(2).astype(str) + " B"
    pm_stats.columns      = ["PM", "Merchants", "Avg Risk", "High Risk Count", "Total SV"]

    return dash_table.DataTable(
        data=pm_stats.to_dict("records"),
        columns=[{"name": c, "id": c} for c in pm_stats.columns],
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": SURFACE, "color": GOLD, "fontWeight": "bold"},
        style_cell={"backgroundColor": NAVY, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "13px", "padding": "6px 10px"},
        style_data_conditional=[
            {"if": {"filter_query": "{High Risk Count} > 0"},
             "backgroundColor": "rgba(239,68,68,0.10)"},
        ],
    )


# ── B5: Risk Summary KPI cards ────────────────────────────────────────────────
@callback(
    Output("overview-risk-summary", "children"),
    Input("store-ml-result",        "data"),
)
def update_risk_summary(ml_json):
    if not ml_json:
        return ""

    df = pd.read_json(ml_json, orient="split")
    if df.empty:
        return ""

    n_high   = int((df["CHURN_RISK"] == "HIGH RISK").sum())   if "CHURN_RISK"      in df.columns else 0
    n_medium = int((df["CHURN_RISK"] == "MEDIUM RISK").sum()) if "CHURN_RISK"      in df.columns else 0
    avg_risk = round(float(df["RISK_SCORE"].mean()), 1)       if "RISK_SCORE"      in df.columns else 0.0
    pct_anom = round(
        float(df["IF_IS_ANOMALY"].sum()) / len(df) * 100, 1
    ) if "IF_IS_ANOMALY" in df.columns and len(df) > 0 else 0.0

    def _kpi(label, value, color):
        return dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.P(label, className="text-muted small mb-1"),
                    html.H4(str(value), style={"color": color, "fontWeight": "bold"}),
                ]),
                style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
            ),
            className="mb-2",
        )

    return dbc.Row([
        _kpi("High Risk Merchants",  n_high,          RED),
        _kpi("Medium Risk",          n_medium,        AMBER),
        _kpi("Avg Portfolio Risk",   f"{avg_risk}/100", GOLD),
        _kpi("Anomaly Rate",         f"{pct_anom}%",  PURPLE),
    ])
