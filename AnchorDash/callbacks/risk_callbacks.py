"""
Risk & Churn tab callbacks.
Maps to Tab 4 logic in pages/4_Dashboard.py.
"""
import plotly.express as px
import plotly.graph_objects as go
from dash import callback, Input, Output, html, dash_table
import dash_bootstrap_components as dbc

from services.data_service import load_card_share, load_monitoring, load_target
from services.ml_service import run_ml

GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
MUTED   = "#8A9BB5"
RED     = "#EF4444"
AMBER   = "#F59E0B"
GREEN   = "#22C55E"

_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=0, r=0, t=36, b=0),
)


def _empty():
    return {
        "layout": {
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor":  "rgba(0,0,0,0)",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
        }
    }


def _get_ml_df(sel_group):
    df_c = load_card_share()
    df_m = load_monitoring()
    df_t = load_target()
    if sel_group and sel_group != "ALL GROUPS":
        if not df_c.empty and "MERCHANT_GROUP" in df_c.columns:
            df_c = df_c[df_c["MERCHANT_GROUP"] == sel_group]
        if not df_m.empty and "MERCHANT_GROUP" in df_m.columns:
            df_m = df_m[df_m["MERCHANT_GROUP"] == sel_group]
    return run_ml(df_c, df_m, df_t if not df_t.empty else None)


@callback(
    Output("chart-risk-pie",          "figure"),
    Output("chart-risk-gauge",        "figure"),
    Output("risk-table",              "children"),
    Output("risk-drilldown-accordion","children"),
    Input("dd-risk-tier",             "value"),
    Input("store-filter-group",       "data"),
)
def update_risk_tab(sel_tier, sel_group):
    df = _get_ml_df(sel_group)
    if df.empty:
        empty = _empty()
        return empty, empty, html.P("No data.", className="text-muted small"), []

    # Filter by risk tier
    df_filt = df.copy()
    if sel_tier and sel_tier != "ALL":
        df_filt = df_filt[df_filt["CHURN_RISK"] == sel_tier]

    # Pie — churn risk distribution
    counts = df["CHURN_RISK"].value_counts().reset_index()
    counts.columns = ["Tier", "Count"]
    tier_colors = {"HIGH RISK": RED, "MEDIUM RISK": AMBER, "STABLE": GREEN}
    fig_pie = px.pie(
        counts, names="Tier", values="Count",
        color="Tier", color_discrete_map=tier_colors,
        hole=0.55, title="Portfolio Risk Distribution",
    )
    fig_pie.update_layout(height=320, **_BASE)

    # Gauge — portfolio average risk score
    avg_risk = df["RISK_SCORE"].mean() if "RISK_SCORE" in df.columns else 0
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=round(avg_risk, 1),
        title={"text": "Avg Portfolio Risk Score", "font": {"color": TEXT}},
        gauge={
            "axis": {"range": [0, 100], "tickcolor": MUTED},
            "bar": {"color": RED if avg_risk >= 60 else (AMBER if avg_risk >= 30 else GREEN)},
            "bgcolor": SURFACE,
            "steps": [
                {"range": [0,  30], "color": "rgba(34,197,94,0.15)"},
                {"range": [30, 60], "color": "rgba(245,158,11,0.15)"},
                {"range": [60,100], "color": "rgba(239,68,68,0.15)"},
            ],
            "threshold": {"line": {"color": GOLD, "width": 2}, "value": avg_risk},
        },
        number={"font": {"color": TEXT}},
    ))
    fig_gauge.update_layout(height=320, **_BASE)

    # Risk table
    show_cols = ["MERCHANT_GROUP", "CLUSTER", "CHURN_RISK", "RISK_SCORE",
                 "ACHIEVEMENT_PCT", "SV_GROWTH_RATE", "WEEKS_ACTIVE", "PM"]
    present = [c for c in show_cols if c in df_filt.columns]
    risk_table = dash_table.DataTable(
        data=df_filt[present].sort_values("RISK_SCORE", ascending=False).round(2).to_dict("records"),
        columns=[{"name": c, "id": c} for c in present],
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=12,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": NAVY, "color": GOLD, "fontWeight": "bold"},
        style_cell={"backgroundColor": SURFACE, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "0.8rem", "padding": "6px 12px"},
        style_data_conditional=[
            {"if": {"filter_query": '{CHURN_RISK} = "HIGH RISK"'},
             "backgroundColor": "rgba(239,68,68,0.15)", "color": RED},
            {"if": {"filter_query": '{CHURN_RISK} = "MEDIUM RISK"'},
             "backgroundColor": "rgba(245,158,11,0.10)"},
        ],
    )

    # Drill-down accordion — one item per HIGH RISK merchant
    high_risk = df[df["CHURN_RISK"] == "HIGH RISK"] if "CHURN_RISK" in df.columns else df.head(0)
    accordion_items = []
    for _, row in high_risk.iterrows():
        merchant = row.get("MERCHANT_GROUP", "Unknown")
        risk_score = row.get("RISK_SCORE", 0)

        # Risk factor contribution bar (domain heuristics)
        factors = []
        if "ZSCORE_GROWTH" in row:
            factors.append({"Factor": "Declining Volume Trend", "Impact": max(0, -row["ZSCORE_GROWTH"])})
        if "ZSCORE_SV" in row:
            factors.append({"Factor": "Low Sales Volume",       "Impact": max(0, -row["ZSCORE_SV"])})
        if "ZSCORE_FBI" in row:
            factors.append({"Factor": "Low Fee Income",         "Impact": max(0, -row["ZSCORE_FBI"])})
        if "ACHIEVEMENT_PCT" in row:
            gap = max(0, 1 - row["ACHIEVEMENT_PCT"] / 100)
            factors.append({"Factor": "Target Gap",             "Impact": gap})

        import pandas as pd
        df_factors = pd.DataFrame(factors).sort_values("Impact", ascending=True)

        bar_colors = [RED if v > 0.5 else (AMBER if v > 0.2 else GREEN)
                      for v in df_factors["Impact"]]
        fig_factors = go.Figure(go.Bar(
            x=df_factors["Impact"], y=df_factors["Factor"],
            orientation="h",
            marker_color=bar_colors,
        ))
        fig_factors.update_layout(
            height=220, title="Risk Factor Contribution",
            **_BASE,
            xaxis=dict(showgrid=False, color=MUTED),
            yaxis=dict(showgrid=False, color=TEXT),
        )

        # IF feature contribution (if available)
        if_cols = [c for c in row.index if c.startswith("IF_CONTRIB_")]
        if_section = html.Div()
        if if_cols and row.get("IF_IS_ANOMALY", False):
            if_vals = {c.replace("IF_CONTRIB_", ""): row[c] for c in if_cols}
            if_vals = dict(sorted(if_vals.items(), key=lambda x: x[1], reverse=True))
            if_section = html.Div([
                html.P("Isolation Forest Anomaly Contribution:", className="text-warning small mb-1"),
                html.Pre(
                    "\n".join(f"  {k}: {v:+.4f}" for k, v in if_vals.items()),
                    className="text-light",
                    style={"fontSize": "0.75rem", "backgroundColor": NAVY, "padding": "8px",
                           "borderRadius": "4px"},
                ),
            ], className="mt-2")

        from dash import dcc as _dcc
        body = dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.Div([html.H6(f"{risk_score:.1f}", className="text-danger fw-bold mb-0"),
                                      html.P("Risk Score", className="text-muted small mb-0")]), width=4),
                    dbc.Col(html.Div([html.H6(f"{row.get('ACHIEVEMENT_PCT', 0):.1f}%",
                                              className="text-warning fw-bold mb-0"),
                                      html.P("Target Achievement", className="text-muted small mb-0")]), width=4),
                    dbc.Col(html.Div([html.H6(str(int(row.get("WEEKS_ACTIVE", 0))),
                                              className="text-info fw-bold mb-0"),
                                      html.P("Weeks Active", className="text-muted small mb-0")]), width=4),
                ], className="mb-3"),
                if_section,
            ], width=5),
            dbc.Col(
                _dcc.Graph(figure=fig_factors, config={"displayModeBar": False}),
                width=7,
            ),
        ])

        accordion_items.append(
            dbc.AccordionItem(
                body,
                title=f"{merchant}  |  Risk: {risk_score:.1f}  |  {row.get('CHURN_RISK', '')}",
                item_id=f"risk-{merchant}",
            )
        )

    return fig_pie, fig_gauge, risk_table, accordion_items
