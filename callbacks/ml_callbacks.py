"""
ML / Segmentation tab callbacks.
Maps to Tab 3 (K-Means++) logic in pages/4_Dashboard.py.

Reads pre-computed ML results from store-ml-result (populated by
ml_store_callback) instead of re-fitting models on every interaction.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import callback, Input, Output, html, dash_table
from dash.exceptions import PreventUpdate

from services.data_service import load_monitoring

GOLD    = "#F0BE48"
NAVY    = "#0D1520"
SURFACE = "#1A2538"
BORDER  = "#2A3A55"
TEXT    = "#EDF1F7"
MUTED   = "#8A9BB5"

CLUSTER_COLORS = {
    "ELITE":   "#A855F7",
    "PREMIUM": "#22C55E",
    "REGULER": "#3B82F6",
    "PASIF":   "#EF4444",
    "DORMANT": "#6B7280",
}

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


# ── Populate ML PM dropdown ───────────────────────────────────────────────────
@callback(
    Output("dd-ml-pm", "options"),
    Input("store-filter-group", "data"),
)
def populate_ml_pm_dropdown(_group):
    df = load_monitoring()
    if df.empty or "PM" not in df.columns:
        return []
    _exclude = {"NAN", "NONE", "UNKNOWN", "UNASSIGNED", ""}
    pms = sorted([p for p in df["PM"].dropna().unique()
                  if str(p).strip().upper() not in _exclude])
    return [{"label": p, "value": p} for p in pms]


# ── Main ML charts ────────────────────────────────────────────────────────────
@callback(
    Output("chart-ml-3d",       "figure"),
    Output("chart-ml-pie",      "figure"),
    Output("chart-ml-box",      "figure"),
    Output("chart-ml-pm-stack", "figure"),
    Output("badge-silhouette",  "children"),
    Output("ml-segment-table",  "children"),
    Input("store-ml-result",    "data"),
    Input("dd-ml-pm",           "value"),
)
def update_ml_charts(ml_json, sel_pms):
    if not ml_json:
        raise PreventUpdate

    df = pd.read_json(ml_json, orient="split")

    if df.empty:
        empty = _empty()
        return empty, empty, empty, empty, "No data", html.P("No data.", className="text-muted small")

    # Apply PM filter post-ML
    if sel_pms and "PM" in df.columns:
        df = df[df["PM"].isin(sel_pms)]

    sil_val  = df["SILHOUETTE_SCORE"].iloc[0] if "SILHOUETTE_SCORE" in df.columns else 0.0
    sil_label = "Strong" if sil_val > 0.5 else ("Moderate" if sil_val > 0.25 else "Weak")
    badge_text = f"Silhouette: {sil_val:.3f} ({sil_label})"

    # Append best-k recommendation if available
    if "BEST_K" in df.columns and "BEST_K_SCORE" in df.columns:
        best_k      = int(df["BEST_K"].iloc[0])
        best_score  = float(df["BEST_K_SCORE"].iloc[0])
        badge_text += f" | Recommended k={best_k} (Score: {best_score:.3f})"

    # 3D Scatter
    fig_3d = px.scatter_3d(
        df, x="AVG_SV", y="AVG_FBI", z="SV_GROWTH_CLIPPED",
        color="CLUSTER", hover_name="MERCHANT_GROUP",
        color_discrete_map=CLUSTER_COLORS,
        title="Merchant Segmentation (3D)",
        labels={"AVG_SV": "Avg SV", "AVG_FBI": "Avg FBI", "SV_GROWTH_CLIPPED": "Growth"},
    )
    fig_3d.update_layout(height=460, **_BASE)

    # Pie
    cluster_counts = df["CLUSTER"].value_counts().reset_index()
    cluster_counts.columns = ["Cluster", "Count"]
    fig_pie = px.pie(
        cluster_counts, names="Cluster", values="Count",
        color="Cluster", color_discrete_map=CLUSTER_COLORS,
        hole=0.5, title="Cluster Distribution",
    )
    fig_pie.update_layout(height=340, **_BASE)

    # Box plot (risk score by cluster)
    fig_box = px.box(
        df, x="CLUSTER", y="RISK_SCORE",
        color="CLUSTER", color_discrete_map=CLUSTER_COLORS,
        title="Risk Score by Cluster",
    )
    fig_box.update_layout(height=320, showlegend=False, **_BASE)
    fig_box.update_xaxes(showgrid=False, color=MUTED)
    fig_box.update_yaxes(showgrid=True, gridcolor=BORDER, color=MUTED)

    # PM × Cluster stacked bar
    if "PM" in df.columns:
        pm_cluster = df.groupby(["PM", "CLUSTER"]).size().reset_index(name="Count")
        fig_stack = px.bar(
            pm_cluster, x="PM", y="Count", color="CLUSTER",
            barmode="stack", color_discrete_map=CLUSTER_COLORS,
            title="Merchants per PM by Cluster",
        )
        fig_stack.update_layout(height=320, legend=dict(orientation="h", y=-0.3), **_BASE)
        fig_stack.update_xaxes(showgrid=False, color=MUTED)
        fig_stack.update_yaxes(showgrid=True, gridcolor=BORDER, color=MUTED)
    else:
        fig_stack = _empty()

    # Summary table
    show_cols = ["MERCHANT_GROUP", "CLUSTER", "CHURN_RISK", "RISK_SCORE",
                 "AVG_SV", "ACHIEVEMENT_PCT", "WEEKS_ACTIVE", "PM"]
    present = [c for c in show_cols if c in df.columns]
    table = dash_table.DataTable(
        data=df[present].round(2).to_dict("records"),
        columns=[{"name": c, "id": c} for c in present],
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": NAVY, "color": GOLD, "fontWeight": "bold",
                      "fontSize": "0.75rem"},
        style_cell={"backgroundColor": SURFACE, "color": TEXT, "border": f"1px solid {BORDER}",
                    "fontSize": "0.8rem", "padding": "6px 12px"},
        style_data_conditional=[
            {"if": {"filter_query": '{CHURN_RISK} = "HIGH RISK"'},
             "backgroundColor": "rgba(239,68,68,0.15)"},
            {"if": {"filter_query": '{CHURN_RISK} = "MEDIUM RISK"'},
             "backgroundColor": "rgba(245,158,11,0.1)"},
        ],
    )

    return fig_3d, fig_pie, fig_box, fig_stack, badge_text, table
