"""
PM Manager page.
Maps to pages/05_PM_Manager.py from the Streamlit app.
Manage Portfolio Manager assignments.
"""
import dash
import dash_bootstrap_components as dbc
from dash import html, dash_table

dash.register_page(__name__, path="/pm-manager", name="PM Manager", order=3)

SURFACE = "#1A2538"
BORDER  = "#2A3A55"


def layout() -> html.Div:
    from services.data_service import load_monitoring, load_target
    df_mon = load_monitoring()
    df_tgt = load_target()

    # Build assignment table from monitoring + target merge
    import pandas as pd
    if not df_mon.empty and "MERCHANT_GROUP" in df_mon.columns and "PM" in df_mon.columns:
        df = df_mon[["MERCHANT_GROUP", "PM"]].drop_duplicates().copy()
        if not df_tgt.empty and "MERCHANT_GROUP" in df_tgt.columns:
            df = pd.merge(df, df_tgt[["MERCHANT_GROUP", "TARGET_VOL_2026"]], on="MERCHANT_GROUP", how="left")
    else:
        df = pd.DataFrame(columns=["MERCHANT_GROUP", "PM", "TARGET_VOL_2026"])

    records = df.to_dict("records")
    columns = [{"name": c, "id": c, "editable": c == "PM"} for c in df.columns]

    return html.Div(
        [
            html.H4("PM Manager", className="text-warning fw-bold mb-1"),
            html.P("Assign Portfolio Managers to merchant groups.",
                   className="text-secondary mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button(
                            [html.I(className="bi bi-floppy-fill me-2"), "Save Assignments"],
                            id="btn-pm-save",
                            color="warning",
                            size="sm",
                        ),
                        width="auto",
                    ),
                ],
                className="mb-3",
            ),
            html.Div(id="pm-save-status", className="mb-3"),

            dash_table.DataTable(
                id="table-pm",
                data=records,
                columns=columns,
                editable=True,
                filter_action="native",
                sort_action="native",
                page_action="native",
                page_size=20,
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#0D1520",
                    "color": "#F0BE48",
                    "fontWeight": "bold",
                    "borderBottom": f"2px solid {BORDER}",
                },
                style_cell={
                    "backgroundColor": SURFACE,
                    "color": "#EDF1F7",
                    "border": f"1px solid {BORDER}",
                    "fontSize": "0.8rem",
                    "padding": "6px 12px",
                },
            ),
        ],
        className="p-2",
    )
