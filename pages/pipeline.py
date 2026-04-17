"""
Automated Pipeline page.
Maps to pages/00_Automated_Pipeline.py from the Streamlit app.
"""
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

dash.register_page(__name__, path="/pipeline", name="Automated Pipeline", order=1)

SURFACE = "#1A2538"
BORDER  = "#2A3A55"


def layout() -> html.Div:
    return html.Div(
        [
            html.H4("Automated Pipeline", className="text-warning fw-bold mb-1"),
            html.P("Upload your raw data files and trigger the ETL pipeline.",
                   className="text-secondary mb-4"),

            dbc.Row(
                [
                    # Upload panel
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Upload SQLite Database", className="text-warning"),
                                dbc.CardBody(
                                    [
                                        dcc.Upload(
                                            id="upload-sqlite-db",
                                            children=html.Div(
                                                [html.I(className="bi bi-cloud-upload me-2"),
                                                 "Drag & drop staging.db or click to browse"],
                                                className="text-secondary",
                                            ),
                                            style={
                                                "border": f"2px dashed {BORDER}",
                                                "borderRadius": "8px",
                                                "padding": "40px",
                                                "textAlign": "center",
                                            },
                                            multiple=False,
                                            accept=".db",
                                        ),
                                        html.Div(id="upload-sqlite-status", className="mt-3"),
                                    ]
                                ),
                            ],
                            style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
                        ),
                        width=6,
                    ),
                    # Status panel
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Pipeline Status", className="text-warning"),
                                dbc.CardBody(html.Div(id="pipeline-status-panel")),
                            ],
                            style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
                        ),
                        width=6,
                    ),
                ],
                className="g-3 mb-4",
            ),

            # Audit log
            dbc.Card(
                [
                    dbc.CardHeader("Recent Ingestion Runs", className="text-warning"),
                    dbc.CardBody(html.Div(id="pipeline-audit-log")),
                ],
                style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
            ),

            dcc.Interval(id="interval-pipeline", interval=5_000, n_intervals=0, disabled=True),
        ],
        className="p-2",
    )
