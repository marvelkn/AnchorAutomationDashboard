"""
Global Settings page.
Maps to pages/0_Master_Configuration.py from the Streamlit app.
Upload / manage master Excel files; sync with Neon.
"""
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc

dash.register_page(__name__, path="/settings", name="Global Settings", order=4)

SURFACE = "#1A2538"
BORDER  = "#2A3A55"

_MASTER_FILES = [
    ("master_mid",        "Master MID",         ".xlsx"),
    ("master_card_share", "Master Card Share",   ".xlsx"),
    ("master_monitoring", "Master Monitoring",   ".xlsx"),
]


def _upload_card(file_key: str, label: str, accept: str) -> dbc.Card:
    return dbc.Card(
        [
            dbc.CardHeader(label, className="text-warning"),
            dbc.CardBody(
                [
                    dcc.Upload(
                        id=f"upload-{file_key}",
                        children=html.Div(
                            [html.I(className="bi bi-file-earmark-excel me-2"), f"Upload {label}"],
                            className="text-secondary",
                        ),
                        style={
                            "border": f"2px dashed {BORDER}",
                            "borderRadius": "8px",
                            "padding": "30px",
                            "textAlign": "center",
                        },
                        multiple=False,
                        accept=accept,
                    ),
                    html.Div(id=f"upload-{file_key}-status", className="mt-2 small"),
                ]
            ),
        ],
        style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
        className="mb-3",
    )


def layout() -> html.Div:
    return html.Div(
        [
            html.H4("Global Settings", className="text-warning fw-bold mb-1"),
            html.P("Upload master Excel files and manage cloud synchronisation.",
                   className="text-secondary mb-4"),

            dbc.Row(
                [dbc.Col(_upload_card(k, l, a), width=4) for k, l, a in _MASTER_FILES],
                className="g-3 mb-4",
            ),

            dbc.Card(
                [
                    dbc.CardHeader("Neon / Cloud Sync", className="text-warning"),
                    dbc.CardBody(
                        [
                            html.P("Push all master files to Neon PostgreSQL cloud storage.",
                                   className="text-secondary small"),
                            dbc.Button(
                                [html.I(className="bi bi-cloud-upload me-2"), "Sync to Neon"],
                                id="btn-sync-neon",
                                color="warning",
                                size="sm",
                            ),
                            html.Div(id="neon-sync-status", className="mt-3 small"),
                        ]
                    ),
                ],
                style={"backgroundColor": SURFACE, "border": f"1px solid {BORDER}"},
            ),
        ],
        className="p-2",
    )
