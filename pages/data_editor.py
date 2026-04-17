"""
Master Records Editor page.
Maps to pages/01_Data_Editor.py from the Streamlit app.
Allows inline CRUD on the PROCESSED_MID table.
"""
import dash
import dash_bootstrap_components as dbc
from dash import html, dash_table, dcc

dash.register_page(__name__, path="/data-editor", name="Master Records Editor", order=2)

SURFACE = "#1A2538"
BORDER  = "#2A3A55"


def layout() -> html.Div:
    from services.data_service import load_mid
    df = load_mid()
    records = df.to_dict("records") if not df.empty else []
    columns = [{"name": c, "id": c, "editable": True} for c in df.columns] if not df.empty else []

    return html.Div(
        [
            html.H4("Master Records Editor", className="text-warning fw-bold mb-1"),
            html.P("Inline editor for the PROCESSED_MID merchant classification table.",
                   className="text-secondary mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button(
                            [html.I(className="bi bi-floppy-fill me-2"), "Save Changes"],
                            id="btn-mid-save",
                            color="warning",
                            size="sm",
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        dbc.Button(
                            [html.I(className="bi bi-x-circle me-2"), "Discard"],
                            id="btn-mid-discard",
                            color="outline-secondary",
                            size="sm",
                        ),
                        width="auto",
                    ),
                ],
                className="mb-3 g-2",
            ),

            html.Div(id="mid-save-status", className="mb-3"),

            dash_table.DataTable(
                id="table-mid",
                data=records,
                columns=columns,
                editable=True,
                filter_action="native",
                sort_action="native",
                page_action="native",
                page_size=25,
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
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#1E2E42",
                    }
                ],
            ),
        ],
        className="p-2",
    )
