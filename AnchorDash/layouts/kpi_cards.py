"""
KPI card helpers — replaces utils/theme.py kpi_card() / kpi_row().

Uses native DBC / Bootstrap utilities — no raw HTML injection needed.
color = Bootstrap contextual color name: "warning" (gold), "danger", "success", "info"
"""
import dash_bootstrap_components as dbc
from dash import html


def kpi_card(value: str, label: str, color: str = "warning") -> dbc.Card:
    """Single KPI card with a coloured top border."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.H4(
                    value,
                    className=f"text-{color} fw-bold font-monospace mb-1",
                    style={"fontSize": "1.5rem"},
                ),
                html.P(
                    label,
                    className="text-secondary text-uppercase mb-0",
                    style={"fontSize": "0.7rem", "letterSpacing": "0.08em"},
                ),
            ],
            className="p-3",
        ),
        className=f"border-top border-{color} border-3 h-100",
        style={"backgroundColor": "#1A2538"},
    )


def kpi_row(cards: list[dbc.Card]) -> dbc.Row:
    """Lay out KPI cards in an equal-width Bootstrap row."""
    return dbc.Row(
        [dbc.Col(c, className="mb-3") for c in cards],
        className="g-3 mb-4",
    )
