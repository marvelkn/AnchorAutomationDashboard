"""
Sidebar layout — rendered once at app startup inside app.py.

The sidebar is a fixed-width Bootstrap column (width=2) containing:
  - Brand logo + title
  - Navigation links (dbc.NavLink with active="exact" for auto-highlighting)
  - DB status badge (populated by nav_callbacks.py via dcc.Interval)
"""
import dash_bootstrap_components as dbc
from dash import html, dcc


# ── Color constants ───────────────────────────────────────────────────────────
# Keep these in sync with assets/custom.css CSS variables.
_GOLD = "#F0BE48"
_NAVY = "#0D1520"
_SURFACE = "#1A2538"


def sidebar_layout() -> html.Div:
    return html.Div(
        [
            # Brand header
            html.Div(
                [
                    html.Img(
                        src="/assets/btn_logo.png",
                        style={"height": "40px", "marginBottom": "8px"},
                    ),
                    html.P(
                        "Merchant Intelligence",
                        className="text-warning fw-bold mb-0",
                        style={"fontSize": "0.8rem", "letterSpacing": "0.05em"},
                    ),
                    html.P(
                        "Platform",
                        className="text-warning fw-bold mt-0",
                        style={"fontSize": "0.8rem", "letterSpacing": "0.05em"},
                    ),
                ],
                className="text-center py-3 border-bottom border-secondary",
            ),
            # Navigation groups
            html.Div(
                [
                    _nav_group(
                        "ANALYTICS",
                        [
                            _nav_link("bi bi-bar-chart-fill", "Dashboard", "/"),
                        ],
                    ),
                    _nav_group(
                        "PIPELINE",
                        [
                            _nav_link("bi bi-gear-fill", "Automated Pipeline", "/pipeline"),
                        ],
                    ),
                    _nav_group(
                        "DATA MANAGEMENT",
                        [
                            _nav_link("bi bi-table", "Master Records Editor", "/data-editor"),
                            _nav_link("bi bi-people-fill", "PM Manager", "/pm-manager"),
                        ],
                    ),
                    _nav_group(
                        "SETTINGS",
                        [
                            _nav_link("bi bi-sliders", "Global Settings", "/settings"),
                        ],
                    ),
                ],
                className="py-2",
            ),
            # DB status badge (updated by nav_callbacks.py every 60 s)
            html.Div(id="sidebar-db-status", className="px-3 pb-3 mt-auto"),
            # Interval to refresh DB status
            dcc.Interval(id="interval-db-status", interval=60_000, n_intervals=0),
        ],
        style={
            "backgroundColor": _SURFACE,
            "minHeight": "100vh",
            "display": "flex",
            "flexDirection": "column",
            "borderRight": "1px solid #2A3A55",
        },
    )


def _nav_group(label: str, links: list) -> html.Div:
    return html.Div(
        [
            html.P(
                label,
                className="text-secondary px-3 mb-1 mt-3",
                style={"fontSize": "0.65rem", "letterSpacing": "0.12em", "fontWeight": "600"},
            ),
            *links,
        ]
    )


def _nav_link(icon_class: str, label: str, href: str) -> dbc.NavLink:
    return dbc.NavLink(
        [html.I(className=f"{icon_class} me-2"), label],
        href=href,
        active="exact",
        className="px-3 py-2 text-light",
        style={"fontSize": "0.85rem"},
    )
