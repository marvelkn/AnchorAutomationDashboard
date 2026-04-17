"""
AnchorDash — Merchant Intelligence Platform
Plotly Dash + Dash Bootstrap Components rewrite of the Streamlit app.

Entry point:
    python app.py                (development)
    gunicorn wsgi:server         (production)
"""
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.sidebar import sidebar_layout

# ── Theme ─────────────────────────────────────────────────────────────────────
# SPACELAB provides a professional navy-compatible palette with strong contrast.
# Changing this one constant re-themes the entire app — no CSS injection needed.
THEME = dbc.themes.SPACELAB

app = dash.Dash(
    __name__,
    use_pages=True,                         # Auto-discovers pages/ folder (Dash 2.x)
    external_stylesheets=[
        THEME,
        dbc.icons.BOOTSTRAP,                # Bootstrap Icons (bi bi-*)
    ],
    suppress_callback_exceptions=True,      # Required for multi-page apps
    title="BTN Anchor Dashboard",
    update_title=None,                      # Don't flash "Updating..." in browser tab
)
server = app.server                         # Expose Flask server for WSGI/gunicorn

# ── Top-level Layout ──────────────────────────────────────────────────────────
# Sidebar (col-2) + page content (col-10).
# dcc.Store components live here so every page's callbacks can read/write them.
app.layout = dbc.Container(
    [
        dbc.Row(
            [
                # Sidebar
                dbc.Col(
                    sidebar_layout(),
                    width=2,
                    id="sidebar-col",
                    className="p-0",
                ),
                # Page content — Dash injects the active page's layout here
                dbc.Col(
                    dash.page_container,
                    width=10,
                    id="main-content",
                    className="p-4",
                    style={"backgroundColor": "#0D1520", "minHeight": "100vh"},
                ),
            ],
            className="g-0",
        ),
        # ── Global dcc.Store (replaces st.session_state) ───────────────────────
        # storage_type="session" → persists while browser tab is open; cleared on close.
        dcc.Store(id="store-filter-group",  storage_type="session"),
        dcc.Store(id="store-filter-brand",  storage_type="session"),
        dcc.Store(id="store-db-exists",     storage_type="session"),
        # Cached ML result — populated once by ml_store_callback, read by all ML/risk callbacks.
        dcc.Store(id="store-ml-result",     storage_type="session"),
    ],
    fluid=True,
    className="p-0",
    style={"backgroundColor": "#0D1520"},
)

# ── Register all callbacks ────────────────────────────────────────────────────
# Import callback modules so their @callback decorators execute.
# Keep this below app/layout definition to avoid circular imports.
import callbacks.nav_callbacks          # noqa: E402, F401
import callbacks.filter_callbacks       # noqa: E402, F401
import callbacks.card_share_callbacks   # noqa: E402, F401
import callbacks.monitoring_callbacks   # noqa: E402, F401
import callbacks.ml_store_callback      # noqa: E402, F401
import callbacks.ml_callbacks           # noqa: E402, F401
import callbacks.risk_callbacks         # noqa: E402, F401
import callbacks.forecast_callbacks     # noqa: E402, F401
import callbacks.overview_callbacks     # noqa: E402, F401


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
