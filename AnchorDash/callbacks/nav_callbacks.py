"""
Nav callbacks — sidebar DB status badge, refreshed every 60 s via dcc.Interval.

Maps to the Streamlit sidebar status strip logic in app.py.
"""
from dash import callback, Input, Output, html
from services.data_service import db_status, db_exists


@callback(
    Output("sidebar-db-status", "children"),
    Input("interval-db-status", "n_intervals"),
)
def update_db_badge(_n):
    if not db_exists():
        return html.Div(
            [html.I(className="bi bi-database-x me-2"), "No database"],
            className="text-danger small px-3 py-2 border border-danger rounded",
        )

    status = db_status()
    last = status.get("last_update", "Unknown")
    size = status.get("size_kb")
    size_str = f" · {size:,} KB" if size else ""

    # Parse freshness (mirrors Streamlit's stale badge logic)
    import datetime
    color = "secondary"
    icon = "bi-database-check"
    try:
        ts = datetime.datetime.fromisoformat(last.replace("Z", "+00:00"))
        age_h = (datetime.datetime.now(datetime.timezone.utc) - ts).total_seconds() / 3600
        if age_h < 24:
            color, icon = "success", "bi-database-check"
        elif age_h < 72:
            color, icon = "warning", "bi-database-exclamation"
        else:
            color, icon = "danger", "bi-database-x"
    except Exception:
        pass

    return html.Div(
        [
            html.I(className=f"bi {icon} me-1"),
            html.Span(f"{last}{size_str}", style={"fontSize": "0.7rem"}),
        ],
        className=f"text-{color} small px-3 py-2 border border-{color} rounded mt-2",
    )
