"""
Filter callbacks — cascading Group → Brand dropdowns + global KPI strip.

These callbacks fire before the tab-specific ones and write results to
dcc.Store so every tab can read the current filter selection.
"""
import pandas as pd
from dash import callback, Input, Output, html
from services.data_service import load_card_share


# ── Populate Group dropdown from stored seed data ─────────────────────────────
@callback(
    Output("dd-group", "options"),
    Input("store-groups", "data"),
)
def set_group_options(groups):
    if not groups:
        return [{"label": "ALL GROUPS", "value": "ALL GROUPS"}]
    return [{"label": g, "value": g} for g in groups]


# ── Cascade: Group selection → Brand options ──────────────────────────────────
@callback(
    Output("dd-brand", "options"),
    Output("dd-brand", "value"),
    Input("dd-group", "value"),
)
def update_brand_options(sel_group: str):
    df = load_card_share()
    if df.empty or "MERCHANT_BRAND" not in df.columns:
        return [{"label": "ALL BRANDS", "value": "ALL BRANDS"}], "ALL BRANDS"
    if sel_group and sel_group != "ALL GROUPS":
        df = df[df["MERCHANT_GROUP"] == sel_group]
    brands = ["ALL BRANDS"] + sorted(df["MERCHANT_BRAND"].dropna().unique().tolist())
    return [{"label": b, "value": b} for b in brands], "ALL BRANDS"


# ── Write selections to dcc.Store (consumed by tab callbacks) ─────────────────
@callback(
    Output("store-filter-group", "data"),
    Output("store-filter-brand", "data"),
    Input("dd-group", "value"),
    Input("dd-brand", "value"),
)
def store_filters(group, brand):
    return group, brand


# ── Filter summary caption ────────────────────────────────────────────────────
@callback(
    Output("filter-summary", "children"),
    Input("dd-group", "value"),
    Input("dd-brand", "value"),
)
def update_filter_summary(group, brand):
    parts = []
    if group and group != "ALL GROUPS":
        parts.append(f"Group: {group}")
    if brand and brand != "ALL BRANDS":
        parts.append(f"Brand: {brand}")
    if parts:
        return f"Showing: {' · '.join(parts)}"
    return "Showing all merchants"


# ── Global (unfiltered) KPI strip ─────────────────────────────────────────────
@callback(
    Output("global-kpi-row", "children"),
    Input("interval-db-status", "n_intervals"),
)
def update_global_kpis(_n):
    from layouts.kpi_cards import kpi_card, kpi_row
    df = load_card_share()
    if df.empty:
        return html.P("No card share data loaded yet.", className="text-muted small")

    n_merchants = df["MERCHANT_GROUP"].nunique() if "MERCHANT_GROUP" in df.columns else 0
    ytd_sv  = df["TOTAL_SV"].sum()  if "TOTAL_SV"  in df.columns else 0
    ytd_trx = df["TOTAL_TRX"].sum() if "TOTAL_TRX" in df.columns else 0
    avg_onus = df["RASIO_ONUS"].mean() if "RASIO_ONUS" in df.columns else 0

    def _fmt(n):
        if n >= 1e12: return f"Rp {n/1e12:.1f}T"
        if n >= 1e9:  return f"Rp {n/1e9:.1f}B"
        if n >= 1e6:  return f"Rp {n/1e6:.1f}M"
        return f"Rp {n:,.0f}"

    return kpi_row([
        kpi_card(str(n_merchants),  "Merchants",        "warning"),
        kpi_card(_fmt(ytd_sv),      "YTD Sales Volume", "warning"),
        kpi_card(f"{ytd_trx:,.0f}", "YTD Transactions", "info"),
        kpi_card(f"{avg_onus:.1%}", "Avg On-Us Ratio",  "success"),
    ])


# ── Card Share year dropdown ───────────────────────────────────────────────────
@callback(
    Output("dd-cs-year", "options"),
    Output("dd-cs-year", "value"),
    Input("store-cs-years", "data"),
)
def set_cs_year_options(years):
    if not years:
        return [{"label": "All", "value": "All"}], "All"
    return [{"label": str(y), "value": str(y)} for y in years], str(years[0])


# ── Monitoring year dropdown ──────────────────────────────────────────────────
@callback(
    Output("dd-mon-year", "options"),
    Output("dd-mon-year", "value"),
    Input("store-mon-years", "data"),
)
def set_mon_year_options(years):
    if not years:
        return [], None
    opts = [{"label": str(y), "value": str(y)} for y in years]
    return opts, str(years[0])


# ── Monitoring PM dropdown (depends on year) ──────────────────────────────────
@callback(
    Output("dd-mon-pm", "options"),
    Output("dd-mon-pm", "value"),
    Input("dd-mon-year", "value"),
)
def update_mon_pm_options(sel_year):
    from services.data_service import load_monitoring_weekly
    df = load_monitoring_weekly()
    if df.empty or "PM" not in df.columns:
        return [{"label": "All PMs", "value": "All PMs"}], "All PMs"
    if sel_year:
        df = df[df["YEAR"].astype(str) == str(sel_year)]
    _exclude = {"NAN", "NONE", "UNKNOWN", "UNASSIGNED", ""}
    pms = ["All PMs"] + sorted([
        p for p in df["PM"].dropna().unique()
        if str(p).strip().upper() not in _exclude
    ])
    return [{"label": p, "value": p} for p in pms], "All PMs"
