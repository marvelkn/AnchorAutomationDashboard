"""
Centralized ML computation callback.

Runs run_ml() exactly once whenever k or the group filter changes,
serializes the result to store-ml-result. All other ML/risk/overview
callbacks read from this store instead of re-fitting K-Means + Isolation
Forest on every interaction (eliminates 100–500 ms latency per click).
"""
from dash import callback, Output, Input

from services.data_service import load_card_share, load_monitoring, load_target
from services.ml_service import run_ml


@callback(
    Output("store-ml-result", "data"),
    Input("slider-k", "value"),
    Input("store-filter-group", "data"),
)
def compute_and_store_ml(k, sel_group):
    k = int(k or 3)
    df_c = load_card_share()
    df_m = load_monitoring()
    df_t = load_target()

    if sel_group and sel_group != "ALL GROUPS":
        if not df_c.empty and "MERCHANT_GROUP" in df_c.columns:
            df_c = df_c[df_c["MERCHANT_GROUP"] == sel_group]
        if not df_m.empty and "MERCHANT_GROUP" in df_m.columns:
            df_m = df_m[df_m["MERCHANT_GROUP"] == sel_group]

    df = run_ml(df_c, df_m, df_t if not df_t.empty else None, k_clusters=k)
    if df.empty:
        return None
    return df.to_json(date_format="iso", orient="split")
