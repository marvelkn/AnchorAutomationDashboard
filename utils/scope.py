"""
Shared "Scope" filter helpers.

Single source of truth for the Merchant Group / Anchor scope picker that
appears in two surfaces:
  - Sidebar (desktop, >900px) — controls live in `with st.sidebar:` from 4_Dashboard.py
  - Sticky button + dialog (mobile/tablet, <=900px) — opens an `st.dialog`

Both surfaces bind to the same session_state keys (`sb_group`, `sb_brand`)
so the active scope is consistent regardless of which control was used.
"""
from __future__ import annotations

from typing import Iterable, Tuple, Callable
import pandas as pd
import streamlit as st


ALL_GROUPS_LABEL = "ALL GROUPS"
TOTAL_GROUP_LABEL = "TOTAL GROUP"
TOTAL_PORTFOLIO_LABEL = "TOTAL PORTFOLIO"


def get_scope_options(df_card: pd.DataFrame, sel_group: str) -> Tuple[list, list]:
    """Return (groups, brands) selectbox options for the current group selection."""
    groups: list = [ALL_GROUPS_LABEL]
    if df_card is not None and not df_card.empty and "MERCHANT_GROUP" in df_card.columns:
        groups += sorted(df_card["MERCHANT_GROUP"].dropna().unique().tolist())

    if sel_group == ALL_GROUPS_LABEL or df_card is None or df_card.empty:
        brands = [TOTAL_PORTFOLIO_LABEL]
    else:
        brands = [TOTAL_GROUP_LABEL]
        if "MERCHANT_ANCHOR" in df_card.columns:
            anchors = (
                df_card[df_card["MERCHANT_GROUP"] == sel_group]["MERCHANT_ANCHOR"]
                .dropna()
                .unique()
                .tolist()
            )
            brands += sorted(anchors)
    return groups, brands


def render_scope_controls(df_card: pd.DataFrame, *, key_prefix: str = "") -> Tuple[str, str]:
    """Render two cascading selectboxes + a Reset Scope button.

    Returns the current (sel_group, sel_brand). Both selectboxes bind to
    `st.session_state` via the canonical keys `sb_group` and `sb_brand`,
    so scope is shared across every render surface.
    """
    sel_group_default = st.session_state.get("sb_group", ALL_GROUPS_LABEL)
    groups, _ = get_scope_options(df_card, sel_group_default)
    if sel_group_default not in groups:
        st.session_state["sb_group"] = ALL_GROUPS_LABEL

    group_key = f"{key_prefix}sb_group" if key_prefix else "sb_group"
    sel_group = st.selectbox("Merchant Group", groups, key=group_key)

    _, brands = get_scope_options(df_card, sel_group)
    cur_brand = st.session_state.get("sb_brand", brands[0])
    if cur_brand not in brands:
        st.session_state["sb_brand"] = brands[0]

    brand_key = f"{key_prefix}sb_brand" if key_prefix else "sb_brand"
    sel_brand = st.selectbox("Merchant Brand (Anchor)", brands, key=brand_key)

    reset_key = f"{key_prefix}reset_scope" if key_prefix else "reset_scope"
    if st.button("Reset Scope", key=reset_key, use_container_width=True):
        st.session_state["sb_group"] = ALL_GROUPS_LABEL
        st.session_state["sb_brand"] = TOTAL_PORTFOLIO_LABEL
        st.rerun()

    return sel_group, sel_brand


def is_default_scope(sel_group: str, sel_brand: str) -> bool:
    return sel_group == ALL_GROUPS_LABEL and sel_brand in (
        TOTAL_GROUP_LABEL,
        TOTAL_PORTFOLIO_LABEL,
    )


def scope_breadcrumb_html(sel_group: str, sel_brand: str) -> str:
    """Compact read-only chip showing active scope. Rendered on the dashboard
    header (desktop, since controls live in the sidebar) and as the mobile
    sticky button label."""
    g = sel_group or ALL_GROUPS_LABEL
    b = sel_brand or TOTAL_PORTFOLIO_LABEL
    cls = "scope-breadcrumb" + (
        " is-default" if is_default_scope(g, b) else " is-active"
    )
    return (
        f'<div class="{cls}">'
        f'<span class="scope-bc-label">Scope</span>'
        f'<span class="scope-bc-sep">&rsaquo;</span>'
        f'<span class="scope-bc-val">{g}</span>'
        f'<span class="scope-bc-sep">&rsaquo;</span>'
        f'<span class="scope-bc-val">{b}</span>'
        f"</div>"
    )


def apply_scope(
    df: pd.DataFrame,
    sel_group: str,
    sel_brand: str,
    *,
    group_col: str = "MERCHANT_GROUP",
    brand_col: str = "MERCHANT_ANCHOR",
) -> pd.DataFrame:
    """Filter a DataFrame by current scope. Pure function — returns a new frame.

    Skips filtering on missing columns or empty frames so it's safe to call on
    any tab's source df.
    """
    if df is None or df.empty:
        return df
    out = df
    if sel_group and sel_group != ALL_GROUPS_LABEL and group_col in out.columns:
        out = out[out[group_col] == sel_group]
    if (
        sel_brand
        and sel_brand not in (TOTAL_GROUP_LABEL, TOTAL_PORTFOLIO_LABEL)
        and brand_col in out.columns
    ):
        out = out[out[brand_col] == sel_brand]
    return out


def responsive_chart_height(desktop_px: int) -> int:
    """Cap a chart's pixel height for use across viewports.

    Plotly's container can shrink on mobile thanks to theme.py's
    `[data-testid="stPlotlyChart"] > div { min-height: 0 }` rule at <=640px,
    so we just return the desktop value here and let CSS do the work.
    Centralising this keeps a single knob if we change strategy later.
    """
    return int(desktop_px)


def mobile_swap_to_tabs(label_pairs: Iterable[Tuple[str, Callable[[], None]]]) -> None:
    """Render `[(label, render_fn), ...]` as a 2-tab switcher on mobile,
    or as side-by-side columns on desktop.

    Implementation: always render `st.tabs(...)`. Wrapped in a div whose CSS
    flattens the tab UI into a flex row at >900px so desktop sees both panels
    side-by-side, while mobile sees the native tab switcher.
    """
    label_pairs = list(label_pairs)
    if not label_pairs:
        return

    st.markdown('<div class="swap-tabs-mobile">', unsafe_allow_html=True)
    tabs = st.tabs([lbl for lbl, _ in label_pairs])
    for tab, (_, fn) in zip(tabs, label_pairs):
        with tab:
            fn()
    st.markdown("</div>", unsafe_allow_html=True)
