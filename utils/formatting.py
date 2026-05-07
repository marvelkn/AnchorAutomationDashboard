"""
Centralized human-readable number formatters for the BTN Anchor dashboard.

Replaces inline lambdas scattered across pages/4_Dashboard.py. Eliminates raw
6-decimal numbers like 28927934.7233 from user-facing tables.

Convention used by this codebase (Indonesian banking):
    Jt = juta     (1e6)
    M  = miliar   (1e9)
    rb = ribu     (1e3)
"""

from __future__ import annotations

import math
from typing import Optional, Union

Number = Union[int, float, None]

NA_DISPLAY = "—"


def _is_missing(value: Number) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return True


def _strip_trailing_zeros(formatted: str) -> str:
    if "." not in formatted:
        return formatted
    stripped = formatted.rstrip("0").rstrip(".")
    return stripped if stripped else "0"


# Currency
def fmt_currency_idr(value: Number, *, compact: bool = False) -> str:
    """Format an IDR amount for display.

    Default mode uses Indonesian banking units (rb / Jt / M).
    Compact mode uses K / M / B for tight columns.

        fmt_currency_idr(28_927_934)        -> "Rp 28.9 Jt"
        fmt_currency_idr(1_927_000_000)     -> "Rp 1.93 M"
        fmt_currency_idr(749_090)           -> "Rp 749 rb"
        fmt_currency_idr(28_900_000, compact=True) -> "Rp 28.9M"
    """
    if _is_missing(value):
        return NA_DISPLAY

    v = float(value)
    sign = "-" if v < 0 else ""
    a = abs(v)

    if compact:
        if a >= 1e9:
            body = f"{a / 1e9:.2f}B"
        elif a >= 1e6:
            body = f"{a / 1e6:.1f}M"
        elif a >= 1e3:
            body = f"{a / 1e3:.0f}K"
        else:
            body = f"{a:.0f}"
        return f"{sign}Rp {body}"

    if a >= 1e9:
        body = f"{a / 1e9:,.2f} M"
    elif a >= 1e6:
        body = f"{a / 1e6:.1f} Jt"
    elif a >= 1e3:
        body = f"{a / 1e3:.0f} rb"
    else:
        body = f"{a:.0f}"
    return f"{sign}Rp {body}"


# Counts
def fmt_count(value: Number, *, decimals: Optional[int] = None) -> str:
    """Format a count with K / M / B suffixes.

        fmt_count(8_810_000)              -> "8.81 M"
        fmt_count(1_500)                  -> "1.5 K"
        fmt_count(8_810_000, decimals=0)  -> "9 M"
    """
    if _is_missing(value):
        return NA_DISPLAY

    v = float(value)
    sign = "-" if v < 0 else ""
    a = abs(v)

    if a < 1e3:
        return f"{sign}{int(round(a))}"

    if a >= 1e9:
        scaled, suffix = a / 1e9, " B"
    elif a >= 1e6:
        scaled, suffix = a / 1e6, " M"
    else:
        scaled, suffix = a / 1e3, " K"

    if decimals is None:
        body = _strip_trailing_zeros(f"{scaled:.2f}")
    else:
        body = f"{scaled:.{decimals}f}"
    return f"{sign}{body}{suffix}"


# Percent (share)
def fmt_pct(value: Number, *, decimals: int = 1, scale: bool = True) -> str:
    """Format a share / percentage value.

        fmt_pct(0.252)                    -> "25.2%"
        fmt_pct(0.252, decimals=0)        -> "25%"
        fmt_pct(25.2, scale=False)        -> "25.2%"
    """
    if _is_missing(value):
        return NA_DISPLAY
    v = float(value) * (100.0 if scale else 1.0)
    return f"{v:.{decimals}f}%"


# Growth (signed percent with optional cap)
def fmt_growth(
    value: Number,
    *,
    decimals: int = 1,
    scale: bool = True,
    cap: Optional[float] = None,
) -> str:
    """Format a signed growth rate. Always shows + on positives.

        fmt_growth(0.1153)                -> "+11.5%"
        fmt_growth(-0.32)                 -> "-32.0%"
        fmt_growth(3287, scale=False, cap=200) -> "> +200%"
        fmt_growth(-500, scale=False, cap=200) -> "< -200%"
    """
    if _is_missing(value):
        return NA_DISPLAY
    v = float(value) * (100.0 if scale else 1.0)

    if cap is not None and v > cap:
        return f"> +{cap:.0f}%"
    if cap is not None and v < -cap:
        return f"< -{cap:.0f}%"

    if v == 0:
        return f"{v:.{decimals}f}%"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.{decimals}f}%"


# Z-score
def fmt_zscore(value: Number) -> str:
    """Two decimals, always."""
    if _is_missing(value):
        return NA_DISPLAY
    return f"{float(value):.2f}"


# Cell-styling helpers for pandas Styler
def _diverging_rgb(t: float) -> str:
    """Diverging red/white/green from t in [-1, 1]. Returns 'rgb(r,g,b)' string."""
    t = max(-1.0, min(1.0, t))
    if t >= 0:
        # white (255,255,255) to green (16,185,129)
        r = int(round(255 + (16 - 255) * t))
        g = int(round(255 + (185 - 255) * t))
        b = int(round(255 + (129 - 255) * t))
    else:
        # white to red (239,68,68)
        s = -t
        r = int(round(255 + (239 - 255) * s))
        g = int(round(255 + (68 - 255) * s))
        b = int(round(255 + (68 - 255) * s))
    return f"rgb({r},{g},{b})"


def zscore_cell_style(value: Number, *, scale: float = 3.0) -> str:
    """CSS for a pandas Styler cell. Diverges red/white/green by Z magnitude.

    A |Z| of `scale` saturates the color. Default 3.0 matches the Health Alerts
    Z-threshold convention.
    """
    if _is_missing(value):
        return ""
    v = float(value)
    if abs(v) < 0.5:
        return ""
    t = max(-1.0, min(1.0, v / scale))
    return f"background-color: {_diverging_rgb(t)};"


def growth_cell_style(value: Number, *, scale: float = 0.5) -> str:
    """CSS for a pandas Styler cell. Diverges red/white/green at zero.

    A growth rate of `scale` (default 0.5 = +50%) saturates the color.
    Pass `scale` in the same form as `value` (fraction or percent).
    """
    if _is_missing(value):
        return ""
    v = float(value)
    if abs(v) < 1e-9:
        return ""
    t = max(-1.0, min(1.0, v / scale))
    return f"background-color: {_diverging_rgb(t)};"
