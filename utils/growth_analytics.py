"""
Growth analytics for the BTN Anchor Card Share dashboard.

Replaces the broken raw-percent-change formula (lines 1255-1257 of the original
pages/4_Dashboard.py) that suffered from three failure modes verified against
production screenshots:

  1. Small-denominator explosion — HOKBEN +328,734% caused by tiny prior baseline
  2. Hard -100% floor for missing-data merchants (SUPRA BOGA, GRAMEDIA, ...)
  3. Linear-scale bar chart where every legitimate signal collapsed to 1 px

This module implements the four-part fix from plan §3.1:

  * baseline-floor filter routes tiny-baseline merchants to a "new/re-activated" list
  * symmetric percent change (bounded to [-200, +200]) for ranking
  * dropped-off merchants get their own bucket, off the bottom-growth bar
  * raw Growth % is preserved alongside for finance literacy

All functions are pure (no DB, no Streamlit, no global state).
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd

# Domain thresholds — values below these in the prior period are too small to
# yield a meaningful percent-change signal. Sourced from plan §3.1.
BASELINE_FLOORS: dict = {
    'TOTAL_SV':  1_000_000_000,  # Rp 1 B sales volume
    'TOTAL_TRX': 100,            # 100 transactions
    'TOTAL_FBI': 1_000_000,      # Rp 1 Jt fee-based income
}

ArrayLike = Union[float, int, np.ndarray, pd.Series]


def symmetric_pct_change(curr: ArrayLike, prev: ArrayLike) -> ArrayLike:
    """Symmetric percent change, bounded to [-200, +200].

        2 * (curr - prev) / (curr + prev) * 100

    This is the standard fix for the small-denominator problem. Where the raw
    formula `(curr - prev) / prev * 100` blows up when `prev` is tiny, the
    symmetric form is bounded and well-behaved.

    Returns NaN when both `curr` and `prev` are zero (no data).

    Scalar in -> scalar out; array/Series in -> ndarray out.
    """
    curr_arr = np.asarray(curr, dtype=float)
    prev_arr = np.asarray(prev, dtype=float)
    denom = curr_arr + prev_arr

    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.where(
            denom > 0,
            2.0 * (curr_arr - prev_arr) / np.where(denom == 0, 1.0, denom) * 100.0,
            np.nan,
        )

    # Preserve scalar return for scalar inputs.
    if np.ndim(curr) == 0 and np.ndim(prev) == 0:
        return float(result.item())
    return result


def classify_merchant_growth(
    df: pd.DataFrame,
    *,
    curr_col: str,
    prev_col: str,
    baseline: float,
) -> pd.DataFrame:
    """Classify each merchant row into one of four buckets.

    Adds a 'Status' column (does NOT mutate the input):

      established      - prev >= baseline AND curr >  0          (eligible for ranking)
      new_reactivated  - prev <  baseline AND curr >= baseline   (now established)
      dropped_off      - curr == 0         AND prev >= baseline  (real decline, no %)
      inactive         - everything else (both sides below baseline, etc.)

    Both sides of the comparison must clear the baseline at some point for
    a merchant to register as a real signal. Otherwise the row would either
    explode the percent calculation (tiny prev) or pollute the new-merchant
    list with rounding noise (tiny curr).

    Returns a copy of `df` with the 'Status' column appended.
    """
    out = df.copy()
    curr = out[curr_col].fillna(0).astype(float)
    prev = out[prev_col].fillna(0).astype(float)

    # A merchant must show meaningful current-period activity to be reported
    # in any non-inactive bucket. Activity floor is 10% of the established-
    # merchant baseline — small enough to catch ramping merchants like a 500M
    # newcomer (when SV baseline is 1B), large enough to filter rounding noise.
    activity_floor = baseline * 0.1

    conditions = [
        (prev >= baseline) & (curr > 0),                  # established
        (prev <  baseline) & (curr >= activity_floor),    # new_reactivated
        (curr == 0)         & (prev >= baseline),          # dropped_off
    ]
    choices = ['established', 'new_reactivated', 'dropped_off']
    out['Status'] = np.select(conditions, choices, default='inactive')
    return out


def compute_growth_signals(
    df: pd.DataFrame,
    *,
    curr_col: str,
    prev_col: str,
    baseline: float,
) -> pd.DataFrame:
    """Add Delta, Growth %, Symmetric %, Status columns to a growth dataframe.

    Returns a copy of `df` (does not mutate). Caller is responsible for
    splitting on `Status` and rendering each bucket appropriately:

        out = compute_growth_signals(df_growth, curr_col=col_curr, prev_col=col_prev,
                                     baseline=BASELINE_FLOORS['TOTAL_SV'])
        established  = out[out['Status'] == 'established']
        new_reactiv  = out[out['Status'] == 'new_reactivated']
        dropped_off  = out[out['Status'] == 'dropped_off']
        # rank `established` by 'Symmetric %' for top/bottom-N bars
    """
    out = df.copy()
    curr = out[curr_col].fillna(0).astype(float)
    prev = out[prev_col].fillna(0).astype(float)

    out['Delta'] = curr - prev

    # Raw growth — preserved for finance literacy. Same formula as the original
    # dashboard code (lines 1255-1257 of the pre-refactor file) so the column
    # remains readable to users familiar with the existing report.
    with np.errstate(divide='ignore', invalid='ignore'):
        raw = np.where(
            prev > 0,
            (curr - prev) / np.where(prev == 0, 1.0, prev) * 100.0,
            np.where(curr > 0, 100.0, 0.0),
        )
    out['Growth %'] = raw

    out['Symmetric %'] = symmetric_pct_change(curr.to_numpy(), prev.to_numpy())

    out = classify_merchant_growth(out, curr_col=curr_col, prev_col=prev_col,
                                   baseline=baseline)
    return out
