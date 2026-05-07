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


# ── Action Inbox helpers (plan §4.2) ──────────────────────────────────────────


def compose_urgency_score(
    risk_score: ArrayLike,
    achievement_pct: ArrayLike = None,
    is_iforest_anomaly: ArrayLike = False,
    *,
    achievement_threshold: float = 60.0,
) -> ArrayLike:
    """Composite urgency score for sorting the Action Inbox (plan §4.2).

    Returns risk_score (0-100) plus two small bonuses:
      + 10 if Isolation Forest also flagged the merchant (multi-method confirmation)
      + min(10, achievement_threshold - achievement_pct) if below target

    Caps at 150 so a single feature can't blow up the ranking. Designed to
    surface "two-way confirmed" alerts above one-signal alerts of equal raw
    risk score, while keeping the score interpretable as "risk + a bit extra".

    Scalar in -> scalar out. Array/Series in -> Series out (preserves index
    so callers can do df.assign(URGENCY=compose_urgency_score(...))).
    """
    rs = pd.to_numeric(pd.Series([risk_score]) if np.ndim(risk_score) == 0
                       else pd.Series(risk_score), errors='coerce').fillna(0.0)

    ifa = pd.Series([is_iforest_anomaly]) if np.ndim(is_iforest_anomaly) == 0 \
          else pd.Series(is_iforest_anomaly)
    ifa = ifa.astype(bool, errors='ignore').fillna(False).astype(bool)

    if achievement_pct is None:
        ach = pd.Series([np.nan] * len(rs))
    elif np.ndim(achievement_pct) == 0:
        ach = pd.Series([achievement_pct] * len(rs))
    else:
        ach = pd.Series(achievement_pct)
    ach = pd.to_numeric(ach, errors='coerce')

    # Below-target bonus: capped at +10 so far-below-target merchants don't
    # drown out the underlying risk_score signal.
    below_gap = (achievement_threshold - ach).clip(lower=0).fillna(0)
    target_bonus = below_gap.clip(upper=10.0)

    # Multi-method confirmation bonus
    if_bonus = ifa.astype(float) * 10.0

    score = (rs + if_bonus.values + target_bonus.values).clip(0, 150)

    if np.ndim(risk_score) == 0:
        return float(score.iloc[0])
    score.index = pd.Series(risk_score).index if hasattr(risk_score, 'index') else score.index
    return score


def extract_recent_weeks(
    weekly_df: pd.DataFrame,
    *,
    merchant: str,
    dimensi: str,
    n_weeks: int = 12,
    merchant_col: str = 'MERCHANT_GROUP',
    dimensi_col: str = 'DIMENSI',
) -> list:
    """Extract the trailing N weeks of values for one merchant + DIMENSI.

    Used by the Action Inbox to render a sparkline next to each merchant card
    so the reader sees the actual trajectory, not just a static % number.

    Returns an empty list if:
      - the dataframe is empty
      - the merchant is not present
      - the row has no W-columns or all values are zero/null

    Parameters
    ----------
    weekly_df       PROCESSED_MONITORING_WEEKLY-shaped DataFrame with W1..W52 columns.
    merchant        Merchant identifier value to look up.
    dimensi         'VOL' / 'TRX' / 'FBI' to disambiguate multi-row merchants.
    n_weeks         How many trailing populated weeks to return.
    """
    if weekly_df is None or weekly_df.empty:
        return []
    if merchant_col not in weekly_df.columns or dimensi_col not in weekly_df.columns:
        return []

    # Find W-columns that exist in this dataframe, sorted numerically.
    w_cols = sorted(
        (c for c in weekly_df.columns
         if isinstance(c, str) and len(c) >= 2 and c.startswith('W') and c[1:].isdigit()),
        key=lambda c: int(c[1:]),
    )
    if not w_cols:
        return []

    matches = weekly_df[
        (weekly_df[merchant_col] == merchant) & (weekly_df[dimensi_col] == dimensi)
    ]
    if matches.empty:
        return []

    row = matches.iloc[0]
    series = pd.to_numeric(row[w_cols], errors='coerce').fillna(0.0)
    nonzero_idx = series[series > 0].index.tolist()
    if not nonzero_idx:
        return []

    # Slice from first non-zero week up to AND INCLUDING the last non-zero week
    # so legitimate mid-series zeros (a quiet week in the middle) survive in the
    # sparkline, but trailing zeros (no data yet for those weeks) are dropped.
    first_active = w_cols.index(nonzero_idx[0])
    last_active  = w_cols.index(nonzero_idx[-1])
    populated = series.iloc[first_active:last_active + 1].tolist()
    return populated[-n_weeks:]
