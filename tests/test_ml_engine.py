"""
Tests for utils.ml_engine — the BTN Anchor ML core (run_ml) and the
Holt-Winters forecast (hw_forecast).

These functions used to live inline in pages/4_Dashboard.py and could not be
tested (the page builds a DB engine at import). They were extracted verbatim
into utils/ml_engine.py; this suite locks in their contract.

Run:
    pytest tests/test_ml_engine.py -v
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.ml_engine import run_ml, hw_forecast, N_CLUSTERS, Z_THRESH, _HW_AVAILABLE

# Columns run_ml guarantees on every return value (the empty-frame contract).
_KEY_COLS = {
    "MERCHANT_GROUP", "CLUSTER", "CHURN_RISK", "RISK_SCORE",
    "ZSCORE_SV", "ZSCORE_FBI", "ZSCORE_GROWTH", "ACHIEVEMENT_PCT",
}
_VALID_TIERS = {"PREMIUM", "REGULER", "PASIF"}
_VALID_RISK = {"HIGH RISK", "MEDIUM RISK", "STABLE"}


# ── builders ──────────────────────────────────────────────────────────────────

def _portfolio(n, *, seed=0, sv=None, fbi=None, growth=None, weeks=40):
    """Build (df_card, df_mon) for n merchants. Scalars broadcast; arrays map 1:1."""
    rng = np.random.default_rng(seed)
    merchants = [f"M{i:02d}" for i in range(n)]

    def _col(val, default_lo, default_hi):
        if val is None:
            return rng.uniform(default_lo, default_hi, n)
        if np.isscalar(val):
            return np.full(n, val, dtype=float)
        return np.asarray(val, dtype=float)

    df_card = pd.DataFrame({
        "MERCHANT_GROUP": merchants,
        "TOTAL_SV":  _col(sv, 1e9, 5e10),
        "TOTAL_TRX": rng.integers(100, 5000, n).astype(float),
        "TOTAL_FBI": _col(fbi, 1e6, 5e8),
        "RASIO_ONUS": rng.uniform(0.1, 0.9, n),
    })
    df_mon = pd.DataFrame({
        "MERCHANT_GROUP": merchants,
        "WEEKS_ACTIVE": np.full(n, weeks, dtype=float),
        "SV_GROWTH_RATE": _col(growth, -0.3, 0.3),
        "PM": [f"PM{i % 3}" for i in range(n)],
    })
    return df_card, df_mon


# ── run_ml: structural contract ────────────────────────────────────────────────

class TestRunMlContract:
    def test_empty_card_returns_empty_frame_with_columns(self):
        out = run_ml(pd.DataFrame(), pd.DataFrame())
        assert len(out) == 0
        assert _KEY_COLS.issubset(set(out.columns))

    def test_small_portfolio_defaults_to_reguler(self):
        # Fewer than N_CLUSTERS merchants -> clustering is skipped, all REGULER.
        df_c, df_m = _portfolio(N_CLUSTERS - 1, seed=1)
        out = run_ml(df_c, df_m)
        assert len(out) == N_CLUSTERS - 1
        assert set(out["CLUSTER"]) == {"REGULER"}

    def test_normal_portfolio_tiers_and_bounds(self):
        df_c, df_m = _portfolio(12, seed=2)
        out = run_ml(df_c, df_m)
        assert len(out) == 12
        assert set(out["CLUSTER"]).issubset(_VALID_TIERS)
        assert set(out["CHURN_RISK"]).issubset(_VALID_RISK)
        assert out["RISK_SCORE"].between(0, 100).all()

    def test_deterministic_across_runs(self):
        df_c, df_m = _portfolio(12, seed=3)
        a = run_ml(df_c, df_m)
        b = run_ml(df_c, df_m)
        assert list(a["CLUSTER"]) == list(b["CLUSTER"])
        np.testing.assert_allclose(a["RISK_SCORE"].values, b["RISK_SCORE"].values)


# ── run_ml: scoring behaviour ──────────────────────────────────────────────────

class TestRunMlScoring:
    def test_declining_merchants_outrank_healthy_on_risk(self):
        # 6 strong (high SV/FBI, +growth) + 6 weak (low SV/FBI, -growth).
        sv = [5e10] * 6 + [1e9] * 6
        fbi = [5e8] * 6 + [1e6] * 6
        growth = [0.4] * 6 + [-0.4] * 6
        df_c, df_m = _portfolio(12, seed=4, sv=sv, fbi=fbi, growth=growth)
        out = run_ml(df_c, df_m).set_index("MERCHANT_GROUP")
        weak = out.loc[[f"M{i:02d}" for i in range(6, 12)], "RISK_SCORE"]
        strong = out.loc[[f"M{i:02d}" for i in range(0, 6)], "RISK_SCORE"]
        assert weak.mean() > strong.mean()

    def test_zscore_breach_never_stays_stable(self):
        # Contract: any merchant whose worst z-score breaches Z_THRESH must not
        # remain STABLE. A tight high-value majority keeps MAD small so the few
        # low-value outliers land far below the median -> guaranteed breach.
        sv = list(np.linspace(8e10, 1.2e11, 9)) + [5e7, 6e7, 7e7]
        fbi = list(np.linspace(3e8, 5e8, 9)) + [1e5, 2e5, 3e5]
        growth = [0.1] * 12
        df_c, df_m = _portfolio(12, seed=5, sv=sv, fbi=fbi, growth=growth)
        out = run_ml(df_c, df_m)
        worst_z = out[["ZSCORE_SV", "ZSCORE_FBI", "ZSCORE_GROWTH"]].min(axis=1)
        breached = out[worst_z < Z_THRESH]
        assert not breached.empty, "fixture should produce at least one breach"
        assert (breached["CHURN_RISK"] != "STABLE").all()

    def test_achievement_clips_at_200(self):
        df_c, df_m = _portfolio(6, seed=6, sv=2e10)
        # One merchant wildly over target -> achievement must clip at 200.
        df_t = pd.DataFrame({
            "MERCHANT_GROUP": df_c["MERCHANT_GROUP"],
            "TARGET_VOL_2026": [1e9, 1e11, 1e11, 1e11, 1e11, 1e11],
        })
        out = run_ml(df_c, df_m, df_t).set_index("MERCHANT_GROUP")
        assert out["ACHIEVEMENT_PCT"].max() <= 200
        assert out.loc["M00", "ACHIEVEMENT_PCT"] == pytest.approx(200.0)


# ── hw_forecast ────────────────────────────────────────────────────────────────

def _monthly_history(n_months, *, start=202301, base=1e10, slope=2e8):
    months, y, m = [], start // 100, start % 100
    for _ in range(n_months):
        months.append(y * 100 + m)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    values = [base + slope * i for i in range(n_months)]
    return pd.DataFrame({"TRX_MONTH": months, "TOTAL_SV": values})


class TestHwForecast:
    def test_no_data_fails_gracefully(self):
        res = hw_forecast(pd.DataFrame())
        assert res["success"] is False
        assert res["reason"]

    def test_too_few_months_fails(self):
        res = hw_forecast(_monthly_history(3))
        assert res["success"] is False
        assert "month" in res["reason"].lower()

    @pytest.mark.skipif(not _HW_AVAILABLE, reason="statsmodels not installed")
    def test_clean_series_forecasts(self):
        res = hw_forecast(_monthly_history(30), periods_ahead=12)
        assert res["success"] is True
        assert len(res["forecast"]) == 12
        assert len(res["lower"]) == 12 and len(res["upper"]) == 12
        assert np.all(res["forecast"] >= 0)
        assert np.all(res["lower"] <= res["forecast"] + 1e-6)
        assert np.all(res["forecast"] <= res["upper"] + 1e-6)
        assert res["projected_eoy"] == pytest.approx(float(np.sum(res["forecast"])))
