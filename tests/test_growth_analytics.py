"""
Tests for utils.growth_analytics — fix the broken Top/Bottom Growth chart.

Plan reference: act-as-a-lead-elegant-blossom.md §3.1
Goal: replace raw `(curr - prev) / prev` formula that explodes on small
denominators (HOKBEN +328,734%) and floors to -100% on missing data
(SUPRA BOGA, GRAMEDIA, etc.) with:
  1. baseline-floor filter
  2. symmetric percent change for ranking
  3. four-bucket classification (established / new_reactivated / dropped_off / inactive)

Run:
    pytest tests/test_growth_analytics.py -v
"""

import math
import os
import sys

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.growth_analytics import (
    BASELINE_FLOORS,
    classify_merchant_growth,
    compose_urgency_score,
    compute_growth_signals,
    extract_recent_weeks,
    symmetric_pct_change,
)


# symmetric_pct_change — bounded growth metric
class TestSymmetricPctChange:
    def test_double_is_plus_66_67(self):
        # 100 -> 200: standard formula gives +100%, symmetric gives +66.67%
        assert symmetric_pct_change(200, 100) == pytest.approx(66.6666, abs=0.01)

    def test_halve_is_minus_66_67(self):
        # symmetric to the doubling case
        assert symmetric_pct_change(100, 200) == pytest.approx(-66.6666, abs=0.01)

    def test_no_change(self):
        assert symmetric_pct_change(100, 100) == pytest.approx(0.0)

    def test_zero_prev_caps_at_plus_200(self):
        # brand new merchant: prev=0, curr>0 -> +200% (the upper bound)
        assert symmetric_pct_change(100, 0) == pytest.approx(200.0)

    def test_zero_curr_caps_at_minus_200(self):
        # dropped off: prev>0, curr=0 -> -200% (lower bound)
        assert symmetric_pct_change(0, 100) == pytest.approx(-200.0)

    def test_both_zero_returns_nan(self):
        assert math.isnan(symmetric_pct_change(0, 0))

    def test_huge_jump_is_bounded_below_200(self):
        # this is the key fix — HOKBEN-style outlier is bounded
        result = symmetric_pct_change(1_000_000_000, 100)
        assert -200 < result <= 200
        assert result > 199  # near max but not exploding

    def test_array_inputs_return_array(self):
        curr = np.array([200, 100, 100, 0])
        prev = np.array([100, 200, 100, 100])
        result = symmetric_pct_change(curr, prev)
        assert isinstance(result, np.ndarray)
        assert result[0] == pytest.approx(66.666, abs=0.01)
        assert result[1] == pytest.approx(-66.666, abs=0.01)
        assert result[2] == pytest.approx(0.0)
        assert result[3] == pytest.approx(-200.0)

    def test_symmetry_property(self):
        # f(a, b) == -f(b, a) for all a, b > 0
        assert symmetric_pct_change(150, 100) == pytest.approx(-symmetric_pct_change(100, 150))


# BASELINE_FLOORS — domain thresholds
class TestBaselineFloors:
    def test_keys_match_dashboard_columns(self):
        assert 'TOTAL_SV' in BASELINE_FLOORS
        assert 'TOTAL_TRX' in BASELINE_FLOORS
        assert 'TOTAL_FBI' in BASELINE_FLOORS

    def test_thresholds_are_positive(self):
        for k, v in BASELINE_FLOORS.items():
            assert v > 0, f"{k} threshold must be positive"

    def test_sv_threshold_is_one_billion_idr(self):
        assert BASELINE_FLOORS['TOTAL_SV'] == 1_000_000_000

    def test_trx_threshold_is_100(self):
        assert BASELINE_FLOORS['TOTAL_TRX'] == 100

    def test_fbi_threshold_is_one_juta(self):
        assert BASELINE_FLOORS['TOTAL_FBI'] == 1_000_000


# classify_merchant_growth — 4-bucket router
class TestClassifyMerchantGrowth:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'MERCHANT_GROUP': ['ESTABLISHED_GROWING', 'ESTABLISHED_DECLINING', 'NEW_CO',
                               'DROPPED_OFF', 'TINY_BOTH', 'ALL_ZERO'],
            'curr': [28_900_000_000, 25_000_000_000, 500_000_000, 0,             100, 0],
            'prev': [27_000_000_000, 30_000_000_000, 0,           2_000_000_000, 100, 0],
        })

    def test_established_merchants_pass_baseline(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        established = set(out[out['Status'] == 'established']['MERCHANT_GROUP'])
        assert 'ESTABLISHED_GROWING' in established
        assert 'ESTABLISHED_DECLINING' in established

    def test_new_company_classified_as_new_reactivated(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        new_re = set(out[out['Status'] == 'new_reactivated']['MERCHANT_GROUP'])
        assert 'NEW_CO' in new_re
        # NEW_CO had prev=0, curr=500M; this should NOT pollute the established list

    def test_dropped_off_classified_correctly(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        dropped = set(out[out['Status'] == 'dropped_off']['MERCHANT_GROUP'])
        assert 'DROPPED_OFF' in dropped
        # DROPPED_OFF must NOT be in established (it would pin -200% / -100%)

    def test_tiny_both_periods_is_inactive(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        inactive = set(out[out['Status'] == 'inactive']['MERCHANT_GROUP'])
        assert 'TINY_BOTH' in inactive
        assert 'ALL_ZERO' in inactive

    def test_every_row_gets_exactly_one_status(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        assert len(out) == len(sample_df)
        assert set(out['Status'].unique()).issubset(
            {'established', 'new_reactivated', 'dropped_off', 'inactive'}
        )

    def test_no_merchant_gets_double_counted(self, sample_df):
        out = classify_merchant_growth(sample_df, curr_col='curr', prev_col='prev',
                                       baseline=BASELINE_FLOORS['TOTAL_SV'])
        assert len(out['MERCHANT_GROUP']) == len(set(out['MERCHANT_GROUP']))


# compute_growth_signals — adds Delta, Growth %, Symmetric % columns
class TestComputeGrowthSignals:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'MERCHANT_GROUP': ['HOKBEN_LIKE', 'NORMAL', 'GRAMEDIA_LIKE'],
            'curr': [328_000_000, 28_900_000_000, 0],
            'prev': [100_000,     27_000_000_000, 2_000_000_000],
        })

    def test_adds_required_columns(self, sample_df):
        out = compute_growth_signals(sample_df, curr_col='curr', prev_col='prev',
                                     baseline=BASELINE_FLOORS['TOTAL_SV'])
        for col in ['Delta', 'Growth %', 'Symmetric %', 'Status']:
            assert col in out.columns, f"missing column {col}"

    def test_hokben_like_gets_classified_not_ranked_at_top(self, sample_df):
        out = compute_growth_signals(sample_df, curr_col='curr', prev_col='prev',
                                     baseline=BASELINE_FLOORS['TOTAL_SV'])
        hokben = out[out['MERCHANT_GROUP'] == 'HOKBEN_LIKE'].iloc[0]
        # raw growth is +328,000,000% which would dominate Top 10
        assert hokben['Growth %'] > 100_000  # raw still huge
        # but symmetric is bounded
        assert -200 <= hokben['Symmetric %'] <= 200
        # and status routes it OUT of the established bucket
        assert hokben['Status'] != 'established'

    def test_normal_merchant_has_sensible_symmetric_pct(self, sample_df):
        out = compute_growth_signals(sample_df, curr_col='curr', prev_col='prev',
                                     baseline=BASELINE_FLOORS['TOTAL_SV'])
        normal = out[out['MERCHANT_GROUP'] == 'NORMAL'].iloc[0]
        # 27B -> 28.9B: roughly +6.8% raw, slightly less symmetric
        assert normal['Status'] == 'established'
        assert -10 < normal['Symmetric %'] < 10
        assert -10 < normal['Growth %'] < 10

    def test_gramedia_like_is_dropped_off_not_minus_100(self, sample_df):
        out = compute_growth_signals(sample_df, curr_col='curr', prev_col='prev',
                                     baseline=BASELINE_FLOORS['TOTAL_SV'])
        gram = out[out['MERCHANT_GROUP'] == 'GRAMEDIA_LIKE'].iloc[0]
        assert gram['Status'] == 'dropped_off'
        # the raw growth is -100% but the row is no longer in 'established' so
        # the Bottom-10 chart will not anchor its scale at -100% anymore.

    def test_does_not_mutate_input(self, sample_df):
        original = sample_df.copy()
        _ = compute_growth_signals(sample_df, curr_col='curr', prev_col='prev',
                                   baseline=BASELINE_FLOORS['TOTAL_SV'])
        pd.testing.assert_frame_equal(sample_df, original)


# compose_urgency_score — Action Inbox sort key (plan §4.2)
class TestComposeUrgencyScore:
    def test_baseline_score_passthrough(self):
        # No bonuses, no penalties: returns the risk score itself.
        assert compose_urgency_score(50.0) == pytest.approx(50.0)
        assert compose_urgency_score(0.0)  == pytest.approx(0.0)
        assert compose_urgency_score(95.0) == pytest.approx(95.0)

    def test_iforest_anomaly_adds_bonus(self):
        # Multi-method confirmation should bubble these merchants up.
        base = compose_urgency_score(50.0)
        with_if = compose_urgency_score(50.0, is_iforest_anomaly=True)
        assert with_if > base
        assert with_if - base == pytest.approx(10.0)

    def test_below_target_adds_proportional_bonus(self):
        # Achievement at 40% (below 60% threshold) should add a 20-point bonus,
        # capped at +10 so it can't drown out the risk_score.
        score = compose_urgency_score(50.0, achievement_pct=40.0)
        assert score > 50.0
        # bonus = min(10, 60 - 40) = 10
        assert score == pytest.approx(60.0)

    def test_below_target_bonus_capped(self):
        # Even achievement = 0% gives no more than +10 bonus.
        score = compose_urgency_score(50.0, achievement_pct=0.0)
        assert score == pytest.approx(60.0)

    def test_above_target_no_bonus(self):
        # Achievement well above threshold: no penalty for hitting target.
        score = compose_urgency_score(50.0, achievement_pct=120.0)
        assert score == pytest.approx(50.0)

    def test_combined_bonuses_stack(self):
        # IF=True + below-target should both apply.
        score = compose_urgency_score(50.0, achievement_pct=30.0,
                                      is_iforest_anomaly=True)
        # 50 + 10 (IF) + 10 (target) = 70
        assert score == pytest.approx(70.0)

    def test_clamps_at_150(self):
        # Even maxed risk + both bonuses can't exceed the upper bound.
        score = compose_urgency_score(200.0, achievement_pct=0.0,
                                      is_iforest_anomaly=True)
        assert score <= 150.0

    def test_handles_nan_risk_score(self):
        import math
        assert compose_urgency_score(float("nan")) == pytest.approx(0.0)
        assert compose_urgency_score(None) == pytest.approx(0.0)

    def test_handles_nan_achievement(self):
        # NaN achievement should be treated as "no signal" not "below target".
        import math
        score = compose_urgency_score(50.0, achievement_pct=float("nan"))
        assert score == pytest.approx(50.0)

    def test_array_inputs(self):
        # Works on Series/array inputs for vectorized sorting.
        risk = pd.Series([10.0, 50.0, 90.0])
        ach  = pd.Series([100.0, 40.0, 0.0])
        ifa  = pd.Series([False, False, True])
        out = compose_urgency_score(risk, achievement_pct=ach, is_iforest_anomaly=ifa)
        assert hasattr(out, '__len__')
        assert len(out) == 3
        assert out.iloc[0] == pytest.approx(10.0)
        assert out.iloc[1] == pytest.approx(60.0)   # 50 + min(10, 60-40)
        assert out.iloc[2] == pytest.approx(110.0)  # 90 + 10 IF + 10 below-target


# extract_recent_weeks — sparkline data extractor (plan §4.2)
class TestExtractRecentWeeks:
    @pytest.fixture
    def weekly_df(self):
        # Mimics PROCESSED_MONITORING_WEEKLY shape: per-merchant per-DIMENSI rows
        # with W1..W52 columns. Most weeks zero/null until late in the year.
        cols = ['MERCHANT_GROUP', 'DIMENSI'] + [f'W{i:02d}' for i in range(1, 53)]
        rows = []
        # INDOMARET — VOL row populated through W12
        indomaret = ['INDOMARET', 'VOL'] + [0] * 52
        for w in range(1, 13):
            indomaret[2 + w] = 1_000_000_000 + w * 50_000_000  # ramping up
        rows.append(indomaret)
        # INDOMARET — TRX row also present (should be ignored for VOL extraction)
        indomaret_trx = ['INDOMARET', 'TRX'] + [0] * 52
        for w in range(1, 13):
            indomaret_trx[2 + w] = 1000 + w
        rows.append(indomaret_trx)
        # GRAMEDIA — no rows for W1..W52 (all zero) — dropped-off scenario
        gramedia = ['GRAMEDIA', 'VOL'] + [0] * 52
        rows.append(gramedia)
        return pd.DataFrame(rows, columns=cols)

    def test_returns_last_n_weeks_for_existing_merchant(self, weekly_df):
        out = extract_recent_weeks(weekly_df, merchant='INDOMARET',
                                   dimensi='VOL', n_weeks=4)
        assert len(out) == 4
        # Should be the trailing 4 populated weeks: W9..W12
        # (1.45B, 1.5B, 1.55B, 1.6B)
        expected = [1_450_000_000, 1_500_000_000, 1_550_000_000, 1_600_000_000]
        assert out == pytest.approx(expected)

    def test_filters_by_dimensi(self, weekly_df):
        # VOL extraction should NOT pick up the TRX row's small numbers.
        out = extract_recent_weeks(weekly_df, merchant='INDOMARET',
                                   dimensi='VOL', n_weeks=4)
        # Last value of VOL row is 1.6B, last value of TRX row is 1012.
        # If filtering worked, we get the billion-scale number, not 1012.
        assert out[-1] > 1_000_000

    def test_returns_empty_for_missing_merchant(self, weekly_df):
        out = extract_recent_weeks(weekly_df, merchant='UNKNOWN',
                                   dimensi='VOL', n_weeks=12)
        assert out == []

    def test_returns_empty_for_all_zero_merchant(self, weekly_df):
        # GRAMEDIA has all zeros — should yield empty list, not 12 zeros.
        out = extract_recent_weeks(weekly_df, merchant='GRAMEDIA',
                                   dimensi='VOL', n_weeks=12)
        assert out == []

    def test_handles_empty_dataframe(self):
        out = extract_recent_weeks(pd.DataFrame(), merchant='X',
                                   dimensi='VOL', n_weeks=12)
        assert out == []

    def test_returns_n_weeks_or_fewer(self, weekly_df):
        # INDOMARET only has 12 populated weeks; asking for 24 returns 12.
        out = extract_recent_weeks(weekly_df, merchant='INDOMARET',
                                   dimensi='VOL', n_weeks=24)
        assert len(out) == 12
