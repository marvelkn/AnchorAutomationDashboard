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
    compute_growth_signals,
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
