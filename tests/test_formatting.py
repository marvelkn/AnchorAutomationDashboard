"""
Tests for utils.formatting — centralized number formatters for the dashboard.

Plan reference: act-as-a-lead-elegant-blossom.md §3.6
Goal: replace raw 6-decimal numbers like 28927934.7233 with human-readable forms.

Run:
    pytest tests/test_formatting.py -v
"""

import os
import sys

import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.formatting import (
    fmt_count,
    fmt_currency_idr,
    fmt_growth,
    fmt_pct,
    fmt_zscore,
    growth_cell_style,
    zscore_cell_style,
)


# fmt_currency_idr
class TestFmtCurrencyIdr:
    @pytest.mark.parametrize("value,expected", [
        (28_927_934.7233, "Rp 28.9 Jt"),
        (1_927_000_000, "Rp 1.93 M"),
        (1_500_000_000_000, "Rp 1,500.00 M"),
        (749_090, "Rp 749 rb"),
        (500, "Rp 500"),
        (0, "Rp 0"),
    ])
    def test_positive_values(self, value, expected):
        assert fmt_currency_idr(value) == expected

    def test_negative_values(self):
        assert fmt_currency_idr(-28_927_934) == "-Rp 28.9 Jt"
        assert fmt_currency_idr(-1_927_000_000) == "-Rp 1.93 M"

    def test_handles_nan_and_none(self):
        assert fmt_currency_idr(float("nan")) == "—"
        assert fmt_currency_idr(None) == "—"

    def test_compact_mode_uses_short_unit(self):
        assert fmt_currency_idr(1_927_000_000, compact=True) == "Rp 1.93B"
        assert fmt_currency_idr(28_900_000, compact=True) == "Rp 28.9M"
        assert fmt_currency_idr(749_090, compact=True) == "Rp 749K"


# fmt_count
class TestFmtCount:
    @pytest.mark.parametrize("value,expected", [
        (8_810_000, "8.81 M"),
        (1_500, "1.5 K"),
        (8_810_000_000, "8.81 B"),
        (999, "999"),
        (0, "0"),
    ])
    def test_basic(self, value, expected):
        assert fmt_count(value) == expected

    def test_negative(self):
        assert fmt_count(-8_810_000) == "-8.81 M"

    def test_handles_nan(self):
        assert fmt_count(float("nan")) == "—"
        assert fmt_count(None) == "—"

    def test_explicit_decimals(self):
        assert fmt_count(8_810_000, decimals=0) == "9 M"
        assert fmt_count(1_500, decimals=2) == "1.50 K"


# fmt_pct
class TestFmtPct:
    def test_share_no_sign(self):
        assert fmt_pct(0.252) == "25.2%"
        assert fmt_pct(0.252, decimals=0) == "25%"
        assert fmt_pct(0) == "0.0%"

    def test_handles_already_in_percent_form(self):
        assert fmt_pct(25.2, scale=False) == "25.2%"

    def test_handles_nan(self):
        assert fmt_pct(float("nan")) == "—"


# fmt_growth
class TestFmtGrowth:
    def test_positive_has_plus_sign(self):
        assert fmt_growth(0.1153) == "+11.5%"
        assert fmt_growth(1.153) == "+115.3%"

    def test_negative_has_minus(self):
        assert fmt_growth(-0.32) == "-32.0%"

    def test_zero_no_sign(self):
        assert fmt_growth(0) == "0.0%"

    def test_already_in_percent_form(self):
        assert fmt_growth(115.3, scale=False) == "+115.3%"
        assert fmt_growth(-32.0, scale=False) == "-32.0%"

    def test_extreme_outlier_capped_with_indicator(self):
        result = fmt_growth(3287.34, scale=False, cap=200)
        assert "+200" in result and ">" in result

    def test_extreme_negative_capped(self):
        result = fmt_growth(-500, scale=False, cap=200)
        assert "-200" in result and "<" in result

    def test_handles_nan(self):
        assert fmt_growth(float("nan")) == "—"


# fmt_zscore
class TestFmtZscore:
    def test_two_decimals_always(self):
        assert fmt_zscore(-3.8305) == "-3.83"
        assert fmt_zscore(0) == "0.00"
        assert fmt_zscore(1.5) == "1.50"

    def test_handles_nan(self):
        assert fmt_zscore(float("nan")) == "—"


# zscore_cell_style — diverging red/white/green based on Z magnitude
class TestZscoreCellStyle:
    def test_strong_negative_returns_red_background(self):
        css = zscore_cell_style(-3.5)
        assert "background-color" in css
        assert "rgb(" in css

    def test_strong_positive_returns_green_background(self):
        css = zscore_cell_style(3.5)
        assert "background-color" in css

    def test_near_zero_is_neutral_or_pale(self):
        css = zscore_cell_style(0.1)
        assert css == "" or "rgb(" in css

    def test_handles_nan(self):
        assert zscore_cell_style(float("nan")) == ""


# growth_cell_style — diverging red/white/green at zero
class TestGrowthCellStyle:
    def test_positive_growth_green_background(self):
        css = growth_cell_style(0.5)
        assert "background-color" in css

    def test_negative_growth_red_background(self):
        css = growth_cell_style(-0.3)
        assert "background-color" in css

    def test_zero_neutral(self):
        css = growth_cell_style(0)
        assert css == "" or "rgb(" in css

    def test_handles_nan(self):
        assert growth_cell_style(float("nan")) == ""


# pandas integration smoke test
class TestPandasIntegration:
    def test_format_dict_works_with_styler(self):
        df = pd.DataFrame({
            "AVG_SV": [28_927_934.7233, 1_927_000_000],
            "ZSCORE_SV": [-3.8305, 1.50],
            "GROWTH": [0.1153, -0.32],
        })
        styled = df.style.format({
            "AVG_SV": fmt_currency_idr,
            "ZSCORE_SV": fmt_zscore,
            "GROWTH": fmt_growth,
        })
        rendered = styled.to_html()
        assert "Rp 28.9 Jt" in rendered
        assert "-3.83" in rendered
        assert "+11.5%" in rendered
        assert "28927934.7233" not in rendered
        assert "-3.8305" not in rendered
