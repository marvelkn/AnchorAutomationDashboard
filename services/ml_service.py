"""
ML Service — clustering, anomaly detection, forecasting.

Extracted verbatim from pages/4_Dashboard.py run_ml() / _hw_forecast().
No Streamlit or Dash imports here — pure Python/Pandas/Sklearn.
"""
import logging

import numpy as np
import pandas as pd

from sklearn.preprocessing import RobustScaler   # A3: immune to heavy right-tail outliers
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.ensemble import IsolationForest

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing as HoltWinters
    _HW_AVAILABLE = True
except ImportError:
    _HW_AVAILABLE = False

try:
    import shap as _shap
    _SHAP_AVAILABLE = True
except ImportError:
    _SHAP_AVAILABLE = False

logger = logging.getLogger(__name__)   # A8: module-level logger


# ── Holt-Winters Forecast ──────────────────────────────────────────────────────

def hw_forecast(monthly_sv_series, periods_ahead: int = 12) -> dict:
    """
    Holt-Winters exponential smoothing on historical monthly SV.
    Falls back to linear extrapolation for sparse data (<6 points).
    """
    result = {
        "forecast": None,
        "projected_eoy": None,
        "method": "Linear (fallback)",
        "success": False,
    }
    if not _HW_AVAILABLE:
        return result

    series = pd.to_numeric(monthly_sv_series, errors="coerce").fillna(0)
    series = series[series > 0]

    # A10: linear extrapolation fallback for sparse data
    if len(series) < 6:
        if len(series) >= 2:
            avg_growth = series.pct_change().mean()
            last_val   = series.iloc[-1]
            forecast   = np.array(
                [last_val * (1 + avg_growth) ** i for i in range(1, periods_ahead + 1)]
            )
            forecast = np.maximum(forecast, 0)
            result.update({
                "success":       True,
                "forecast":      forecast.tolist(),
                "projected_eoy": float(forecast.sum()),
                "method":        "linear_extrapolation",
            })
        return result

    try:
        use_seasonal = len(series) >= 24
        if use_seasonal:
            model = HoltWinters(
                series.values,
                trend="add",
                seasonal="add",
                seasonal_periods=12,
                initialization_method="estimated",
            )
            method_label = "Holt-Winters (trend + seasonal)"
        else:
            model = HoltWinters(
                series.values,
                trend="add",
                seasonal=None,
                initialization_method="estimated",
            )
            method_label = "Holt's Double Smoothing (trend only)"

        fit = model.fit(optimized=True, remove_bias=True)
        forecast_values = np.maximum(fit.forecast(periods_ahead), 0)
        result.update({
            "forecast":      forecast_values.tolist(),
            "projected_eoy": float(np.sum(forecast_values)),
            "method":        method_label,
            "success":       True,
        })
    except Exception as e:
        logger.warning(f"[ML] Holt-Winters failed for series of length {len(series)}: {e}")  # A8

    return result


# ── Best-k Recommender ─────────────────────────────────────────────────────────

def find_best_k(X_s: np.ndarray, k_range=range(2, 7)) -> tuple[int, dict]:
    """Return best k via silhouette score (A6)."""
    scores: dict[int, float] = {}
    for k in k_range:
        if len(X_s) <= k:
            break
        km     = KMeans(n_clusters=k, init="k-means++", n_init=10, random_state=42)
        labels = km.fit_predict(X_s)
        try:
            scores[k] = float(silhouette_score(X_s, labels))
        except Exception:
            pass
    if not scores:
        return 3, {}
    best_k = max(scores, key=scores.get)
    return best_k, scores


# ── ML Pipeline ────────────────────────────────────────────────────────────────

ML_COLS = [
    "MERCHANT_GROUP", "CLUSTER", "CHURN_RISK", "RISK_SCORE", "SILHOUETTE_SCORE",
    "BEST_K", "BEST_K_SCORE",
    "PM", "WEEKS_ACTIVE", "SV_GROWTH_RATE", "ACHIEVEMENT_PCT", "AVG_SV", "AVG_FBI",
    "ZSCORE_SV", "ZSCORE_FBI", "ZSCORE_GROWTH", "SV_GROWTH_CLIPPED",
    "TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI", "RASIO_ONUS",
    "IF_ANOMALY_SCORE", "IF_IS_ANOMALY",
    "IF_CONTRIB_AVG_SV", "IF_CONTRIB_AVG_FBI", "IF_CONTRIB_RASIO_ONUS",
    "IF_CONTRIB_SV_GROWTH", "IF_CONTRIB_ACHIEVEMENT", "IF_CONTRIB_WEEKS_ACTIVE",
]


def run_ml(
    df_c: pd.DataFrame,
    df_m: pd.DataFrame,
    df_t: pd.DataFrame | None = None,
    k_clusters: int = 3,
    z_thresh: float = -1.5,
) -> pd.DataFrame:
    """
    BTN Anchor ML Pipeline v2.
    1. Merge Card Share + Monitoring
    2. Feature Engineering (AVG_SV/FBI normalised by WEEKS_ACTIVE)
    3. K-Means++ Clustering with composite multi-metric ranking
    4. Isolation Forest anomaly detection + SHAP feature contribution (A1)
    5. Modified Z-Score (MAD) outlier scoring
    6. Composite Risk Score 0-100 (Growth 40%, SV 30%, FBI 20%, Target 10%)
    7. Percentile-based CHURN_RISK labelling (A5)
    8. Silhouette Score + best-k recommendation (A6)
    """
    if df_c.empty:
        return pd.DataFrame(columns=ML_COLS)

    # 1. Merge
    if not df_m.empty:
        agg_cols = {c: "sum" for c in ["TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI"] if c in df_c.columns}
        if "RASIO_ONUS" in df_c.columns:
            agg_cols["RASIO_ONUS"] = "mean"
        df = df_c.groupby("MERCHANT_GROUP").agg(agg_cols).reset_index()
        df = pd.merge(df, df_m, on="MERCHANT_GROUP", how="left")
    else:
        df = df_c.copy()

    if df.empty:
        return pd.DataFrame(columns=ML_COLS)

    for col in ["TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI", "RASIO_ONUS"]:
        if col not in df.columns:
            df[col] = 0

    # 2. Feature Engineering
    df["WEEKS_ACTIVE"] = (
        pd.to_numeric(df.get("WEEKS_ACTIVE", pd.Series([12] * len(df))), errors="coerce")
        .fillna(12)
        .clip(1, 52)
    )
    months_active   = (df["WEEKS_ACTIVE"] / 4.33).clip(1, 12)
    df["AVG_SV"]    = df["TOTAL_SV"] / months_active
    df["AVG_FBI"]   = df["TOTAL_FBI"] / months_active
    df["RASIO_ONUS"] = df["RASIO_ONUS"].clip(0, 1).fillna(0)

    df["SV_GROWTH_RATE"] = (
        pd.to_numeric(df.get("SV_GROWTH_RATE", pd.Series([0] * len(df))), errors="coerce")
        .fillna(0)
    )

    if len(df) > 1:
        low, high = df["SV_GROWTH_RATE"].quantile([0.05, 0.95])
        df["SV_GROWTH_CLIPPED"] = df["SV_GROWTH_RATE"].clip(low, high)
    else:
        df["SV_GROWTH_CLIPPED"] = df["SV_GROWTH_RATE"]

    if df_t is not None and not df_t.empty and "TARGET_VOL_2026" in df_t.columns:
        df = pd.merge(df, df_t[["MERCHANT_GROUP", "TARGET_VOL_2026"]], on="MERCHANT_GROUP", how="left")
        df["ACHIEVEMENT_PCT"] = np.where(
            df["TARGET_VOL_2026"].fillna(0) > 0,
            (df["TOTAL_SV"] / df["TARGET_VOL_2026"] * 100).clip(0, 200),
            0,
        )
    else:
        df["ACHIEVEMENT_PCT"] = 0

    # 3. Clustering
    FEAT = ["AVG_SV", "AVG_FBI", "RASIO_ONUS", "SV_GROWTH_CLIPPED", "ACHIEVEMENT_PCT", "WEEKS_ACTIVE"]
    X = df[FEAT].fillna(0).copy()
    X["AVG_SV"]  = np.log1p(X["AVG_SV"])
    X["AVG_FBI"] = np.log1p(X["AVG_FBI"])

    df["SILHOUETTE_SCORE"] = 0.0
    df["RISK_SCORE"]       = 0.0
    df["BEST_K"]           = k_clusters
    df["BEST_K_SCORE"]     = 0.0

    try:
        if len(df) >= k_clusters:
            X_s = RobustScaler().fit_transform(X)   # A3: RobustScaler (median+IQR)

            # A6: recommend best k before the user's chosen clustering
            if len(df) >= 3:
                best_k, best_scores = find_best_k(X_s)
                df["BEST_K"]       = best_k
                df["BEST_K_SCORE"] = best_scores.get(best_k, 0.0)

            km = KMeans(n_clusters=k_clusters, init="k-means++", n_init=20, random_state=42)
            df["CLUSTER_RAW"] = km.fit_predict(X_s)

            cs = df.groupby("CLUSTER_RAW").agg(
                {"AVG_SV": "mean", "ACHIEVEMENT_PCT": "mean", "SV_GROWTH_CLIPPED": "mean"}
            )
            for col in cs.columns:
                rng = cs[col].max() - cs[col].min()
                cs[col] = (cs[col] - cs[col].min()) / (rng + 1e-9)
            cs["COMPOSITE"] = (
                0.60 * cs["AVG_SV"]
                + 0.25 * cs["ACHIEVEMENT_PCT"]
                + 0.15 * cs["SV_GROWTH_CLIPPED"]
            )
            rank = {c: i for i, c in enumerate(cs["COMPOSITE"].sort_values(ascending=False).index)}

            lbl_maps = {
                3: {0: "PREMIUM", 1: "REGULER", 2: "PASIF"},
                4: {0: "ELITE", 1: "PREMIUM", 2: "REGULER", 3: "PASIF"},
                5: {0: "ELITE", 1: "PREMIUM", 2: "REGULER", 3: "PASIF", 4: "DORMANT"},
            }
            lbl         = lbl_maps.get(k_clusters, {i: f"TIER {i+1}" for i in range(k_clusters)})
            df["CLUSTER"] = df["CLUSTER_RAW"].map(lambda c: lbl[rank[c]])

            if len(df) >= 2:
                df["SILHOUETTE_SCORE"] = round(float(silhouette_score(X_s, df["CLUSTER_RAW"])), 4)

            # 4a. Isolation Forest — A4: dynamic contamination
            try:
                if len(df) >= 4:
                    n             = len(df)
                    contamination = min(max(2 / n, 0.05), 0.15)   # A4
                    iso = IsolationForest(
                        n_estimators=100,
                        contamination=contamination,
                        random_state=42,
                        n_jobs=-1,
                    )
                    iso.fit(X_s)
                    df["IF_ANOMALY_SCORE"] = (-iso.score_samples(X_s)).round(4)
                    df["IF_IS_ANOMALY"]    = iso.predict(X_s) == -1

                    _lofo_keys = [
                        "IF_CONTRIB_AVG_SV", "IF_CONTRIB_AVG_FBI",
                        "IF_CONTRIB_RASIO_ONUS", "IF_CONTRIB_SV_GROWTH",
                        "IF_CONTRIB_ACHIEVEMENT", "IF_CONTRIB_WEEKS_ACTIVE",
                    ]

                    # A1: SHAP values replace mathematically-incorrect zero-ablation LOFO
                    if _SHAP_AVAILABLE:
                        try:
                            explainer = _shap.TreeExplainer(iso)
                            sv        = explainer.shap_values(X_s)
                            if not isinstance(sv, np.ndarray):
                                sv = np.array(sv)
                            for fi, col in enumerate(_lofo_keys):
                                df[col] = np.abs(sv[:, fi]).round(4)
                        except Exception as e:
                            logger.warning(f"[ML] SHAP failed ({e}); falling back to ablation LOFO")
                            _lofo_ablation(iso, X_s, _lofo_keys, df)
                    else:
                        _lofo_ablation(iso, X_s, _lofo_keys, df)
                else:
                    _zero_if(df)
            except Exception as e:
                logger.warning(f"[ML] Isolation Forest failed: {e}")   # A8
                _zero_if(df)
        else:
            df["CLUSTER"] = "REGULER"

        # 4b. Modified Z-Score (MAD)
        def _mad_zscore(series):
            s = pd.to_numeric(series, errors="coerce").fillna(0)
            if len(s) < 2:
                return pd.Series(0.0, index=s.index)
            median = s.median()
            mad    = (s - median).abs().median()
            if mad < 1e-9:
                # A9: all values nearly identical — z-score is undefined, return zeros
                logger.debug(f"[ML] MAD≈0 — all values nearly identical (median={median:.4f})")
                return pd.Series(0.0, index=s.index)
            return 0.6745 * (s - median) / mad

        if len(df) > 1:
            df["ZSCORE_SV"]     = _mad_zscore(np.log1p(df["AVG_SV"]))
            df["ZSCORE_FBI"]    = _mad_zscore(np.log1p(df["AVG_FBI"]))
            # A7: SV_GROWTH_CLIPPED already winsorized to [p5, p95] — no log transform needed
            df["ZSCORE_GROWTH"] = _mad_zscore(df["SV_GROWTH_CLIPPED"])
        else:
            df["ZSCORE_SV"] = df["ZSCORE_FBI"] = df["ZSCORE_GROWTH"] = 0.0

        # 5. Composite Risk Score (0-100)
        df["RISK_SCORE"] = (
            np.clip(-df["ZSCORE_GROWTH"], 0, 3) / 3 * 40
            + np.clip(-df["ZSCORE_SV"],    0, 3) / 3 * 30
            + np.clip(-df["ZSCORE_FBI"],   0, 3) / 3 * 20
            + np.clip(1 - df["ACHIEVEMENT_PCT"] / 100, 0, 1) * 10
        ).clip(0, 100).round(1)

        # 6. A5: Percentile-based CHURN_RISK — consistent proportions regardless of score range
        p90 = df["RISK_SCORE"].quantile(0.90)
        p70 = df["RISK_SCORE"].quantile(0.70)
        df["CHURN_RISK"] = df["RISK_SCORE"].apply(
            lambda s: "HIGH RISK" if s >= p90 else ("MEDIUM RISK" if s >= p70 else "STABLE")
        )

    except Exception as e:
        logger.warning(f"[ML] Pipeline failed: {e}")   # A8
        df["CLUSTER"]    = "UNKNOWN"
        df["CHURN_RISK"] = "STABLE"
        df["RISK_SCORE"] = 0.0
        df["ZSCORE_SV"]  = df["ZSCORE_FBI"] = df["ZSCORE_GROWTH"] = 0.0

    for col in ML_COLS:
        if col not in df.columns:
            df[col] = np.nan

    return df[ML_COLS]


def _lofo_ablation(
    iso: IsolationForest,
    X_s: np.ndarray,
    lofo_keys: list[str],
    df: pd.DataFrame,
) -> None:
    """Original zero-ablation LOFO — used as SHAP fallback."""
    _base_scores = -iso.score_samples(X_s)
    for _fi, _fk in enumerate(lofo_keys):
        _X_abl          = X_s.copy()
        _X_abl[:, _fi]  = 0.0
        df[_fk]         = (_base_scores - (-iso.score_samples(_X_abl))).round(4)


def _zero_if(df: pd.DataFrame) -> None:
    """Fill Isolation Forest columns with zeros in-place."""
    df["IF_ANOMALY_SCORE"] = 0.0
    df["IF_IS_ANOMALY"]    = False
    for fk in [
        "IF_CONTRIB_AVG_SV", "IF_CONTRIB_AVG_FBI",
        "IF_CONTRIB_RASIO_ONUS", "IF_CONTRIB_SV_GROWTH",
        "IF_CONTRIB_ACHIEVEMENT", "IF_CONTRIB_WEEKS_ACTIVE",
    ]:
        df[fk] = 0.0
