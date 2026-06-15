"""
BTN Anchor ML engine — pure, Streamlit-free analytics core.

`run_ml` (K-Means tiering + MAD z-score + composite risk + Isolation Forest) used
to live inline in pages/4_Dashboard.py, where it could not be unit-tested (the
page runs st.set_page_config and builds a DB engine at import). It is extracted
here verbatim, with the single Streamlit call swapped for a module logger, so the
dashboard imports it through a thin cached wrapper while tests import it directly.

No Streamlit, no DB, no global state — safe to import in tests and CLI scripts.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score
from sklearn.ensemble import IsolationForest

log = logging.getLogger(__name__)

# Model parameters. The operating cluster count is chosen *dynamically* by
# select_optimal_k() — it sweeps Elbow + Silhouette + Davies-Bouldin on every call
# and picks the Silhouette-optimal K within the operable band [K_MIN, K_MAX].
# N_CLUSTERS is only a fallback/default for samples too small to sweep (n < 3).
# Each constant can be overridden via an environment variable without code changes.
import os as _os
K_MIN      = int(_os.getenv("ANCHOR_K_MIN",      "2"))    # smallest operable tier count
K_MAX      = int(_os.getenv("ANCHOR_K_MAX",      "5"))    # largest operable tier count
N_CLUSTERS = int(_os.getenv("ANCHOR_N_CLUSTERS", "3"))    # fallback for tiny samples
Z_THRESH   = float(_os.getenv("ANCHOR_Z_THRESH", "-1.2")) # anomaly upgrade threshold

# Ordered tier vocabulary (best → worst), aligned with theme.CLUSTER_COLORS and the
# dashboard's _TIER_RANK. For a chosen K the clusters (ranked best→worst by COMPOSITE)
# are labelled from the matching ladder (e.g. K=3 -> PREMIUM/REGULER/PASIF, K=5 ->
# ELITE/PREMIUM/REGULER/PASIF/DORMANT), keeping tier names stable across re-runs.
_TIER_LADDER = {
    2: ['PREMIUM', 'PASIF'],
    3: ['PREMIUM', 'REGULER', 'PASIF'],
    4: ['ELITE', 'PREMIUM', 'REGULER', 'PASIF'],
    5: ['ELITE', 'PREMIUM', 'REGULER', 'PASIF', 'DORMANT'],
}


def prepare_cluster_features(df_c, df_m, df_t=None):
    """
    Merge the Card-Share + Monitoring (+ optional Target) frames, engineer the six
    clustering features, log-transform the two monetary ones, and standardize the
    matrix that K-Means / the K-sweep operate on.

    Returns ``(df, X_s)`` where ``df`` is the engineered per-merchant frame and
    ``X_s`` is the StandardScaler-transformed feature matrix (``None`` when there is
    nothing to cluster). Shared by :func:`run_ml` and the dashboard's elbow/silhouette
    diagnostic so both see an identical feature space.
    """
    # ── 1. Merge ──────────────────────────────────────────────────────────────
    if df_c is None or df_c.empty:
        return pd.DataFrame(), None

    if df_m is not None and not df_m.empty:
        agg_cols = {c: 'sum' for c in ['TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI'] if c in df_c.columns}
        if 'RASIO_ONUS' in df_c.columns: agg_cols['RASIO_ONUS'] = 'mean'
        df = df_c.groupby('MERCHANT_GROUP').agg(agg_cols).reset_index()
        df = pd.merge(df, df_m, on='MERCHANT_GROUP', how='left')
    else:
        df = df_c.copy()

    if df.empty:
        return df, None

    for col in ['TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'RASIO_ONUS']:
        if col not in df.columns: df[col] = 0

    # ── 2. Feature Engineering ────────────────────────────────────────────────
    # Normalize monthly avg by actual weeks active, not a hardcoded /12
    df['WEEKS_ACTIVE'] = pd.to_numeric(
        df.get('WEEKS_ACTIVE', pd.Series([12] * len(df))), errors='coerce'
    ).fillna(12).clip(1, 52)

    months_active = (df['WEEKS_ACTIVE'] / 4.33).clip(1, 12)
    df['AVG_SV']     = df['TOTAL_SV'] / months_active
    df['AVG_FBI']    = df['TOTAL_FBI'] / months_active
    df['RASIO_ONUS'] = df['RASIO_ONUS'].clip(0, 1).fillna(0)

    df['SV_GROWTH_RATE'] = pd.to_numeric(
        df.get('SV_GROWTH_RATE', pd.Series([0] * len(df))), errors='coerce'
    ).fillna(0)

    if len(df) > 1:
        low, high = df['SV_GROWTH_RATE'].quantile([0.05, 0.95])
        df['SV_GROWTH_CLIPPED'] = df['SV_GROWTH_RATE'].clip(low, high)
    else:
        df['SV_GROWTH_CLIPPED'] = df['SV_GROWTH_RATE']

    if df_t is not None and not df_t.empty and 'TARGET_VOL_2026' in df_t.columns:
        df = pd.merge(df, df_t[['MERCHANT_GROUP', 'TARGET_VOL_2026']], on='MERCHANT_GROUP', how='left')
        df['ACHIEVEMENT_PCT'] = np.where(
            df['TARGET_VOL_2026'].fillna(0) > 0,
            (df['TOTAL_SV'] / df['TARGET_VOL_2026'] * 100).clip(0, 200), 0
        )
    else:
        df['ACHIEVEMENT_PCT'] = 0

    # ── 3. Feature matrix — log-transform monetary skew, then standardize ─────
    FEAT = ['AVG_SV', 'AVG_FBI', 'RASIO_ONUS', 'SV_GROWTH_CLIPPED', 'ACHIEVEMENT_PCT', 'WEEKS_ACTIVE']
    X = df[FEAT].fillna(0).copy()
    X['AVG_SV']  = np.log1p(X['AVG_SV'])
    X['AVG_FBI'] = np.log1p(X['AVG_FBI'])
    X_s = StandardScaler().fit_transform(X) if len(df) >= 1 else None

    return df, X_s


def _find_elbow(k_values, inertias):
    """
    Locate the elbow of an inertia (WCSS) curve with no external dependency.

    Normalizes K and inertia to [0, 1] and returns the K whose point lies farthest
    (perpendicular distance) from the straight chord joining the first and last
    points — the classic "kneedle" construction. With fewer than three points the
    curve cannot bend, so the first K is returned.
    """
    ks   = np.asarray(k_values, dtype=float)
    wcss = np.asarray(inertias,  dtype=float)
    if ks.size < 3:
        return int(ks[0]) if ks.size else 0
    x = (ks - ks.min())   / (np.ptp(ks)   + 1e-12)
    y = (wcss - wcss.min()) / (np.ptp(wcss) + 1e-12)
    x1, y1, x2, y2 = x[0], y[0], x[-1], y[-1]
    denom = np.hypot(x2 - x1, y2 - y1) + 1e-12
    dist  = np.abs((y2 - y1) * x - (x2 - x1) * y + x2 * y1 - y2 * x1) / denom
    return int(ks[int(np.argmax(dist))])


def select_optimal_k(X_s, k_min=K_MIN, k_max=K_MAX, business_k=N_CLUSTERS, random_state=42):
    """
    Dynamically determine the operating cluster count from the data.

    Sweeps K over ``[k_min, min(k_max, n-1)]`` and scores every candidate by inertia
    (WCSS, the K-Means objective), Silhouette (intra-cluster cohesion vs. inter-cluster
    separation), and Davies-Bouldin. From those curves it derives the Elbow K and the
    Silhouette-optimal K.

    The operating count ``chosen_k`` is the **Silhouette-optimal K**, clamped to the
    operable band ``[k_min, k_max]`` to prevent over-segmentation (parsimony). The Elbow K
    is reported alongside as supporting evidence. ``business_k`` is used only as the
    fallback for samples too small to sweep (n < 3). Never raises on small samples.

    Returns a diagnostics dict::

        {k_values, inertia, silhouette, davies_bouldin,
         k_elbow, k_silhouette, chosen_k, business_anchored, justification}
    """
    n    = 0 if X_s is None else len(X_s)
    k_hi = min(k_max, n - 1)
    fallback_k = max(k_min, min(business_k, k_hi if k_hi >= k_min else business_k))
    diag = {
        "k_values": [], "inertia": [], "silhouette": [], "davies_bouldin": [],
        "k_elbow": fallback_k, "k_silhouette": fallback_k,
        "chosen_k": fallback_k, "business_anchored": True, "justification": "",
    }
    # Silhouette needs 2 <= K <= n-1; need at least one valid candidate.
    if n < max(k_min + 1, 3) or k_hi < k_min:
        diag["justification"] = (
            f"Sampel terlalu kecil untuk sweep K (n={n}); jumlah klaster ditetapkan "
            f"pada nilai cadangan K={fallback_k}."
        )
        return diag

    ks, inertias, sils, dbis = [], [], [], []
    for k in range(k_min, k_hi + 1):
        km = KMeans(n_clusters=k, init="k-means++", n_init=20, random_state=random_state)
        labels = km.fit_predict(X_s)
        ks.append(int(k))
        inertias.append(round(float(km.inertia_), 4))
        sils.append(round(float(silhouette_score(X_s, labels)), 4))
        dbis.append(round(float(davies_bouldin_score(X_s, labels)), 4))

    k_elbow  = _find_elbow(ks, inertias)
    k_sil    = int(ks[int(np.argmax(sils))])
    # Operating K = Silhouette-optimal, clamped to the operable band [k_min, k_max].
    chosen_k = max(k_min, min(k_sil, k_max))
    diag.update({
        "k_values": ks, "inertia": inertias, "silhouette": sils,
        "davies_bouldin": dbis, "k_elbow": k_elbow, "k_silhouette": k_sil,
        "chosen_k": chosen_k, "business_anchored": False,
    })

    agree = " (konsisten dengan titik siku)" if k_elbow == chosen_k else ""
    diag["justification"] = (
        f"Sweep K pada rentang [{k_min}..{k_hi}]: titik siku (elbow) inersia pada "
        f"K={k_elbow}, Silhouette tertinggi pada K={k_sil}. Jumlah klaster operasional "
        f"dipilih secara dinamis pada K={chosen_k}{agree}, dibatasi pada rentang "
        f"[{k_min},{k_max}] untuk mencegah over-segmentasi (prinsip parsimoni)."
    )
    return diag


def run_ml(df_c, df_m, df_t=None):
    """
    BTN Anchor ML Pipeline v2:
    1. Merge Card Share + Monitoring
    2. Feature Engineering — AVG_SV/FBI normalized by actual WEEKS_ACTIVE (not fixed /12)
    3. K-Means++ Clustering — K selected dynamically by Elbow+Silhouette sweep
       (select_optimal_k) within [K_MIN, K_MAX]; tiers labelled from _TIER_LADDER
    4. Modified Z-Score (MAD) — robust anomaly detection, resistant to outliers in small portfolios
    5. Composite Risk Score 0–100 — weighted: Growth 40%, SV 30%, FBI 20%, Achievement 10%
    6. Three-tier CHURN_RISK — HIGH (≥60) / MEDIUM (30–59) / STABLE (<30)
    7. Cohesion metrics — Silhouette Score + Davies-Bouldin Index
    8. PCA 2-D projection — for the tier-separation scatter plot
    """
    ML_COLS = ['MERCHANT_GROUP', 'CLUSTER', 'CHURN_RISK', 'RISK_SCORE',
               'SILHOUETTE_SCORE', 'DB_SCORE', 'PCA_X', 'PCA_Y', 'PCA_VAR1', 'PCA_VAR2',
               'PM', 'WEEKS_ACTIVE', 'SV_GROWTH_RATE', 'ACHIEVEMENT_PCT', 'AVG_SV', 'AVG_FBI',
               'ZSCORE_SV', 'ZSCORE_FBI', 'ZSCORE_GROWTH', 'SV_GROWTH_CLIPPED',
               'TOTAL_SV', 'TOTAL_TRX', 'TOTAL_FBI', 'RASIO_ONUS',
               'IF_ANOMALY_SCORE', 'IF_IS_ANOMALY',
               'IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI', 'IF_CONTRIB_RASIO_ONUS',
               'IF_CONTRIB_SV_GROWTH', 'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']

    if df_c.empty:
        return pd.DataFrame(columns=ML_COLS)

    # ── 1–2. Merge + feature engineering + scaled feature matrix ──────────────
    # Delegated to prepare_cluster_features() so the dashboard's elbow/silhouette
    # diagnostic operates on the exact same feature space K-Means clusters on.
    df, X_s = prepare_cluster_features(df_c, df_m, df_t)
    if df.empty:
        return pd.DataFrame(columns=ML_COLS)

    df['SILHOUETTE_SCORE'] = 0.0
    df['DB_SCORE']         = 0.0
    df['PCA_X']            = 0.0
    df['PCA_Y']            = 0.0
    df['PCA_VAR1']         = 0.0
    df['PCA_VAR2']         = 0.0
    df['RISK_SCORE']       = 0.0

    k_diag = None
    try:
        if len(df) >= 3 and X_s is not None:
            # ── 3. Dynamic K-selection (Elbow + Silhouette) ───────────────────
            # The sweep runs on every call; the operating K is the Silhouette-optimal
            # count within the operable band [K_MIN, K_MAX]. Tiers are then labelled
            # from _TIER_LADDER[chosen_k] (PREMIUM/REGULER/PASIF at K=3).
            k_diag = select_optimal_k(X_s, k_min=K_MIN, k_max=K_MAX, business_k=N_CLUSTERS)
            log.info("K-Means cluster-count selection — %s", k_diag["justification"])
            km  = KMeans(n_clusters=k_diag["chosen_k"], init='k-means++', n_init=20, random_state=42)
            df['CLUSTER_RAW'] = km.fit_predict(X_s)

            # PCA 2-D projection — compresses the 6 scaled clustering features onto
            # two axes so the tier separation can be drawn as a scatter plot. Uses
            # the exact same X_s that K-Means clustered on, so the picture is honest.
            pca    = PCA(n_components=2, random_state=42)
            coords = pca.fit_transform(X_s)
            df['PCA_X'] = coords[:, 0]
            df['PCA_Y'] = coords[:, 1]
            df['PCA_VAR1'] = round(float(pca.explained_variance_ratio_[0]) * 100, 1)
            df['PCA_VAR2'] = round(float(pca.explained_variance_ratio_[1]) * 100, 1)

            # Multi-metric composite ranking: normalize each metric across clusters
            # then weight: SV 60%, Achievement 25%, Growth 15%
            cs = df.groupby('CLUSTER_RAW').agg(
                {'AVG_SV': 'mean', 'ACHIEVEMENT_PCT': 'mean', 'SV_GROWTH_CLIPPED': 'mean'}
            )
            for col in cs.columns:
                rng = cs[col].max() - cs[col].min()
                cs[col] = (cs[col] - cs[col].min()) / (rng + 1e-9)
            cs['COMPOSITE'] = 0.60 * cs['AVG_SV'] + 0.25 * cs['ACHIEVEMENT_PCT'] + 0.15 * cs['SV_GROWTH_CLIPPED']
            rank = {c: i for i, c in enumerate(cs['COMPOSITE'].sort_values(ascending=False).index)}

            # Dynamic tier labels — clusters ranked best→worst by COMPOSITE are mapped
            # onto the matching ladder for the chosen K (K=3 → PREMIUM/REGULER/PASIF).
            _ladder = _TIER_LADDER.get(k_diag["chosen_k"], _TIER_LADDER[N_CLUSTERS])
            lbl = {i: name for i, name in enumerate(_ladder)}
            df['CLUSTER'] = df['CLUSTER_RAW'].map(lambda c: lbl[rank[c]])

            # Cohesion metrics — how cohesive (tight) and well-separated the tiers are.
            #   Silhouette Score: -1 to 1 | >0.5 strong | 0.25–0.5 moderate | <0.25 weak (higher better)
            #   Davies-Bouldin Index: 0 and up | <0.8 strong | 0.8–1.5 moderate | >1.5 weak (lower better)
            if len(df) >= 2:
                df['SILHOUETTE_SCORE'] = round(float(silhouette_score(X_s, df['CLUSTER_RAW'])), 4)
                df['DB_SCORE']         = round(float(davies_bouldin_score(X_s, df['CLUSTER_RAW'])), 4)

            # ── 4a. Isolation Forest — Multivariate Anomaly Detection ─────────
            # Liu et al. (2008): builds n_estimators random trees; anomalies need
            # fewer splits to isolate → shorter average path length → anomaly score.
            # Uses same X_s (scaled, log-transformed) as K-Means for methodological
            # consistency. contamination=0.10 flags ~10% of portfolio (~3-4 merchants).
            try:
                if len(df) >= 4:
                    iso = IsolationForest(
                        n_estimators=100, contamination=0.10,
                        random_state=42, n_jobs=-1
                    )
                    iso.fit(X_s)
                    df['IF_ANOMALY_SCORE'] = (-iso.score_samples(X_s)).round(4)
                    df['IF_IS_ANOMALY']    = (iso.predict(X_s) == -1)

                    # ── LOFO Feature Contribution ──────────────────────────
                    # Leave-One-Feature-Out: for each feature, neutralize it
                    # (set to 0 = portfolio mean in scaled space), re-score,
                    # measure delta. No re-fitting needed — just re-scoring.
                    # Positive delta = feature makes this merchant MORE anomalous.
                    _lofo_keys = [
                        'IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                        'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                        'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE'
                    ]
                    _base_scores = -iso.score_samples(X_s)
                    for _fi, _fk in enumerate(_lofo_keys):
                        _X_abl = X_s.copy()
                        _X_abl[:, _fi] = 0.0
                        df[_fk] = (_base_scores - (-iso.score_samples(_X_abl))).round(4)
                else:
                    df['IF_ANOMALY_SCORE'] = 0.0
                    df['IF_IS_ANOMALY']    = False
                    for _fk in ['IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                                 'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                                 'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']:
                        df[_fk] = 0.0
            except Exception:
                df['IF_ANOMALY_SCORE'] = 0.0
                df['IF_IS_ANOMALY']    = False
                for _fk in ['IF_CONTRIB_AVG_SV', 'IF_CONTRIB_AVG_FBI',
                             'IF_CONTRIB_RASIO_ONUS', 'IF_CONTRIB_SV_GROWTH',
                             'IF_CONTRIB_ACHIEVEMENT', 'IF_CONTRIB_WEEKS_ACTIVE']:
                    df[_fk] = 0.0

        else:
            df['CLUSTER'] = 'REGULER'

        # ── 4. Modified Z-Score (MAD) ─────────────────────────────────────────
        # MAD = Median Absolute Deviation — resistant to extreme outliers.
        # Formula: z = 0.6745 * (x - median) / MAD
        # More reliable than standard Z-score for small portfolios (tens of merchants)
        def _mad_zscore(series):
            s = pd.to_numeric(series, errors='coerce').fillna(0)
            if len(s) < 2: return pd.Series(0.0, index=s.index)
            median = s.median()
            mad    = (s - median).abs().median()
            if mad < 1e-9: return pd.Series(0.0, index=s.index)
            return 0.6745 * (s - median) / mad

        if len(df) > 1:
            df['ZSCORE_SV']     = _mad_zscore(np.log1p(df['AVG_SV']))
            df['ZSCORE_FBI']    = _mad_zscore(np.log1p(df['AVG_FBI']))
            df['ZSCORE_GROWTH'] = _mad_zscore(df['SV_GROWTH_CLIPPED'])
        else:
            df['ZSCORE_SV'] = df['ZSCORE_FBI'] = df['ZSCORE_GROWTH'] = 0.0

        # ── 5. Composite Risk Score (0–100) ───────────────────────────────────
        # Weights reflect predictive importance for merchant churn:
        # Growth trend (40%) > Volume anomaly (30%) > FBI anomaly (20%) > Target gap (10%)
        df['RISK_SCORE'] = (
            np.clip(-df['ZSCORE_GROWTH'], 0, 3) / 3 * 40 +
            np.clip(-df['ZSCORE_SV'],     0, 3) / 3 * 30 +
            np.clip(-df['ZSCORE_FBI'],    0, 3) / 3 * 20 +
            np.clip(1 - df['ACHIEVEMENT_PCT'] / 100, 0, 1) * 10
        ).clip(0, 100).round(1)

        # ── 6. Three-tier Churn Risk ──────────────────────────────────────────
        def _risk_tier(score):
            if score >= 60: return 'HIGH RISK'
            if score >= 30: return 'MEDIUM RISK'
            return 'STABLE'
        df['CHURN_RISK'] = df['RISK_SCORE'].apply(_risk_tier)

        # ── Z_THRESH override: any z-score breach upgrades STABLE → MEDIUM RISK ──
        if len(df) > 1:
            zscore_breach = (
                (df['ZSCORE_SV']     < Z_THRESH) |
                (df['ZSCORE_FBI']    < Z_THRESH) |
                (df['ZSCORE_GROWTH'] < Z_THRESH)
            )
            df.loc[zscore_breach & (df['CHURN_RISK'] == 'STABLE'), 'CHURN_RISK'] = 'MEDIUM RISK'

    except Exception as e:
        log.warning("ML pipeline encountered an error and fell back to defaults: %s", e, exc_info=True)
        df['CLUSTER']    = 'UNKNOWN'
        df['CHURN_RISK'] = 'STABLE'
        df['RISK_SCORE'] = 0.0
        df['ZSCORE_SV']  = df['ZSCORE_FBI'] = df['ZSCORE_GROWTH'] = 0.0

    for col in ML_COLS:
        if col not in df.columns: df[col] = np.nan

    # Expose the K-selection sweep (Elbow + Silhouette evidence) to non-Streamlit
    # callers and tests. The dashboard recomputes it via select_optimal_k directly
    # because @st.cache_data does not preserve DataFrame.attrs across its boundary.
    if k_diag is not None:
        df.attrs['k_diagnostics'] = k_diag
    return df
