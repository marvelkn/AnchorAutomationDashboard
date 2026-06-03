"""
Generate the Elbow + Silhouette cluster-count diagnostic figure for the report.

Runs the *same* dynamic K-selection sweep the dashboard uses
(utils.ml_engine.select_optimal_k) over the real BTN Anchor portfolio and writes:

  * fig_elbow_silhouette.png  — inertia (WCSS) and Silhouette vs K, K=3 marked
  * fig_elbow_silhouette.csv  — (K, inertia, silhouette, davies_bouldin)

into the LaTeX report's assets/pics directory, so the figure in Bab3 reflects the
true data rather than a hand-drawn sketch.

Data source (default): the live Neon database via utils.cloud_db.build_engine()
(reads tables processed_card_share, processed_monitoring, target). You may instead
point at exported CSVs with --card-csv / --mon-csv / --target-csv.

Usage (from the project root, with DATABASE_URL set):

    python scripts/plot_elbow_silhouette.py
    python scripts/plot_elbow_silhouette.py --card-csv card.csv --mon-csv mon.csv
    python scripts/plot_elbow_silhouette.py --out "C:/path/to/assets/pics"

Run it once whenever the merchant portfolio changes; commit the regenerated PNG
alongside the report.
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.ml_engine import prepare_cluster_features, select_optimal_k, N_CLUSTERS

# Default output dir — the LaTeX report's image folder (sibling project tree).
_DEFAULT_OUT = os.path.normpath(os.path.join(
    PROJECT_ROOT, "..", "..", "..", "Laporan", "LaTex",
    "2521_Magang___Marvel_Kevin_Nathanael", "assets", "pics",
))


def _load_from_db():
    """Read the three source tables from Neon, mirroring the dashboard loader."""
    from utils.cloud_db import build_engine
    eng = build_engine()
    with eng.connect() as conn:
        df_card = pd.read_sql_query("SELECT * FROM processed_card_share", conn)
        df_mon  = pd.read_sql_query("SELECT * FROM processed_monitoring", conn)
        try:
            df_target = pd.read_sql_query("SELECT * FROM target", conn)
        except Exception:
            df_target = pd.DataFrame()
    for df in (df_card, df_mon, df_target):
        if len(df.columns):
            df.columns = [c.upper() for c in df.columns]
    return df_card, df_mon, df_target


def _load_from_csv(card, mon, target):
    df_card = pd.read_csv(card)
    df_mon  = pd.read_csv(mon)
    df_target = pd.read_csv(target) if target else pd.DataFrame()
    for df in (df_card, df_mon, df_target):
        if len(df.columns):
            df.columns = [c.upper() for c in df.columns]
    return df_card, df_mon, df_target


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=_DEFAULT_OUT, help="output directory for PNG/CSV")
    ap.add_argument("--card-csv", default=None, help="card-share CSV (overrides DB)")
    ap.add_argument("--mon-csv", default=None, help="monitoring CSV (overrides DB)")
    ap.add_argument("--target-csv", default=None, help="target CSV (optional)")
    ap.add_argument("--k-max", type=int, default=8, help="largest K to evaluate")
    args = ap.parse_args()

    if args.card_csv and args.mon_csv:
        df_card, df_mon, df_target = _load_from_csv(args.card_csv, args.mon_csv, args.target_csv)
        src = "CSV"
    else:
        df_card, df_mon, df_target = _load_from_db()
        src = "Neon DB"

    df, X_s = prepare_cluster_features(df_card, df_mon, df_target if not df_target.empty else None)
    n = 0 if X_s is None else len(X_s)
    print(f"[plot_elbow_silhouette] source={src}  merchants={n}")
    if n < 3:
        print("ERROR: need at least 3 merchants with complete features to sweep K.", file=sys.stderr)
        return 1

    diag = select_optimal_k(X_s, k_min=2, k_max=args.k_max, business_k=N_CLUSTERS)
    print("[plot_elbow_silhouette] " + diag["justification"])

    ks  = diag["k_values"]
    inr = diag["inertia"]
    sil = diag["silhouette"]
    dbi = diag["davies_bouldin"]

    os.makedirs(args.out, exist_ok=True)
    csv_path = os.path.join(args.out, "fig_elbow_silhouette.csv")
    pd.DataFrame({"K": ks, "inertia": inr, "silhouette": sil, "davies_bouldin": dbi}) \
        .to_csv(csv_path, index=False)
    print(f"[plot_elbow_silhouette] wrote {csv_path}")

    # ── Dual-axis figure: inertia (WCSS) + Silhouette vs K ──────────────────────
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    NAVY, GOLD, RED = "#1F3A93", "#C9A227", "#C0392B"
    fig, ax1 = plt.subplots(figsize=(7.2, 4.3))

    ax1.plot(ks, inr, marker="o", color=NAVY, linewidth=2, label="Inersia (WCSS)")
    ax1.set_xlabel("Jumlah Klaster (K)")
    ax1.set_ylabel("Inersia / WCSS", color=NAVY)
    ax1.tick_params(axis="y", labelcolor=NAVY)
    ax1.set_xticks(ks)

    ax2 = ax1.twinx()
    ax2.plot(ks, sil, marker="s", color=GOLD, linewidth=2, label="Silhouette Score")
    ax2.set_ylabel("Silhouette Score", color=GOLD)
    ax2.tick_params(axis="y", labelcolor=GOLD)

    if N_CLUSTERS in ks:
        ax1.axvline(N_CLUSTERS, ls="--", color=RED, alpha=0.75, linewidth=1.4)
        ax1.annotate(f"K={N_CLUSTERS} (terpilih)",
                     xy=(N_CLUSTERS, max(inr)), xytext=(6, -4),
                     textcoords="offset points", color=RED, fontsize=9, fontweight="bold")

    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [ln.get_label() for ln in lines], loc="upper right", fontsize=9)
    ax1.set_title(f"Penentuan Jumlah Klaster Optimal (Elbow + Silhouette), n={n} merchant")
    fig.tight_layout()

    png_path = os.path.join(args.out, "fig_elbow_silhouette.png")
    fig.savefig(png_path, dpi=200)
    print(f"[plot_elbow_silhouette] wrote {png_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
