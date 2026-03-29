"""
Build database/database/staging.db from CSV files under data/raw/ (and optional testing/).

Simulates the company flow:
  Oracle → (mentor refresh) SQLite mirror → your ETL → raw_* tables used by Streamlit + 02_transform.

Historical years 2024–2025 are derived from 2026 monthly rows using fixed scale factors
(demo only — replace with real extracts when your mentor loads Oracle).

Usage:
  python build_staging_from_raw.py

Outputs:
  database/staging.db
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH_DB = os.path.join(BASE_DIR, "database", "staging.db")
PATH_CARD = os.path.join(BASE_DIR, "data", "raw", "CARD_SHARE_ANCHOR_2026.csv")
PATH_WEEK_WIDE = os.path.join(BASE_DIR, "data", "raw", "WEEKLY_SERIES_2026_ANCHOR_NEW.csv")
PATH_WEEK_RICH = os.path.join(BASE_DIR, "data", "testing", "NEW_WeeklyMonitor_Weeks1to22_Full.csv")
PATH_MID = os.path.join(BASE_DIR, "data", "raw", "real", "MID_NULL_2026.csv")

RNG = np.random.default_rng(42)

# Scale factors vs 2026 same-calendar-month (business growth simulation)
SCALE_2024 = 0.68
SCALE_2025 = 0.84


def _num_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith(("TRX_", "SV_", "FBI_", "VOL_"))]


def load_card_normalized(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"TRANSACTION_MONTH": str})
    df.columns = [c.strip().upper() for c in df.columns]
    ren = {
        "VOL_DEBIT_ONUS": "SV_DEBIT_ONUS",
        "VOL_DEBIT_OFFUS": "SV_DEBIT_OFFUS",
        "VOL_CREDIT_OFFUS": "SV_CREDIT_OFFUS",
        "VOL_QRIS_ONUS": "SV_QRIS_ONUS",
        "VOL_QRIS_OFFUS": "SV_QRIS_OFFUS",
    }
    df = df.rename(columns={k: v for k, v in ren.items() if k in df.columns})
    df["MERCHANT_GROUP"] = df["MERCHANT_GROUP"].astype(str).str.strip().str.upper()
    df["MERCHANT_ANCHOR"] = df["MERCHANT_BRAND"].astype(str).str.strip().str.upper()
    df["TRX_MONTH"] = pd.to_numeric(df["TRANSACTION_MONTH"], errors="coerce").astype("Int64")
    df["YEAR"] = (df["TRX_MONTH"] // 100).astype(int)
    df["TRX_QRIS_OFFUS"] = df.get("TRX_QRIS_OFFUS", pd.Series(0, index=df.index)).fillna(0)
    for c in _num_cols(df):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _totals_block(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    d["TOTAL_SV"] = (
        d["SV_DEBIT_ONUS"]
        + d["SV_DEBIT_OFFUS"]
        + d["SV_CREDIT_OFFUS"]
        + d["SV_QRIS_ONUS"]
        + d["SV_QRIS_OFFUS"]
    )
    d["TOTAL_TRX"] = (
        d["TRX_DEBIT_ONUS"]
        + d["TRX_DEBIT_OFFUS"]
        + d["TRX_CREDIT_OFFUS"]
        + d["TRX_QRIS_ONUS"]
        + d["TRX_QRIS_OFFUS"]
    )
    d["TOTAL_FBI"] = (
        d["FBI_DEBIT_ONUS"]
        + d["FBI_DEBIT_OFFUS"]
        + d["FBI_CREDIT_OFFUS"]
        + d["FBI_QRIS_ONUS"]
        + d["FBI_QRIS_OFFUS"]
    )
    d["RASIO_ONUS"] = np.where(d["TOTAL_SV"] > 0, d["SV_DEBIT_ONUS"] / d["TOTAL_SV"], 0.0)
    return d


def _year_scale(src: pd.DataFrame, year: int, scale: float) -> pd.DataFrame:
    if src.empty:
        return src
    out = src.copy()
    mo = out["TRX_MONTH"].astype(int) % 100
    out["TRX_MONTH"] = year * 100 + mo
    out["YEAR"] = year
    for c in _num_cols(out):
        noise = RNG.uniform(0.97, 1.03, size=len(out))
        if c.startswith("TRX_"):
            out[c] = (out[c] * scale * noise).round(0).astype(np.int64)
        else:
            out[c] = (out[c] * scale * noise).round(2)
    return _totals_block(out)


def expand_history(df_2026_src: pd.DataFrame) -> pd.DataFrame:
    """Full 2026 months (filled from last actual month if missing) + scaled 2024/2025 copies."""
    cur = df_2026_src[df_2026_src["YEAR"] == 2026].copy()
    if cur.empty:
        cur = df_2026_src.copy()
    months_have = set(int(x) for x in cur["TRX_MONTH"].dropna().unique())
    template_ym = max(months_have) if months_have else 202603
    tmpl = cur[cur["TRX_MONTH"] == template_ym].copy()
    if tmpl.empty:
        tmpl = cur.iloc[: max(1, len(cur) // 3)].copy()

    filled = []
    for m in range(1, 13):
        ym = 202600 + m
        chunk = cur[cur["TRX_MONTH"] == ym].copy()
        if chunk.empty:
            bump = 0.97 ** max(0, m - (template_ym % 100))
            chunk = tmpl.copy()
            chunk["TRX_MONTH"] = ym
            chunk["YEAR"] = 2026
            for c in _num_cols(chunk):
                if c.startswith("TRX_"):
                    chunk[c] = (
                        chunk[c] * bump * RNG.uniform(0.96, 1.04, size=len(chunk))
                    ).round(0).astype(np.int64)
                else:
                    chunk[c] = (chunk[c] * bump * RNG.uniform(0.96, 1.04, size=len(chunk))).round(2)
        filled.append(chunk)
    full_2026 = pd.concat(filled, ignore_index=True)
    full_2026 = _totals_block(full_2026)

    out = pd.concat(
        [
            _year_scale(full_2026, 2024, SCALE_2024),
            _year_scale(full_2026, 2025, SCALE_2025),
            full_2026,
        ],
        ignore_index=True,
    )
    return out.drop_duplicates(
        subset=["MERCHANT_GROUP", "MERCHANT_ANCHOR", "TRX_MONTH"], keep="last"
    )


def build_raw_master(df_detail: pd.DataFrame) -> pd.DataFrame:
    """One row per MERCHANT_GROUP (matches 01_extract_and_clean raw_master)."""
    agg = (
        df_detail.groupby("MERCHANT_GROUP", as_index=False)
        .agg(MERCHANT_BRAND=("MERCHANT_ANCHOR", "first"), TOTAL_MID=("MERCHANT_ANCHOR", "nunique"))
    )
    agg["SEGMEN"] = "ANCHOR"
    agg["EQUIP_TYPES"] = "EDC, QRIS"
    agg["TOTAL_MID"] = (agg["TOTAL_MID"] * 8).clip(lower=1).astype(int)
    return agg


def build_agg_card_share(df_detail: pd.DataFrame) -> pd.DataFrame:
    """YTD-style 2026 aggregate for raw_card_share (dashboard + ML)."""
    d26 = df_detail[df_detail["YEAR"] == 2026].copy()
    if d26.empty:
        d26 = df_detail.copy()
    out = d26.groupby("MERCHANT_GROUP").agg(
        TOTAL_SV=("TOTAL_SV", "sum"),
        TOTAL_TRX=("TOTAL_TRX", "sum"),
        TOTAL_FBI=("TOTAL_FBI", "sum"),
        SV_ONUS=("SV_DEBIT_ONUS", "sum"),
        RASIO_ONUS=("RASIO_ONUS", "mean"),
        N_BULAN=("TRX_MONTH", "nunique"),
        BULAN_TERAKHIR=("TRX_MONTH", "max"),
    ).reset_index()
    return out


def build_card_history(df_detail: pd.DataFrame) -> pd.DataFrame:
    return (
        df_detail.groupby(["MERCHANT_GROUP", "TRX_MONTH", "YEAR"], as_index=False)
        .agg(TOTAL_SV=("TOTAL_SV", "sum"), TOTAL_TRX=("TOTAL_TRX", "sum"), TOTAL_FBI=("TOTAL_FBI", "sum"))
    )


def parse_weekly_wide(path_primary: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (raw_monitoring, raw_weekly long) compatible with 01_extract_and_clean.
    Prefers NEW_WeeklyMonitor (more weeks) when present.
    """
    path = PATH_WEEK_RICH if os.path.isfile(PATH_WEEK_RICH) else path_primary
    df = pd.read_csv(path)
    df["MERCHANT_GROUP"] = df["MERCHANT_GROUP"].astype(str).str.strip().str.upper()

    # Discover week indices from columns like "TRX Week 01" or "TRX_Week 01 2026"
    week_to_cols: dict[int, dict[str, str]] = {}
    for col in df.columns:
        if col == "MERCHANT_GROUP":
            continue
        m = re.search(r"Week\s*0*(\d+)", col, re.I)
        if not m:
            continue
        w = int(m.group(1))
        week_to_cols.setdefault(w, {})
        c_up = col.upper()
        if c_up.startswith("TRX"):
            week_to_cols[w]["TRX"] = col
        elif c_up.startswith("VOL"):
            week_to_cols[w]["VOL"] = col
        elif c_up.startswith("FBI"):
            week_to_cols[w]["FBI"] = col

    weeks = sorted(week_to_cols.keys())
    rows_long = []
    for _, r in df.iterrows():
        mg = r["MERCHANT_GROUP"]
        for w in weeks:
            trx = pd.to_numeric(r.get(week_to_cols[w].get("TRX"), 0), errors="coerce") or 0
            vol = pd.to_numeric(r.get(week_to_cols[w].get("VOL"), 0), errors="coerce") or 0
            fbi = pd.to_numeric(r.get(week_to_cols[w].get("FBI"), 0), errors="coerce") or 0
            if trx == 0 and vol == 0 and fbi == 0:
                continue
            rows_long.append(
                {
                    "MERCHANT_GROUP": mg,
                    "PM": "ANCHOR_PM",
                    "WEEK": w,
                    "WEEKLY_TRX": float(trx),
                    "WEEKLY_VOL": float(vol),
                    "WEEKLY_FBI": float(fbi),
                }
            )

    if not rows_long:
        return pd.DataFrame(), pd.DataFrame()

    wdf = pd.DataFrame(rows_long)
    # PM per merchant from first row
    pm_map = wdf.groupby("MERCHANT_GROUP")["PM"].first()
    vol_df = wdf.pivot_table(
        index="MERCHANT_GROUP", columns="WEEK", values="WEEKLY_VOL", aggfunc="sum"
    ).sort_index(axis=1)

    mon_rows = []
    for mg in vol_df.index:
        ser = vol_df.loc[mg].dropna()
        ser = ser[ser > 0]
        if ser.empty:
            continue
        w_first = int(ser.index.min())
        w_last = int(ser.index.max())
        mon_rows.append(
            {
                "MERCHANT_GROUP": mg,
                "YTD_VOL": float(ser.sum()),
                "VOL_WEEK_PERTAMA": float(ser.iloc[0]),
                "VOL_WEEK_TERAKHIR": float(ser.iloc[-1]),
                "WEEKS_ACTIVE": int((vol_df.loc[mg].fillna(0) > 0).sum()),
                "PM": pm_map.get(mg, "ANCHOR_PM"),
                "SV_GROWTH_RATE": float(
                    (ser.iloc[-1] - ser.iloc[0]) / ser.iloc[0] if ser.iloc[0] else 0.0
                ),
            }
        )
    raw_mon = pd.DataFrame(mon_rows)
    wlong = pd.DataFrame(rows_long)
    return raw_mon, wlong


def build_target(raw_card: pd.DataFrame) -> pd.DataFrame:
    """Synthetic FY2026 targets from observed YTD (demo)."""
    t = raw_card[
        ["MERCHANT_GROUP", "TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI"]
    ].copy()
    t["PM"] = "ANCHOR_PM"
    t["TARGET_VOL_2026"] = t["TOTAL_SV"] * 1.12
    t["TARGET_TRX_2026"] = t["TOTAL_TRX"] * 1.08
    t["TARGET_FBI_2026"] = t["TOTAL_FBI"] * 1.10
    return t[["MERCHANT_GROUP", "PM", "TARGET_VOL_2026", "TARGET_TRX_2026", "TARGET_FBI_2026"]]


def load_mid_optional() -> pd.DataFrame | None:
    if not os.path.isfile(PATH_MID):
        return None
    m = pd.read_csv(PATH_MID, dtype={"MERCHANT_ID": str})
    m.columns = [c.strip().upper() for c in m.columns]
    m["MERCHANT_GROUP"] = np.nan
    m["SEGMEN"] = "ANCHOR"
    m["FETCHED_AT"] = datetime.now().isoformat(timespec="seconds")
    return m[
        ["MERCHANT_ID", "MERCHANT_NAME", "EQUIP", "MERCHANT_GROUP", "SEGMEN", "FETCHED_AT"]
    ]


def main() -> None:
    os.makedirs(os.path.dirname(PATH_DB), exist_ok=True)
    if not os.path.isfile(PATH_CARD):
        raise FileNotFoundError(f"Missing {PATH_CARD}")

    print("Loading card share CSV...")
    raw = load_card_normalized(PATH_CARD)
    print(f"  Rows (raw): {len(raw)}")
    print("Expanding 2024-2026 history...")
    full = expand_history(raw)
    print(f"  Rows (expanded detail): {len(full)}")

    raw_master = build_raw_master(full)
    raw_card_share = build_agg_card_share(full)
    raw_hist = build_card_history(full)
    raw_target = build_target(raw_card_share)

    print("Parsing weekly monitoring...")
    raw_mon, raw_week_long = parse_weekly_wide(PATH_WEEK_WIDE)

    if raw_mon.empty:
        print("  Warning: no weekly rows parsed — raw_monitoring / raw_weekly will be empty.")

    mid_df = load_mid_optional()

    if os.path.exists(PATH_DB):
        os.remove(PATH_DB)
    conn = sqlite3.connect(PATH_DB)

    raw_master.to_sql("raw_master", conn, if_exists="replace", index=False)
    raw_card_share.to_sql("raw_card_share", conn, if_exists="replace", index=False)
    raw_hist.to_sql("raw_card_history", conn, if_exists="replace", index=False)
    raw_target.to_sql("raw_target", conn, if_exists="replace", index=False)
    raw_mon.to_sql("raw_monitoring", conn, if_exists="replace", index=False)
    if not raw_week_long.empty:
        raw_week_long.to_sql("raw_weekly", conn, if_exists="replace", index=False)

    # Meta for mentor workflow
    meta = pd.DataFrame(
        [
            ("built_at", datetime.now().isoformat(timespec="seconds")),
            ("source_card", os.path.basename(PATH_CARD)),
            ("source_weekly", os.path.basename(PATH_WEEK_WIDE)),
            ("note", "2024/2025 scaled from 2026 extract — replace with Oracle extracts when available"),
        ],
        columns=["k", "v"],
    )
    meta.to_sql("meta_staging_build", conn, if_exists="replace", index=False)

    if mid_df is not None:
        mid_df.to_sql("src_mid_fetch", conn, if_exists="replace", index=False)
        print(f"  src_mid_fetch: {len(mid_df)} terminals (MERCHANT_GROUP nullable for matching).")

    conn.close()

    print(f"\nDone: {PATH_DB}")
    print(f"  raw_master: {len(raw_master)}")
    print(f"  raw_card_share: {len(raw_card_share)}")
    print(f"  raw_card_history: {len(raw_hist)}")
    print(f"  raw_target: {len(raw_target)}")
    print(f"  raw_monitoring: {len(raw_mon)}")
    print("\nNext: run 02_transform_and_ml.py to refresh mart_merchant_cluster.")


if __name__ == "__main__":
    main()
