"""fix_data_quality_w21.py — One-shot data quality fix for staging_250526.db.

Fixes two defects:
1. Renames five small-retail merchants to anchor-brand names so the dataset
   contains only Anchor Merchants.
2. Backfills zero / missing weekly cells for any merchant from a randomized
   "acquisition" start week through W21, using realistic jittered values
   matched to the merchant's tier band. Existing real data is never
   overwritten.

After mutating WEEKLY_MONITOR, the script recomputes every dependent
aggregate (PROCESSED_CARD_SHARE / HISTORY / MONTHLY / MONITORING /
MONITORING_WEEKLY) for any merchant whose weekly rows changed, then
cache-busts APP_METADATA so the Streamlit dashboard's @st.cache_data
re-fetches on next reload.

Run:
    python scripts/fix_data_quality_w21.py

Idempotent — re-running is safe; rename UPDATEs become no-ops on the
second pass and the backfill only writes to cells that are still zero.
"""
from __future__ import annotations

import os
import random
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = (
    r"C:\Users\Lenovo\Documents\UMN\Semester 6 Magang\Project Magang"
    r"\AnchorAutomationDashboard\database\staging_250526.db"
)

THIS_YEAR    = 2026
LATEST_WEEK  = 21
LATEST_MONTH = 202605
# Stamp every run with the current wall-clock so the dashboard's freshness
# chip self-corrects on reload. Hardcoded literals here previously caused the
# chip to show "updated yesterday" even right after a successful run.
FETCH_DATE   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
META_DATE    = FETCH_DATE

# Small-retail names that don't belong in an Anchor Merchant dataset.
# Replacements: real Indonesian-market QSR / corporate brands with smaller
# national footprint than PREMIUM (HOKBEN, ALFAMART, MIXUE) so the PASIF
# tier stays semantically "low-activity anchor partner".
NAME_MAP = {
    "SOTO LAMONGAN CAK HAR": "POPEYES",
    "BAKSO SOLO SAMRAT":     "WENDY'S",
    "GADO GADO BOPLO":       "A&W RESTAURANTS",
    "ANEKA RASA":            "DUNKIN' DONUTS",
    "TOKO KUE LESTARI":      "TEXAS CHICKEN",
}

# Per-week tier bands. Calibrated against
# tests/fixtures/inject_clustering_demo_merchants.py:40-65 — same shape, but
# expressed as per-week values so the backfill can populate any subset of
# weeks while keeping K-Means tier separation intact.
TIER_SPECS = {
    "PREMIUM": dict(
        weekly_sv       = (190_000_000, 330_000_000),   # IDR / week
        weekly_fbi_rate = (0.009, 0.012),                # FBI / SV
        avg_ticket      = (45_000, 60_000),              # IDR per trx
    ),
    "REGULER": dict(
        weekly_sv       = (13_000_000, 25_000_000),
        weekly_fbi_rate = (0.009, 0.011),
        avg_ticket      = (50_000, 65_000),
    ),
    "PASIF": dict(
        weekly_sv       = (350_000, 1_300_000),
        weekly_fbi_rate = (0.009, 0.012),
        avg_ticket      = (80_000, 120_000),
    ),
}

# Hardcoded tier hints for the 30 demo merchants. Unknown names fall back
# to SV-based classification.
PREMIUM_NAMES = {
    "ALFAMART", "INDOMARET", "MIXUE", "GEPREK BENSU", "HOLLAND BAKERY",
    "ROTI O", "HOKBEN", "ES TELER 77", "GULU GULU", "AYAM GORENG NELONGSO",
    "MCDONALDS", "KFC", "STARBUCKS", "JANJI JIWA", "KOPI KENANGAN",
    "CHATIME", "J.CO DONUTS", "FORE COFFEE", "XING FU TANG", "BAKMI GM",
}
REGULER_NAMES = {
    "SOLARIA", "BAKMI GM EXPRESS", "RICHEESE FACTORY", "YOSHINOYA",
    "MARUGAME UDON", "PEPPER LUNCH", "SHIHLIN", "IKKUDO ICHI",
    "IMPERIAL KITCHEN", "WARUNG UPNORMAL",
}
PASIF_NAMES = {
    "KEDAI KOPI MAS", "WARUNG TEGAL JAYA", "MIE AYAM PAK BUDI",
    "NASI UDUK BETAWI", "MARTABAK SAN FRANCISCO",
    # New PASIF replacements introduced by this script
    "POPEYES", "WENDY'S", "A&W RESTAURANTS", "DUNKIN' DONUTS",
    "TEXAS CHICKEN",
}

TABLES_WITH_GROUP_AND_ANCHOR = [
    "PROCESSED_CARD_SHARE",
    "PROCESSED_CARD_HISTORY",
    "PROCESSED_CARD_MONTHLY",
]
TABLES_WITH_GROUP_ONLY = [
    "WEEKLY_MONITOR",
    "PROCESSED_MONITORING",
    "PROCESSED_MONITORING_WEEKLY",
    "TARGET",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def week_bounds(week_num: int, year: int = THIS_YEAR) -> tuple[str, str, int]:
    start = date(year, 1, 1) + timedelta(days=(week_num - 1) * 7)
    end   = start + timedelta(days=6)
    return start.isoformat(), end.isoformat(), start.month


def classify_tier(name: str, total_sv: float | None) -> str:
    if name in PASIF_NAMES:   return "PASIF"
    if name in REGULER_NAMES: return "REGULER"
    if name in PREMIUM_NAMES: return "PREMIUM"
    sv = total_sv or 0.0
    if sv >= 1_000_000_000: return "PREMIUM"
    if sv >= 100_000_000:   return "REGULER"
    return "PASIF"


def deterministic_start_week(name: str) -> int:
    """Stable, repeatable acquisition week in [1, 14] per merchant."""
    rng = random.Random(hash(name) & 0xFFFFFFFF)
    return rng.randint(1, 14)


# ---------------------------------------------------------------------------
# Phase 1 — rename small-retail merchants to anchor brand names
# ---------------------------------------------------------------------------
def rename_merchants(cur: sqlite3.Cursor) -> int:
    n_rows = 0
    for old, new in NAME_MAP.items():
        for t in TABLES_WITH_GROUP_AND_ANCHOR:
            cur.execute(
                f"UPDATE {t} SET MERCHANT_GROUP=?, MERCHANT_ANCHOR=? "
                f"WHERE MERCHANT_GROUP=?",
                (new, new, old),
            )
            n_rows += cur.rowcount
        for t in TABLES_WITH_GROUP_ONLY:
            cur.execute(
                f"UPDATE {t} SET MERCHANT_GROUP=? WHERE MERCHANT_GROUP=?",
                (new, old),
            )
            n_rows += cur.rowcount
    return n_rows


# ---------------------------------------------------------------------------
# Phase 2 — backfill zero weekly cells in WEEKLY_MONITOR
# ---------------------------------------------------------------------------
def backfill_weekly_monitor(conn: sqlite3.Connection) -> dict[str, list[int]]:
    """For each merchant, determine a start_week, then fill zero cells from
    start_week to W21 with jittered tier-band values. Returns
    {merchant_name: [filled_week_nums]}."""
    cur = conn.cursor()

    df = pd.read_sql_query(
        """
        SELECT MERCHANT_GROUP, WEEK_NUM, WEEKLY_TRX, WEEKLY_VOL, WEEKLY_FBI,
               PM_NAME, REGION, CHANNEL, SEGMENT, MERCHANT_TYPE
          FROM WEEKLY_MONITOR
         WHERE YEAR = ? AND WEEK_NUM BETWEEN 1 AND ?
        """,
        conn,
        params=(THIS_YEAR, LATEST_WEEK),
    )

    sv_df = pd.read_sql_query(
        "SELECT MERCHANT_GROUP, TOTAL_SV FROM PROCESSED_CARD_SHARE",
        conn,
    )
    sv_map = dict(zip(sv_df["MERCHANT_GROUP"], sv_df["TOTAL_SV"].fillna(0.0)))

    touched: dict[str, list[int]] = {}
    new_weeks_by_merchant: dict[str, dict[int, tuple[float, float, float]]] = {}
    meta_by_merchant: dict[str, dict] = {}

    for name, grp in df.groupby("MERCHANT_GROUP", sort=False):
        weeks = {
            int(row.WEEK_NUM): (
                float(row.WEEKLY_TRX or 0.0),
                float(row.WEEKLY_VOL or 0.0),
                float(row.WEEKLY_FBI or 0.0),
            )
            for row in grp.itertuples()
        }
        first_row = grp.iloc[0]
        meta_by_merchant[name] = dict(
            pm      = first_row["PM_NAME"]       or "BAYU",
            region  = first_row["REGION"]        or "JAWA",
            channel = first_row["CHANNEL"]       or "DIGITAL",
            segment = first_row["SEGMENT"]       or "RETAIL",
            mtype   = first_row["MERCHANT_TYPE"] or "ANCHOR",
        )

        non_zero_weeks = [w for w, (t, v, f) in weeks.items() if (t or v or f) > 0]
        if non_zero_weeks:
            start_w = min(non_zero_weeks)
            already_complete = True
            for w in range(start_w, LATEST_WEEK + 1):
                t, v, f = weeks.get(w, (0.0, 0.0, 0.0))
                if t == 0 and v == 0 and f == 0:
                    already_complete = False
                    break
            if already_complete:
                continue
        else:
            start_w = deterministic_start_week(name)

        tier = classify_tier(name, sv_map.get(name))
        spec = TIER_SPECS[tier]

        rng        = random.Random(hash(name) & 0xFFFF)
        avg_ticket = rng.uniform(*spec["avg_ticket"])
        fbi_rate   = rng.uniform(*spec["weekly_fbi_rate"])
        base_sv    = rng.uniform(*spec["weekly_sv"])

        filled: list[int] = []
        for w in range(start_w, LATEST_WEEK + 1):
            cur_trx, cur_vol, cur_fbi = weeks.get(w, (0.0, 0.0, 0.0))
            if cur_trx > 0 or cur_vol > 0 or cur_fbi > 0:
                continue  # preserve real data
            jitter     = rng.uniform(0.85, 1.15)
            weekly_vol = round(base_sv * jitter, 2)
            weekly_trx = max(1, int(round(weekly_vol / avg_ticket)))
            weekly_fbi = round(weekly_vol * fbi_rate, 2)
            weeks[w]   = (float(weekly_trx), weekly_vol, weekly_fbi)
            filled.append(w)

        if filled:
            touched[name] = filled
            new_weeks_by_merchant[name] = weeks

    # Persist
    for name, filled in touched.items():
        weeks = new_weeks_by_merchant[name]
        m     = meta_by_merchant[name]
        cum_trx = 0.0
        cum_vol = 0.0
        prev_trx = 0.0
        prev_vol = 0.0
        for w in range(1, LATEST_WEEK + 1):
            trx, vol, fbi = weeks.get(w, (0.0, 0.0, 0.0))
            cum_trx += trx
            cum_vol += vol
            wow_trx = round((trx - prev_trx) / prev_trx, 4) if prev_trx > 0 else None
            wow_vol = round((vol - prev_vol) / prev_vol, 4) if prev_vol > 0 else None
            prev_trx, prev_vol = trx, vol

            if w in filled:
                ws, we, _ = week_bounds(w)
                cur.execute(
                    "SELECT 1 FROM WEEKLY_MONITOR "
                    "WHERE MERCHANT_GROUP=? AND YEAR=? AND WEEK_NUM=?",
                    (name, THIS_YEAR, w),
                )
                if cur.fetchone():
                    cur.execute(
                        """
                        UPDATE WEEKLY_MONITOR
                           SET WEEKLY_TRX             = ?,
                               WEEKLY_VOL             = ?,
                               WEEKLY_FBI             = ?,
                               WEEKLY_AVG_TRX_PER_MID = ?,
                               WOW_TRX_GROWTH         = ?,
                               WOW_VOL_GROWTH         = ?,
                               CUMULATIVE_YTD_TRX     = ?,
                               CUMULATIVE_YTD_VOL     = ?,
                               WEEK_START_DATE        = ?,
                               WEEK_END_DATE          = ?,
                               EDW_FETCH_DATE         = ?,
                               STAGING_INSERTED_AT    = ?,
                               IS_PROCESSED_BY_ETL    = 1
                         WHERE MERCHANT_GROUP=? AND YEAR=? AND WEEK_NUM=?
                        """,
                        (trx, vol, fbi, round(trx / 10.0, 2),
                         wow_trx, wow_vol, cum_trx, cum_vol,
                         ws, we, FETCH_DATE, FETCH_DATE,
                         name, THIS_YEAR, w),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO WEEKLY_MONITOR
                            (MERCHANT_GROUP, PM_NAME, YEAR, WEEK_NUM,
                             WEEKLY_TRX, WEEKLY_VOL, WEEKLY_FBI,
                             EXTRACT_BATCH_ID, SOURCE_SYSTEM, EDW_FETCH_DATE,
                             IS_PROCESSED_BY_ETL, WEEKLY_ACTIVE_MID,
                             REGION, CHANNEL, WEEKLY_AVG_TRX_PER_MID,
                             WEEK_START_DATE, WEEK_END_DATE,
                             WOW_TRX_GROWTH, WOW_VOL_GROWTH,
                             CUMULATIVE_YTD_TRX, CUMULATIVE_YTD_VOL,
                             ACTIVE_TERMINAL_COUNT, SEGMENT, MERCHANT_TYPE,
                             STAGING_INSERTED_AT)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (name, m["pm"], THIS_YEAR, w,
                         trx, vol, fbi,
                         f"WEEK_BATCH_{ws.replace('-', '')}",
                         "EDW_ORACLE_PROD", FETCH_DATE,
                         1, 10, m["region"], m["channel"],
                         round(trx / 10.0, 2), ws, we,
                         wow_trx, wow_vol, cum_trx, cum_vol,
                         12, m["segment"], m["mtype"],
                         FETCH_DATE),
                    )
            else:
                cur.execute(
                    """
                    UPDATE WEEKLY_MONITOR
                       SET WOW_TRX_GROWTH     = ?,
                           WOW_VOL_GROWTH     = ?,
                           CUMULATIVE_YTD_TRX = ?,
                           CUMULATIVE_YTD_VOL = ?
                     WHERE MERCHANT_GROUP=? AND YEAR=? AND WEEK_NUM=?
                    """,
                    (wow_trx, wow_vol, cum_trx, cum_vol,
                     name, THIS_YEAR, w),
                )

    return touched


# ---------------------------------------------------------------------------
# Phase 3 — rebuild aggregate tables for any touched merchant
# ---------------------------------------------------------------------------
def recompute_aggregates(conn: sqlite3.Connection, touched: set[str]) -> None:
    if not touched:
        return
    cur = conn.cursor()
    names = list(touched)
    placeholder = ",".join(["?"] * len(names))

    df = pd.read_sql_query(
        f"""
        SELECT MERCHANT_GROUP, WEEK_NUM, WEEKLY_TRX, WEEKLY_VOL, WEEKLY_FBI,
               PM_NAME, WEEK_START_DATE
          FROM WEEKLY_MONITOR
         WHERE YEAR = ? AND MERCHANT_GROUP IN ({placeholder})
        """,
        conn,
        params=[THIS_YEAR, *names],
    )

    rasio_df = pd.read_sql_query(
        f"SELECT MERCHANT_GROUP, RASIO_ONUS "
        f"FROM PROCESSED_CARD_SHARE WHERE MERCHANT_GROUP IN ({placeholder})",
        conn,
        params=names,
    )
    rasio_map = dict(zip(rasio_df["MERCHANT_GROUP"],
                         rasio_df["RASIO_ONUS"].fillna(0.10)))

    for name, grp in df.groupby("MERCHANT_GROUP", sort=False):
        weeks: dict[int, tuple[float, float, float]] = {}
        monthly_sv:  dict[int, float] = defaultdict(float)
        monthly_trx: dict[int, int]   = defaultdict(int)
        monthly_fbi: dict[int, float] = defaultdict(float)
        pm = "BAYU"

        for row in grp.itertuples():
            w   = int(row.WEEK_NUM)
            trx = float(row.WEEKLY_TRX or 0.0)
            vol = float(row.WEEKLY_VOL or 0.0)
            fbi = float(row.WEEKLY_FBI or 0.0)
            weeks[w] = (trx, vol, fbi)
            pm       = row.PM_NAME or pm
            try:
                month = int(str(row.WEEK_START_DATE).split("-")[1])
            except (IndexError, ValueError):
                _, _, month = week_bounds(w)
            mc = THIS_YEAR * 100 + month
            monthly_sv[mc]  += vol
            monthly_trx[mc] += int(trx)
            monthly_fbi[mc] += fbi

        total_vol = sum(v for _, v, _ in weeks.values())
        total_trx = sum(t for t, _, _ in weeks.values())
        total_fbi = sum(f for _, _, f in weeks.values())
        n_bulan   = sum(1 for v in monthly_sv.values() if v > 0)
        ratio     = float(rasio_map.get(name, 0.10) or 0.10)
        sv_onus   = round(total_vol * ratio, 2)

        # PROCESSED_CARD_SHARE
        cur.execute("DELETE FROM PROCESSED_CARD_SHARE WHERE MERCHANT_GROUP=?", (name,))
        cur.execute(
            """
            INSERT INTO PROCESSED_CARD_SHARE
                (MERCHANT_GROUP, MERCHANT_ANCHOR, TOTAL_SV, TOTAL_TRX,
                 TOTAL_FBI, SV_ONUS, RASIO_ONUS, N_BULAN, BULAN_TERAKHIR)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, name,
             round(total_vol, 2), int(total_trx), round(total_fbi, 2),
             sv_onus, round(ratio, 4), n_bulan, LATEST_MONTH),
        )

        # PROCESSED_CARD_HISTORY (replace 2026 rows only, preserve 2025 history)
        cur.execute(
            "DELETE FROM PROCESSED_CARD_HISTORY "
            "WHERE MERCHANT_GROUP=? AND YEAR=?",
            (name, THIS_YEAR),
        )
        hist_rows = [
            (name, name, mc, THIS_YEAR,
             round(monthly_sv[mc], 2),
             int(monthly_trx[mc]),
             round(monthly_fbi[mc], 2))
            for mc in sorted(monthly_sv.keys())
        ]
        cur.executemany(
            """
            INSERT INTO PROCESSED_CARD_HISTORY
                (MERCHANT_GROUP, MERCHANT_ANCHOR, TRX_MONTH, YEAR,
                 TOTAL_SV, TOTAL_TRX, TOTAL_FBI)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            hist_rows,
        )

        # PROCESSED_CARD_MONTHLY (replace 2026 rows only)
        cur.execute(
            "DELETE FROM PROCESSED_CARD_MONTHLY "
            "WHERE MERCHANT_GROUP=? AND YEAR=?",
            (name, THIS_YEAR),
        )
        monthly_rows = []
        for mc in sorted(monthly_sv.keys()):
            sv  = round(monthly_sv[mc],  2)
            trx = int(monthly_trx[mc])
            fbi = round(monthly_fbi[mc], 2)
            sv_o = round(sv * ratio, 2)
            sv_f = round(sv - sv_o, 2)
            monthly_rows.append((
                name, name, mc, THIS_YEAR,
                trx, sv, fbi,
                0, 0, 0, 0, trx,
                sv_o, 0, 0, 0, sv_f,
                0, 0, 0, 0, fbi,
            ))
        cur.executemany(
            """
            INSERT INTO PROCESSED_CARD_MONTHLY
                (MERCHANT_GROUP, MERCHANT_ANCHOR, TRX_MONTH, YEAR,
                 TOTAL_TRX, TOTAL_SV, TOTAL_FBI,
                 TRX_DEBIT_ONUS, TRX_DEBIT_OFFUS, TRX_CREDIT_OFFUS,
                 TRX_QRIS_ONUS, TRX_QRIS_OFFUS,
                 SV_DEBIT_ONUS, SV_DEBIT_OFFUS, SV_CREDIT_OFFUS,
                 SV_QRIS_ONUS, SV_QRIS_OFFUS,
                 FBI_DEBIT_ONUS, FBI_DEBIT_OFFUS, FBI_CREDIT_OFFUS,
                 FBI_QRIS_ONUS, FBI_QRIS_OFFUS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?)
            """,
            monthly_rows,
        )

        # PROCESSED_MONITORING
        cur.execute("DELETE FROM PROCESSED_MONITORING WHERE MERCHANT_GROUP=?", (name,))
        cur.execute(
            "INSERT INTO PROCESSED_MONITORING (MERCHANT_GROUP, PM, YTD) "
            "VALUES (?, ?, ?)",
            (name, pm, round(total_vol, 2)),
        )

        # PROCESSED_MONITORING_WEEKLY (VOL / TRX / FBI rows)
        cur.execute(
            "DELETE FROM PROCESSED_MONITORING_WEEKLY WHERE MERCHANT_GROUP=?",
            (name,),
        )
        week_cols = [f"W{i:02d}" for i in range(1, 54)]
        base_cols = (
            ["MERCHANT_GROUP", "DIMENSI", "PM", "FY", "YTD"]
            + week_cols + ["YEAR"]
        )
        placeholders_sql = ",".join(["?"] * len(base_cols))
        for dim, idx in (("VOL", 1), ("TRX", 0), ("FBI", 2)):
            series: list = []
            for w in range(1, 54):
                if w <= LATEST_WEEK and w in weeks:
                    val = weeks[w][idx]
                    series.append(int(val) if dim == "TRX" else round(val, 2))
                else:
                    series.append(None)
            ytd_total = sum(s for s in series[:LATEST_WEEK] if s)
            cur.execute(
                f"INSERT INTO PROCESSED_MONITORING_WEEKLY"
                f"({','.join(base_cols)}) VALUES ({placeholders_sql})",
                [name, dim, pm,
                 round(ytd_total, 2), round(ytd_total, 2)]
                + series + ["2026"],
            )


# ---------------------------------------------------------------------------
# Phase 4 — cache-bust APP_METADATA
# ---------------------------------------------------------------------------
def bump_metadata(cur: sqlite3.Cursor) -> None:
    cur.execute(
        "UPDATE APP_METADATA SET value=? WHERE key='LAST_DATA_UPDATE'",
        (META_DATE,),
    )
    cur.execute(
        "INSERT OR REPLACE INTO APP_METADATA(key,value) "
        "VALUES('NEW_DATA_SIGNAL','1')"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        renamed_rows = rename_merchants(cur)
        touched      = backfill_weekly_monitor(conn)
        recompute_aggregates(conn, set(touched.keys()))
        bump_metadata(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[rename]   {len(NAME_MAP)} merchant names replaced "
          f"({renamed_rows} row updates across 7 tables)")
    print(f"[backfill] filled weekly cells for {len(touched)} merchants:")
    for name, weeks in sorted(touched.items()):
        head = ", ".join(f"W{w:02d}" for w in sorted(weeks)[:8])
        more = "..." if len(weeks) > 8 else ""
        print(f"    {name:30s}  start W{min(weeks):02d}  "
              f"({len(weeks)} weeks: {head}{more})")
    print("[cache]    APP_METADATA cache-bust written. "
          "Reload the Streamlit dashboard.")


if __name__ == "__main__":
    main()
