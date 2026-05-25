"""
Injects 30 synthetic merchants (10 PREMIUM / 10 REGULER / 10 PASIF) directly
into the live staging SQLite store so the dashboard's K-Means clustering yields
three visually distinct, statistically cohesive tiers on the PCA scatter plot.

Run:
    python tests/fixtures/inject_clustering_demo_merchants.py

Additive mode: existing real merchants and the 10 alert-system fixture
merchants are left untouched. Re-running this script is idempotent — every
synthetic merchant is deleted by name first, then re-inserted.

The tier centroids are calibrated against the feature engineering in
pages/4_Dashboard.py:359-431 (log1p on AVG_SV/AVG_FBI, StandardScaler, K=3,
composite ranking 0.60*AVG_SV + 0.25*ACHIEVEMENT + 0.15*GROWTH).
"""
from __future__ import annotations
import os
import random
import sqlite3
from datetime import date, timedelta

DB_PATH = (
    r"C:\Users\Lenovo\Documents\UMN\Semester 6 Magang\Project Magang"
    r"\AnchorAutomationDashboard\database\staging_250526.db"
)

THIS_YEAR    = 2026
LATEST_WEEK  = 21              # W21 = 2026-05-21..2026-05-27
LATEST_MONTH = 202605
FETCH_DATE   = "2026-05-25 00:00:00"
META_DATE    = "2026-05-25 12:00:00"

random.seed(42)

# ---------------------------------------------------------------------------
# Tier specifications — each centroid + jitter band is chosen so the
# log-scaled features land ~1.5–2 σ apart, with no overlap on RASIO_ONUS.
# ---------------------------------------------------------------------------
TIER_SPECS = {
    "PREMIUM": dict(
        sv_range     = (4_000_000_000, 7_000_000_000),
        fbi_range    = (40_000_000,    70_000_000),
        onus_range   = (0.65,          0.80),
        achv_range   = (0.90,          1.40),       # target = sv / achv
        weeks_active = LATEST_WEEK,                  # all 21 weeks populated
        trend_factor = 1.06,                         # +6% MoM (rising)
    ),
    "REGULER": dict(
        sv_range     = (250_000_000,   450_000_000),
        fbi_range    = (2_500_000,     4_500_000),
        onus_range   = (0.35,          0.50),
        achv_range   = (0.40,          0.65),
        weeks_active = 18,                           # W04..W21 populated
        trend_factor = 1.00,                         # flat
    ),
    "PASIF": dict(
        sv_range     = (2_000_000,     8_000_000),
        fbi_range    = (20_000,        80_000),
        onus_range   = (0.05,          0.18),
        achv_range   = (0.05,          0.20),
        weeks_active = 6,                            # W16..W21 populated
        trend_factor = 0.80,                         # -20% MoM (declining)
    ),
}

# Indonesian F&B / retail brands. Confirmed NOT to collide with the existing
# alert-fixture set: KOPI KENANGAN, JANJI JIWA, MCDONALDS, KFC, CHATIME,
# J.CO DONUTS, STARBUCKS, FORE COFFEE, XING FU TANG, BAKMI GM.
TIER_NAMES = {
    "PREMIUM": [
        "ALFAMART", "INDOMARET", "MIXUE", "GEPREK BENSU", "HOLLAND BAKERY",
        "ROTI O", "HOKBEN", "ES TELER 77", "GULU GULU", "AYAM GORENG NELONGSO",
    ],
    "REGULER": [
        "SOLARIA", "BAKMI GM EXPRESS", "RICHEESE FACTORY", "YOSHINOYA",
        "MARUGAME UDON", "PEPPER LUNCH", "SHIHLIN", "IKKUDO ICHI",
        "IMPERIAL KITCHEN", "WARUNG UPNORMAL",
    ],
    "PASIF": [
        "KEDAI KOPI MAS", "WARUNG TEGAL JAYA", "MIE AYAM PAK BUDI",
        "NASI UDUK BETAWI", "SOTO LAMONGAN CAK HAR", "BAKSO SOLO SAMRAT",
        "GADO GADO BOPLO", "ANEKA RASA", "TOKO KUE LESTARI",
        "MARTABAK SAN FRANCISCO",
    ],
}

PMS = ["BAYU", "RIFALDI", "NINA"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def week_bounds(week_num: int, year: int = THIS_YEAR) -> tuple[str, str]:
    start = date(year, 1, 1) + timedelta(days=(week_num - 1) * 7)
    end   = start + timedelta(days=6)
    return start.isoformat(), end.isoformat()


def _monthly_series(total: float, f: float, six_int: bool = False) -> list:
    """Spread `total` across 6 months with multiplicative trend `f`."""
    if f == 1:
        v = total / 6
        return [max(int(round(v)), 1) for _ in range(6)] if six_int else [round(v, 2)] * 6
    last = total * (f - 1) / (f**6 - 1) * f**5
    series = [last * f**(i - 5) for i in range(6)]
    return [max(int(round(x)), 1) for x in series] if six_int else [round(x, 2) for x in series]


def build_merchant_spec(name: str, tier: str, idx: int) -> dict:
    spec = TIER_SPECS[tier]
    sv   = random.uniform(*spec["sv_range"])
    fbi  = random.uniform(*spec["fbi_range"])
    onus_ratio = random.uniform(*spec["onus_range"])
    achv = random.uniform(*spec["achv_range"])

    target_vol = sv / achv
    target_trx = max(int(target_vol / 100_000), 10)
    target_fbi = fbi / achv

    f = spec["trend_factor"]
    trx_total = max(int(sv / 50_000), 10)
    monthly_sv  = _monthly_series(sv,        f)
    monthly_fbi = _monthly_series(fbi,       f)
    monthly_trx = _monthly_series(trx_total, f, six_int=True)

    weeks_active   = spec["weeks_active"]
    first_active_w = LATEST_WEEK - weeks_active + 1
    avg_w_sv  = sv  / weeks_active
    avg_w_trx = trx_total / weeks_active
    avg_w_fbi = fbi / weeks_active

    weekly_vol = [None] * LATEST_WEEK
    weekly_trx = [None] * LATEST_WEEK
    weekly_fbi = [None] * LATEST_WEEK
    for w in range(first_active_w, LATEST_WEEK + 1):
        jitter = random.uniform(0.85, 1.15)
        weekly_vol[w - 1] = round(avg_w_sv  * jitter, 2)
        weekly_trx[w - 1] = max(1, int(round(avg_w_trx * jitter)))
        weekly_fbi[w - 1] = round(avg_w_fbi * jitter, 2)

    return dict(
        name         = name,
        pm           = PMS[idx % len(PMS)],
        tier         = tier,
        ytd_sv       = round(sv,  2),
        ytd_trx      = trx_total,
        ytd_fbi      = round(fbi, 2),
        sv_onus      = round(sv * onus_ratio, 2),
        rasio_onus   = round(onus_ratio, 4),
        target_vol   = round(target_vol, 2),
        target_trx   = target_trx,
        target_fbi   = round(target_fbi, 2),
        monthly_sv   = monthly_sv,
        monthly_trx  = monthly_trx,
        monthly_fbi  = monthly_fbi,
        weekly_vol   = weekly_vol,
        weekly_trx   = weekly_trx,
        weekly_fbi   = weekly_fbi,
        weeks_active = weeks_active,
    )


# ---------------------------------------------------------------------------
# Per-table inserts (column lists mirror tests/fixtures/generate_alert_test_data.py)
# ---------------------------------------------------------------------------
def insert_card_share(cur, m):
    cur.execute("""
        INSERT INTO PROCESSED_CARD_SHARE
            (MERCHANT_GROUP, MERCHANT_ANCHOR, TOTAL_SV, TOTAL_TRX, TOTAL_FBI,
             SV_ONUS, RASIO_ONUS, N_BULAN, BULAN_TERAKHIR)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (m["name"], m["name"], m["ytd_sv"], m["ytd_trx"], m["ytd_fbi"],
          m["sv_onus"], m["rasio_onus"], 5, LATEST_MONTH))


def insert_card_history(cur, m):
    months = [202512, 202601, 202602, 202603, 202604, 202605]
    years  = [2025,   2026,   2026,   2026,   2026,   2026]
    rows = [
        (m["name"], m["name"], months[i], years[i],
         m["monthly_sv"][i], m["monthly_trx"][i], m["monthly_fbi"][i])
        for i in range(6)
    ]
    cur.executemany("""
        INSERT INTO PROCESSED_CARD_HISTORY
            (MERCHANT_GROUP, MERCHANT_ANCHOR, TRX_MONTH, YEAR,
             TOTAL_SV, TOTAL_TRX, TOTAL_FBI)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)


def insert_card_monthly(cur, m):
    months = [202512, 202601, 202602, 202603, 202604, 202605]
    years  = [2025,   2026,   2026,   2026,   2026,   2026]
    rows = []
    for i in range(6):
        sv  = m["monthly_sv"][i]
        trx = m["monthly_trx"][i]
        fbi = m["monthly_fbi"][i]
        sv_onus  = round(sv * m["rasio_onus"], 2)
        sv_offus = round(sv - sv_onus, 2)
        rows.append((
            m["name"], m["name"], months[i], years[i],
            trx, sv, fbi,
            0, 0, 0, 0, trx,
            sv_onus, 0, 0, 0, sv_offus,
            0, 0, 0, 0, fbi,
        ))
    cur.executemany("""
        INSERT INTO PROCESSED_CARD_MONTHLY
            (MERCHANT_GROUP, MERCHANT_ANCHOR, TRX_MONTH, YEAR,
             TOTAL_TRX, TOTAL_SV, TOTAL_FBI,
             TRX_DEBIT_ONUS, TRX_DEBIT_OFFUS, TRX_CREDIT_OFFUS,
             TRX_QRIS_ONUS, TRX_QRIS_OFFUS,
             SV_DEBIT_ONUS, SV_DEBIT_OFFUS, SV_CREDIT_OFFUS,
             SV_QRIS_ONUS, SV_QRIS_OFFUS,
             FBI_DEBIT_ONUS, FBI_DEBIT_OFFUS, FBI_CREDIT_OFFUS,
             FBI_QRIS_ONUS, FBI_QRIS_OFFUS)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)


def insert_monitoring(cur, m):
    cur.execute("""
        INSERT INTO PROCESSED_MONITORING (MERCHANT_GROUP, PM, YTD)
        VALUES (?, ?, ?)
    """, (m["name"], m["pm"], m["ytd_sv"]))


def insert_monitoring_weekly(cur, m):
    week_cols = [f"W{i:02d}" for i in range(1, 54)]
    base_cols = ["MERCHANT_GROUP", "DIMENSI", "PM", "FY", "YTD"] + week_cols + ["YEAR"]
    placeholders = ",".join(["?"] * len(base_cols))
    sql = f"INSERT INTO PROCESSED_MONITORING_WEEKLY({','.join(base_cols)}) VALUES ({placeholders})"
    for dim, series, ytd_total in [
        ("VOL", m["weekly_vol"], m["ytd_sv"]),
        ("TRX", m["weekly_trx"], m["ytd_trx"]),
        ("FBI", m["weekly_fbi"], m["ytd_fbi"]),
    ]:
        weeks_data = list(series) + [None] * (53 - len(series))
        row = [m["name"], dim, m["pm"], ytd_total, ytd_total] + weeks_data + ["2026"]
        cur.execute(sql, row)


def insert_target(cur, m):
    cur.execute("""
        INSERT INTO TARGET
            (MERCHANT_GROUP, PM,
             FBI_2025, TARGET_FBI_2026,
             TRX_2025, TARGET_TRX_2026,
             VOL_2025, TARGET_VOL_2026)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (m["name"], m["pm"],
          round(m["target_fbi"] * 0.9, 2), m["target_fbi"],
          int(m["target_trx"] * 0.9),      m["target_trx"],
          round(m["target_vol"] * 0.9, 2), m["target_vol"]))


def insert_weekly_monitor(cur, m):
    cols = ["MERCHANT_GROUP", "PM_NAME", "YEAR", "WEEK_NUM",
            "WEEKLY_TRX", "WEEKLY_VOL", "WEEKLY_FBI",
            "EXTRACT_BATCH_ID", "SOURCE_SYSTEM", "EDW_FETCH_DATE",
            "IS_PROCESSED_BY_ETL", "WEEKLY_ACTIVE_MID", "REGION", "CHANNEL",
            "WEEKLY_AVG_TRX_PER_MID", "WEEK_START_DATE", "WEEK_END_DATE",
            "WOW_TRX_GROWTH", "WOW_VOL_GROWTH",
            "CUMULATIVE_YTD_TRX", "CUMULATIVE_YTD_VOL",
            "ACTIVE_TERMINAL_COUNT", "SEGMENT", "MERCHANT_TYPE",
            "STAGING_INSERTED_AT"]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO WEEKLY_MONITOR({','.join(cols)}) VALUES ({placeholders})"
    cum_trx = 0.0
    cum_vol = 0.0
    prev_vol = None
    prev_trx = None
    rows = []
    for w in range(1, LATEST_WEEK + 1):
        ws, we = week_bounds(w)
        vol = float(m["weekly_vol"][w - 1] or 0)
        trx = float(m["weekly_trx"][w - 1] or 0)
        fbi = float(m["weekly_fbi"][w - 1] or 0)
        cum_trx += trx
        cum_vol += vol
        wow_trx = None if prev_trx in (None, 0) else round((trx - prev_trx) / prev_trx, 4)
        wow_vol = None if prev_vol in (None, 0) else round((vol - prev_vol) / prev_vol, 4)
        prev_trx, prev_vol = trx, vol
        rows.append((
            m["name"], m["pm"], THIS_YEAR, w,
            trx, vol, fbi,
            f"WEEK_BATCH_{ws.replace('-', '')}", "EDW_ORACLE_PROD", FETCH_DATE,
            1, 10, "JAWA", "DIGITAL",
            round(trx / 10.0, 2) if trx else 0.0,
            ws, we, wow_trx, wow_vol,
            cum_trx, cum_vol, 12, "RETAIL", "ANCHOR",
            FETCH_DATE,
        ))
    cur.executemany(sql, rows)


# ---------------------------------------------------------------------------
# Cleanup + cache bust + main
# ---------------------------------------------------------------------------
TABLES = [
    "PROCESSED_CARD_SHARE", "PROCESSED_CARD_HISTORY",
    "PROCESSED_CARD_MONTHLY", "PROCESSED_MONITORING",
    "PROCESSED_MONITORING_WEEKLY", "TARGET", "WEEKLY_MONITOR",
]


def cleanup(cur, names: list[str]) -> None:
    placeholders = ",".join(["?"] * len(names))
    for t in TABLES:
        cur.execute(f"DELETE FROM {t} WHERE MERCHANT_GROUP IN ({placeholders})", names)


def bump_metadata(cur) -> None:
    cur.execute("UPDATE APP_METADATA SET value=? WHERE key='LAST_DATA_UPDATE'", (META_DATE,))
    cur.execute("INSERT OR REPLACE INTO APP_METADATA(key,value) VALUES('NEW_DATA_SIGNAL','1')")


def main() -> None:
    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB not found: {DB_PATH}")

    merchants: list[dict] = []
    for tier in ("PREMIUM", "REGULER", "PASIF"):
        for i, name in enumerate(TIER_NAMES[tier]):
            merchants.append(build_merchant_spec(name, tier, i))

    all_names = [m["name"] for m in merchants]

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cleanup(cur, all_names)
        for m in merchants:
            insert_card_share(cur, m)
            insert_card_history(cur, m)
            insert_card_monthly(cur, m)
            insert_monitoring(cur, m)
            insert_monitoring_weekly(cur, m)
            insert_target(cur, m)
            insert_weekly_monitor(cur, m)
        bump_metadata(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    by_tier: dict[str, list[dict]] = {"PREMIUM": [], "REGULER": [], "PASIF": []}
    for m in merchants:
        by_tier[m["tier"]].append(m)

    print(f"Injected {len(merchants)} merchants into {DB_PATH}")
    for tier, ms in by_tier.items():
        svs   = [m["ytd_sv"] for m in ms]
        onuss = [m["rasio_onus"] for m in ms]
        achvs = [m["ytd_sv"] / m["target_vol"] * 100 for m in ms]
        print(f"  {tier:7s} n={len(ms):2d}  "
              f"TOTAL_SV {min(svs):>16,.0f} .. {max(svs):>16,.0f}  "
              f"RASIO_ONUS {min(onuss):.2f}..{max(onuss):.2f}  "
              f"ACHV% {min(achvs):>5.1f}..{max(achvs):>5.1f}")
    print("Reload the dashboard -> Clustering tab to see the new PCA scatter & silhouette.")


if __name__ == "__main__":
    main()
