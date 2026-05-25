"""
Generates two SQL deliverables for the BTN Anchor Automation Dashboard
alert-system QA harness:

    tests/fixtures/inject_alert_test_data.sql   -- writes 10 synthetic merchants
    tests/fixtures/rollback_alert_test_data.sql -- removes them

Run:
    python tests/fixtures/generate_alert_test_data.py

The injection script targets database/staging_<DDMMYY>.db (the live SQLite
store the dashboard rotates daily). The 10 synthetic merchants are named
after real Indonesian F&B brands that are NOT present in the current
merchant list, so the rollback uses an explicit name list (no prefix).

Calendar convention (matches existing WEEKLY_MONITOR rows):
    W01 = Jan 1..Jan 7, W21 = May 21..May 27 (contains 2026-05-25).

See C:/Users/Lenovo/.claude/plans/act-as-an-expert-calm-cupcake.md for the
full alert audit and merchant-to-alert mapping.
"""
from __future__ import annotations
import os
from datetime import date, timedelta
from textwrap import dedent

THIS_YEAR = 2026
LATEST_WEEK = 21          # W21 = 2026-05-21..2026-05-27
LATEST_MONTH = 202605
FETCH_DATE = "2026-05-25 00:00:00"
META_DATE = "2026-05-25 12:00:00"
PREV_META_DATE = "2026-04-07 16:32:54"   # what was in APP_METADATA before injection

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
INJECT_PATH = os.path.join(OUT_DIR, "inject_alert_test_data.sql")
ROLLBACK_PATH = os.path.join(OUT_DIR, "rollback_alert_test_data.sql")


def week_bounds(week_num: int, year: int = THIS_YEAR) -> tuple[str, str]:
    start = date(year, 1, 1) + timedelta(days=(week_num - 1) * 7)
    end = start + timedelta(days=6)
    return start.isoformat(), end.isoformat()


def sql_str(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def insert(table: str, cols: list[str], row: list) -> str:
    cols_csv = ",".join(cols)
    vals_csv = ",".join(sql_str(v) for v in row)
    return f"INSERT INTO {table}({cols_csv}) VALUES ({vals_csv});"


def flat_weekly(value: float, n: int = LATEST_WEEK) -> list[float]:
    return [value] * n


def custom_weekly(values: list[float]) -> list[float]:
    assert len(values) == LATEST_WEEK, f"need {LATEST_WEEK} weekly values"
    return values


# ---------------------------------------------------------------------------
# Merchant specifications
# ---------------------------------------------------------------------------
MERCHANTS = [
    dict(
        name="KOPI KENANGAN",
        pm="BAYU",
        target_vol=5_000_000_000, target_trx=50_000, target_fbi=50_000_000,
        ytd_sv=50_000_000, ytd_trx=180, ytd_fbi=400_000,
        sv_onus=15_000_000, rasio_onus=0.30,
        monthly_sv=[600_000_000, 80_000_000, 30_000_000, 15_000_000, 8_000_000, 5_000_000],
        monthly_trx=[2200, 400, 180, 90, 50, 30],
        monthly_fbi=[6_000_000, 800_000, 300_000, 150_000, 80_000, 50_000],
        weekly_vol=[round(20_000_000 * (0.92 ** i), 2) for i in range(LATEST_WEEK)],
        weekly_trx=[max(2, round(20 * (0.92 ** i), 0)) for i in range(LATEST_WEEK)],
        weekly_fbi=[round(200_000 * (0.92 ** i), 2) for i in range(LATEST_WEEK)],
    ),
    dict(
        name="JANJI JIWA",
        pm="RIFALDI",
        target_vol=4_000_000_000, target_trx=40_000, target_fbi=40_000_000,
        ytd_sv=1_600_000_000, ytd_trx=14_000, ytd_fbi=14_000_000,
        sv_onus=500_000_000, rasio_onus=0.31,
        monthly_sv=[800_000_000, 600_000_000, 500_000_000, 350_000_000, 220_000_000, 130_000_000],
        monthly_trx=[7000, 5500, 4400, 3000, 1900, 1200],
        monthly_fbi=[7_000_000, 5_500_000, 4_400_000, 3_000_000, 1_900_000, 1_200_000],
        weekly_vol=flat_weekly(80_000_000),
        weekly_trx=flat_weekly(700),
        weekly_fbi=flat_weekly(800_000),
    ),
    dict(
        name="MCDONALDS",
        pm="NINA",
        target_vol=150_000_000_000, target_trx=1_200_000, target_fbi=1_500_000_000,
        ytd_sv=180_000_000_000, ytd_trx=1_500_000, ytd_fbi=2_000_000_000,
        sv_onus=80_000_000_000, rasio_onus=0.44,
        monthly_sv=[30_000_000_000, 32_000_000_000, 34_000_000_000, 36_000_000_000, 38_000_000_000, 40_000_000_000],
        monthly_trx=[250_000, 270_000, 285_000, 300_000, 320_000, 340_000],
        monthly_fbi=[300_000_000, 320_000_000, 340_000_000, 360_000_000, 380_000_000, 400_000_000],
        weekly_vol=[round(8_500_000_000 + i * 50_000_000, 2) for i in range(LATEST_WEEK)],
        weekly_trx=[round(70_000 + i * 500, 0) for i in range(LATEST_WEEK)],
        weekly_fbi=[round(90_000_000 + i * 500_000, 2) for i in range(LATEST_WEEK)],
    ),
    dict(
        name="KFC",
        pm="NINA",
        target_vol=20_000_000_000, target_trx=200_000, target_fbi=200_000_000,
        ytd_sv=8_000_000_000, ytd_trx=20, ytd_fbi=80_000_000,
        sv_onus=7_900_000_000, rasio_onus=0.9875,
        monthly_sv=[1_600_000_000] * 6,
        monthly_trx=[4, 4, 4, 4, 4, 4],
        monthly_fbi=[16_000_000] * 6,
        weekly_vol=flat_weekly(380_000_000),
        weekly_trx=flat_weekly(1),
        weekly_fbi=flat_weekly(3_800_000),
    ),
    dict(
        name="CHATIME",
        pm="RIFALDI",
        target_vol=10_000_000_000, target_trx=80_000, target_fbi=100_000_000,
        ytd_sv=4_000_000_000, ytd_trx=30_000, ytd_fbi=40_000_000,
        sv_onus=1_300_000_000, rasio_onus=0.325,
        monthly_sv=[900_000_000, 850_000_000, 880_000_000, 870_000_000, 850_000_000, 550_000_000],
        monthly_trx=[7500, 7100, 7300, 7200, 7100, 4500],
        monthly_fbi=[8_500_000, 8_000_000, 8_300_000, 8_200_000, 8_100_000, 5_200_000],
        weekly_vol=custom_weekly([200_000_000] * 20 + [40_000_000]),
        weekly_trx=custom_weekly([1700] * 20 + [350]),
        weekly_fbi=custom_weekly([1_900_000] * 20 + [380_000]),
    ),
    dict(
        name="J.CO DONUTS",
        pm="NINA",
        target_vol=15_000_000_000, target_trx=120_000, target_fbi=150_000_000,
        ytd_sv=8_000_000_000, ytd_trx=60_000, ytd_fbi=80_000_000,
        sv_onus=2_600_000_000, rasio_onus=0.325,
        monthly_sv=[1_800_000_000, 2_000_000_000, 2_100_000_000, 2_200_000_000, 3_000_000_000, 0],
        monthly_trx=[16_000, 17_500, 18_000, 18_500, 26_000, 0],
        monthly_fbi=[18_000_000, 20_000_000, 21_000_000, 22_000_000, 30_000_000, 0],
        weekly_vol=custom_weekly([500_000_000] * 18 + [200_000_000, 100_000_000, 0]),
        weekly_trx=custom_weekly([4500] * 18 + [1800, 900, 0]),
        weekly_fbi=custom_weekly([5_000_000] * 18 + [2_000_000, 1_000_000, 0]),
    ),
    dict(
        name="STARBUCKS",
        pm="NINA",
        target_vol=2_000_000_000, target_trx=15_000, target_fbi=20_000_000,
        ytd_sv=300_000_000, ytd_trx=2_300, ytd_fbi=3_000_000,
        sv_onus=100_000_000, rasio_onus=0.333,
        monthly_sv=[0, 0, 0, 0, 0, 250_000_000],
        monthly_trx=[0, 0, 0, 0, 0, 2300],
        monthly_fbi=[0, 0, 0, 0, 0, 2_500_000],
        weekly_vol=custom_weekly([0] * 17 + [40_000_000, 60_000_000, 70_000_000, 80_000_000]),
        weekly_trx=custom_weekly([0] * 17 + [400, 550, 650, 700]),
        weekly_fbi=custom_weekly([0] * 17 + [400_000, 600_000, 700_000, 800_000]),
    ),
    dict(
        # NOTE: W21 spike must beat global mean+3*std across ALL of WEEKLY_MONITOR
        # YEAR=2026 (~4.2B std due to real big-retailer rows). 25B gives z ≈ 5.6.
        name="FORE COFFEE",
        pm="RIFALDI",
        target_vol=30_000_000_000, target_trx=200_000, target_fbi=300_000_000,
        ytd_sv=27_000_000_000, ytd_trx=29_000, ytd_fbi=275_000_000,
        sv_onus=9_000_000_000, rasio_onus=0.333,
        monthly_sv=[450_000_000, 420_000_000, 440_000_000, 430_000_000, 410_000_000, 25_300_000_000],
        monthly_trx=[3700, 3500, 3650, 3550, 3400, 14_900],
        monthly_fbi=[4_500_000, 4_200_000, 4_400_000, 4_300_000, 4_100_000, 253_000_000],
        weekly_vol=custom_weekly([100_000_000] * 20 + [25_000_000_000]),
        weekly_trx=custom_weekly([900] * 20 + [12_000]),
        weekly_fbi=custom_weekly([1_000_000] * 20 + [250_000_000]),
    ),
    dict(
        name="XING FU TANG",
        pm="RIFALDI",
        target_vol=10_000_000_000, target_trx=100_000, target_fbi=100_000_000,
        ytd_sv=9_000_000_000, ytd_trx=11, ytd_fbi=90_000_000,
        sv_onus=3_000_000_000, rasio_onus=0.333,
        monthly_sv=[1_500_000_000, 0, 0, 0, 0, 9_000_000_000],
        monthly_trx=[10, 0, 0, 0, 0, 1],
        monthly_fbi=[15_000_000, 0, 0, 0, 0, 90_000_000],
        weekly_vol=custom_weekly([0] * 20 + [9_000_000_000]),
        weekly_trx=custom_weekly([0] * 20 + [1]),
        weekly_fbi=custom_weekly([0] * 20 + [90_000_000]),
    ),
    dict(
        name="BAKMI GM",
        pm="BAYU",
        target_vol=6_000_000_000, target_trx=50_000, target_fbi=60_000_000,
        ytd_sv=30_000_000, ytd_trx=30, ytd_fbi=300_000,
        sv_onus=29_500_000, rasio_onus=0.9833,
        monthly_sv=[500_000_000, 60_000_000, 12_000_000, 6_000_000, 4_000_000, 2_000_000],
        monthly_trx=[1500, 200, 40, 20, 10, 5],
        monthly_fbi=[5_000_000, 600_000, 120_000, 60_000, 40_000, 20_000],
        weekly_vol=[round(8_000_000 * (0.88 ** i), 2) for i in range(LATEST_WEEK)],
        weekly_trx=[max(0, round(8 * (0.88 ** i), 0)) for i in range(LATEST_WEEK)],
        weekly_fbi=[round(80_000 * (0.88 ** i), 2) for i in range(LATEST_WEEK)],
    ),
]


def _name_list_sql() -> str:
    return ",".join("'" + m["name"].replace("'", "''") + "'" for m in MERCHANTS)


def _cleanup_block() -> str:
    names = _name_list_sql()
    tables = ["PROCESSED_CARD_SHARE", "PROCESSED_CARD_HISTORY",
              "PROCESSED_CARD_MONTHLY", "PROCESSED_MONITORING",
              "PROCESSED_MONITORING_WEEKLY", "TARGET", "WEEKLY_MONITOR"]
    return "\n".join(
        f"DELETE FROM {t:33s} WHERE MERCHANT_GROUP IN ({names});" for t in tables
    )


def _header() -> str:
    return dedent("""\
        -- ===========================================================================
        -- inject_alert_test_data.sql
        -- Generated by tests/fixtures/generate_alert_test_data.py
        -- Target DB: database/staging_<DDMMYY>.db (the live, daily-rotated SQLite store)
        -- Synthetic merchants are named after real Indonesian F&B brands not present
        -- in the existing merchant list. Rollback uses an explicit name list (see
        -- rollback_alert_test_data.sql).
        -- Synthetic rows are dated to 2026-05-25 / calendar week 21.
        -- ===========================================================================

        BEGIN TRANSACTION;

        -- ---------- 1. IDEMPOTENT CLEANUP -----------------------------------------
        """) + _cleanup_block() + "\n\n"


def emit_card_share(m):
    cols = ["MERCHANT_GROUP", "MERCHANT_ANCHOR", "TOTAL_SV", "TOTAL_TRX",
            "TOTAL_FBI", "SV_ONUS", "RASIO_ONUS", "N_BULAN", "BULAN_TERAKHIR"]
    row = [m["name"], m["name"], m["ytd_sv"], m["ytd_trx"], m["ytd_fbi"],
           m["sv_onus"], m["rasio_onus"], 5, LATEST_MONTH]
    return insert("PROCESSED_CARD_SHARE", cols, row)


def emit_card_history(m):
    months = [202512, 202601, 202602, 202603, 202604, 202605]
    years = [2025, 2026, 2026, 2026, 2026, 2026]
    cols = ["MERCHANT_GROUP", "MERCHANT_ANCHOR", "TRX_MONTH", "YEAR",
            "TOTAL_SV", "TOTAL_TRX", "TOTAL_FBI"]
    out = []
    for i, mo in enumerate(months):
        out.append(insert("PROCESSED_CARD_HISTORY", cols,
                          [m["name"], m["name"], mo, years[i],
                           m["monthly_sv"][i], m["monthly_trx"][i], m["monthly_fbi"][i]]))
    return "\n".join(out)


def emit_card_monthly(m):
    months = [202512, 202601, 202602, 202603, 202604, 202605]
    years = [2025, 2026, 2026, 2026, 2026, 2026]
    cols = ["MERCHANT_GROUP", "MERCHANT_ANCHOR", "TRX_MONTH", "YEAR",
            "TOTAL_TRX", "TOTAL_SV", "TOTAL_FBI",
            "TRX_DEBIT_ONUS", "TRX_DEBIT_OFFUS", "TRX_CREDIT_OFFUS",
            "TRX_QRIS_ONUS", "TRX_QRIS_OFFUS",
            "SV_DEBIT_ONUS", "SV_DEBIT_OFFUS", "SV_CREDIT_OFFUS",
            "SV_QRIS_ONUS", "SV_QRIS_OFFUS",
            "FBI_DEBIT_ONUS", "FBI_DEBIT_OFFUS", "FBI_CREDIT_OFFUS",
            "FBI_QRIS_ONUS", "FBI_QRIS_OFFUS"]
    out = []
    for i, mo in enumerate(months):
        sv = m["monthly_sv"][i]
        trx = m["monthly_trx"][i]
        fbi = m["monthly_fbi"][i]
        sv_onus = round(sv * m["rasio_onus"], 2)
        sv_offus = round(sv - sv_onus, 2)
        out.append(insert("PROCESSED_CARD_MONTHLY", cols,
                          [m["name"], m["name"], mo, years[i],
                           trx, sv, fbi,
                           0, 0, 0, 0, trx,
                           sv_onus, 0, 0, 0, sv_offus,
                           0, 0, 0, 0, fbi]))
    return "\n".join(out)


def emit_monitoring(m):
    cols = ["MERCHANT_GROUP", "PM", "YTD"]
    return insert("PROCESSED_MONITORING", cols, [m["name"], m["pm"], m["ytd_sv"]])


def emit_monitoring_weekly(m):
    week_cols = [f"W{i:02d}" for i in range(1, 54)]
    base_cols = ["MERCHANT_GROUP", "DIMENSI", "PM", "FY", "YTD"] + week_cols + ["YEAR"]
    rows = []
    for dim, series, ytd_total in [
        ("VOL", m["weekly_vol"], m["ytd_sv"]),
        ("TRX", m["weekly_trx"], m["ytd_trx"]),
        ("FBI", m["weekly_fbi"], m["ytd_fbi"]),
    ]:
        weeks_data = list(series) + [None] * (53 - len(series))
        row = [m["name"], dim, m["pm"], ytd_total, ytd_total] + weeks_data + ["2026"]
        rows.append(insert("PROCESSED_MONITORING_WEEKLY", base_cols, row))
    return "\n".join(rows)


def emit_target(m):
    cols = ["MERCHANT_GROUP", "PM",
            "FBI_2025", "TARGET_FBI_2026",
            "TRX_2025", "TARGET_TRX_2026",
            "VOL_2025", "TARGET_VOL_2026"]
    row = [m["name"], m["pm"],
           m["target_fbi"] * 0.9, m["target_fbi"],
           m["target_trx"] * 0.9, m["target_trx"],
           m["target_vol"] * 0.9, m["target_vol"]]
    return insert("TARGET", cols, row)


def emit_weekly_monitor(m):
    cols = ["MERCHANT_GROUP", "PM_NAME", "YEAR", "WEEK_NUM",
            "WEEKLY_TRX", "WEEKLY_VOL", "WEEKLY_FBI",
            "EXTRACT_BATCH_ID", "SOURCE_SYSTEM", "EDW_FETCH_DATE",
            "IS_PROCESSED_BY_ETL", "WEEKLY_ACTIVE_MID", "REGION", "CHANNEL",
            "WEEKLY_AVG_TRX_PER_MID", "WEEK_START_DATE", "WEEK_END_DATE",
            "WOW_TRX_GROWTH", "WOW_VOL_GROWTH",
            "CUMULATIVE_YTD_TRX", "CUMULATIVE_YTD_VOL",
            "ACTIVE_TERMINAL_COUNT", "SEGMENT", "MERCHANT_TYPE",
            "STAGING_INSERTED_AT"]
    out = []
    cum_trx = 0.0
    cum_vol = 0.0
    prev_vol = None
    prev_trx = None
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
        out.append(insert("WEEKLY_MONITOR", cols, [
            m["name"], m["pm"], THIS_YEAR, w,
            trx, vol, fbi,
            f"WEEK_BATCH_{ws.replace('-', '')}", "EDW_ORACLE_PROD", FETCH_DATE,
            1, 10, "JAWA", "DIGITAL",
            round(trx / 10.0, 2) if trx else 0.0,
            ws, we, wow_trx, wow_vol,
            cum_trx, cum_vol, 12, "RETAIL", "ANCHOR",
            FETCH_DATE,
        ]))
    return "\n".join(out)


def build_inject_sql() -> str:
    parts = [_header()]
    for m in MERCHANTS:
        parts.append(f"-- ------------------------------------------------------------------")
        parts.append(f"-- {m['name']}  (PM={m['pm']})")
        parts.append(f"-- ------------------------------------------------------------------")
        parts.append(emit_card_share(m))
        parts.append(emit_card_history(m))
        parts.append(emit_card_monthly(m))
        parts.append(emit_monitoring(m))
        parts.append(emit_monitoring_weekly(m))
        parts.append(emit_target(m))
        parts.append(emit_weekly_monitor(m))
        parts.append("")
    parts.append("-- ---------- 3. METADATA BUMP ----------------------------------------------")
    parts.append(f"UPDATE APP_METADATA SET value='{META_DATE}' WHERE key='LAST_DATA_UPDATE';")
    parts.append("INSERT OR REPLACE INTO APP_METADATA(key,value) VALUES('NEW_DATA_SIGNAL','1');")
    parts.append("")
    names = _name_list_sql()
    parts.append("-- ---------- 4. VERIFICATION COUNTS ---------------------------------------")
    parts.append(f"SELECT 'PROCESSED_CARD_SHARE'        AS tbl, COUNT(*) AS n FROM PROCESSED_CARD_SHARE        WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'PROCESSED_CARD_HISTORY',      COUNT(*) FROM PROCESSED_CARD_HISTORY      WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'PROCESSED_CARD_MONTHLY',      COUNT(*) FROM PROCESSED_CARD_MONTHLY      WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'PROCESSED_MONITORING',        COUNT(*) FROM PROCESSED_MONITORING        WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'PROCESSED_MONITORING_WEEKLY', COUNT(*) FROM PROCESSED_MONITORING_WEEKLY WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'TARGET',                      COUNT(*) FROM TARGET                      WHERE MERCHANT_GROUP IN ({names})")
    parts.append(f"UNION ALL SELECT 'WEEKLY_MONITOR',              COUNT(*) FROM WEEKLY_MONITOR              WHERE MERCHANT_GROUP IN ({names});")
    parts.append("")
    parts.append("COMMIT;")
    return "\n".join(parts) + "\n"


def build_rollback_sql() -> str:
    names = _name_list_sql()
    in_clause = f"MERCHANT_GROUP IN ({names})"
    return dedent(f"""\
        -- ===========================================================================
        -- rollback_alert_test_data.sql
        -- Removes every synthetic merchant injected by inject_alert_test_data.sql
        -- and restores APP_METADATA.LAST_DATA_UPDATE.
        -- Targeted merchant names (real Indonesian brands, not present in the
        -- original dataset):
        --   {", ".join(m["name"] for m in MERCHANTS)}
        -- ===========================================================================
        BEGIN TRANSACTION;

        DELETE FROM PROCESSED_CARD_SHARE        WHERE {in_clause};
        DELETE FROM PROCESSED_CARD_HISTORY      WHERE {in_clause};
        DELETE FROM PROCESSED_CARD_MONTHLY      WHERE {in_clause};
        DELETE FROM PROCESSED_MONITORING        WHERE {in_clause};
        DELETE FROM PROCESSED_MONITORING_WEEKLY WHERE {in_clause};
        DELETE FROM TARGET                      WHERE {in_clause};
        DELETE FROM WEEKLY_MONITOR              WHERE {in_clause};

        UPDATE APP_METADATA SET value='{PREV_META_DATE}' WHERE key='LAST_DATA_UPDATE';
        DELETE FROM APP_METADATA WHERE key='NEW_DATA_SIGNAL';

        SELECT 'PROCESSED_CARD_SHARE'        AS tbl, COUNT(*) AS n FROM PROCESSED_CARD_SHARE        WHERE {in_clause}
        UNION ALL SELECT 'PROCESSED_CARD_HISTORY',      COUNT(*) FROM PROCESSED_CARD_HISTORY      WHERE {in_clause}
        UNION ALL SELECT 'PROCESSED_CARD_MONTHLY',      COUNT(*) FROM PROCESSED_CARD_MONTHLY      WHERE {in_clause}
        UNION ALL SELECT 'PROCESSED_MONITORING',        COUNT(*) FROM PROCESSED_MONITORING        WHERE {in_clause}
        UNION ALL SELECT 'PROCESSED_MONITORING_WEEKLY', COUNT(*) FROM PROCESSED_MONITORING_WEEKLY WHERE {in_clause}
        UNION ALL SELECT 'TARGET',                      COUNT(*) FROM TARGET                      WHERE {in_clause}
        UNION ALL SELECT 'WEEKLY_MONITOR',              COUNT(*) FROM WEEKLY_MONITOR              WHERE {in_clause};

        COMMIT;
    """)


def main():
    inject = build_inject_sql()
    rollback = build_rollback_sql()
    with open(INJECT_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(inject)
    with open(ROLLBACK_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(rollback)
    print(f"Wrote {INJECT_PATH}  ({len(inject):,} bytes)")
    print(f"Wrote {ROLLBACK_PATH}  ({len(rollback):,} bytes)")
    print(f"Merchants: {len(MERCHANTS)}")
    print(f"Latest week anchor: W{LATEST_WEEK} = {week_bounds(LATEST_WEEK)}")


if __name__ == "__main__":
    main()
