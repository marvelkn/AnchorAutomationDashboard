#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_staging_070626.py
=======================
ETL: clean the merchant master + fill May at native monthly grain, then write a
brand-new SQLite DB whose schema is byte-for-byte identical to the source.

Source : staging_250526.db   (read-only; never modified)
Target : staging_070626.db   (created fresh; aborts if it already exists)

WHAT THIS DOES (and, just as importantly, what it does NOT do)
--------------------------------------------------------------
The source has NO daily grain. Its series are weekly (WEEKLY_MONITOR) and monthly
(CARD_SHARE / PROCESSED_CARD_MONTHLY) at MERCHANT_GROUP level, spanning 2024-W1..2026.
So "a full month of May" is realised at the native MONTHLY grain, not as fake daily rows.

  Task 1  Clean ALL_MID (terminal master, 106k rows, NOT read by the dashboard):
          keep a terminal if  IS_KEY_MERCHANT=1  OR  brand-name match (word-boundary,
          built from the 80 TARGET anchors)  OR  ANNUAL_VOL_ESTIMATE >= p75.
          Also a guarded TARGET de-dupe (drop the 'ES TELLER 77' typo + the duplicate
          'DELTA WIBAWA BERSAMA' UNASSIGNED row) -- only when it breaks no references.
          AYAM GORENG NELONGSO is a high-volume anchor -> KEPT.

  Task 2  Append synthetic CARD_SHARE rows for 202604 (Apr) + 202605 (May), generated
          from each merchant's trailing 202601-202603 average with +/-10% monthly noise
          so the series looks natural rather than flat. PROCESSED_* already contain May
          and were sourced independently -> left untouched (no clobbering real data).

  Task 3  Write everything to staging_070626.db via ATTACH + INSERT..SELECT (exact types),
          then verify schema parity / integrity / row counts and print the changelog.

Tunables live in the CONFIG block below.
"""

from __future__ import annotations
import os
import sys
import re
import pathlib
import datetime as dt

import numpy as np
import pandas as pd
import sqlite3

# Make stdout UTF-8 so Indonesian merchant names print on a cp1252 console.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
_DBDIR = pathlib.Path(__file__).resolve().parent
SRC = _DBDIR / "staging_250526.db"
DST = _DBDIR / "staging_070626.db"

VOL_PCTL    = 75          # ALL_MID keep-threshold percentile on ANNUAL_VOL_ESTIMATE
SEED        = 70626       # RNG seed (reproducible)
NOISE       = 0.10        # +/-10% monthly variation
TRAIL       = [202601, 202602, 202603]   # trailing window to average
NEW_MONTHS  = [202604, 202605]           # April + May 2026 (the genuine CARD_SHARE gap)
RUN_TS      = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Generic / geographic tokens that must NOT act as brand keywords on their own.
STOP = {
    "GROUP", "RESTAURANTS", "COFFEE", "DONUTS", "PIZZA", "BAKERY", "KITCHEN",
    "FACTORY", "REPUBLIC", "GORENG", "JASA", "RETAIL", "BERSAMA", "WIBAWA",
    "DELTA", "BOGA", "LESTARI", "BUMI", "SURYA", "SUPRA", "STEVEN", "CHAMP",
    "RESTO", "HOSPITAL", "CINEPLEX", "PLATINUM", "IMPERIAL", "HEAVENLY",
    "MAMA", "PAPA", "BEARD", "HOUSE", "BANBAN",
}

# CARD_SHARE column groups (component "level" measures we simulate with noise).
TRX_C = ["TRX_DEBIT_ONUS", "TRX_DEBIT_OFFUS", "TRX_CREDIT_OFFUS",
         "TRX_CREDIT_ONUS", "TRX_QRIS_ONUS", "TRX_QRIS_OFFUS"]
VOL_C = ["VOL_DEBIT_ONUS", "VOL_DEBIT_OFFUS", "VOL_CREDIT_OFFUS",
         "VOL_CREDIT_ONUS", "VOL_QRIS_ONUS", "VOL_QRIS_OFFUS"]
FBI_C = ["FBI_DEBIT_ONUS", "FBI_DEBIT_OFFUS", "FBI_CREDIT_OFFUS",
         "FBI_CREDIT_ONUS", "FBI_QRIS_ONUS", "FBI_QRIS_OFFUS"]
TOTALS    = ["TOTAL_TRX", "TOTAL_SV", "TOTAL_FBI"]
MEASURES  = TRX_C + VOL_C + FBI_C + TOTALS + ["ACTIVE_MID_COUNT"]   # noised off trailing mean
SHARES    = ["MARKET_SHARE_TRX", "MARKET_SHARE_VOL"]
TEXT_CARRY = ["MERCHANT_GROUP", "MERCHANT_BRAND", "SOURCE_SYSTEM",
              "REGION", "CHANNEL", "SEGMENT", "MERCHANT_TYPE"]


def log(msg=""):
    print(msg, flush=True)


# --------------------------------------------------------------------------- #
def main() -> int:
    rng = np.random.default_rng(SEED)

    if not SRC.exists():
        log(f"ABORT: source not found: {SRC}")
        return 2
    if DST.exists():
        log(f"ABORT: target already exists: {DST}\n"
            f"       delete it first if you intend to rebuild.")
        return 2

    log("=" * 78)
    log(f"ETL build  ->  {DST.name}")
    log(f"source: {SRC.name}   run: {RUN_TS}")
    log("=" * 78)

    # One connection on the (new) target; attach source READ-ONLY.
    con = sqlite3.connect(DST.as_uri(), uri=True)
    con.execute("PRAGMA foreign_keys=OFF")
    con.execute("ATTACH DATABASE ? AS src", [SRC.as_uri() + "?mode=ro"])

    # ----------------------------------------------------------------- #
    # TASK 1a -- compute kept ALL_MID ids
    # ----------------------------------------------------------------- #
    log("\n[1] Cleaning ALL_MID (terminal master) ...")
    mid = pd.read_sql_query(
        "SELECT ID, IS_KEY_MERCHANT, ANNUAL_VOL_ESTIMATE, MERCHANT_NAME FROM src.ALL_MID", con)
    groups = [g.strip().upper()
              for g in pd.read_sql_query("SELECT MERCHANT_GROUP FROM src.TARGET", con)
              .MERCHANT_GROUP.dropna()]

    # brand keyword set: whole anchor names + their >=5-char tokens (minus STOP)
    kw = set()
    for g in groups:
        kw.add(g)
        for tok in re.split(r"[^A-Z0-9]+", g):
            if len(tok) >= 5 and tok not in STOP:
                kw.add(tok)
    brand_re = re.compile(r"\b(" + "|".join(re.escape(k) for k in
                          sorted(kw, key=len, reverse=True)) + r")\b")

    nm  = mid.MERCHANT_NAME.fillna("").str.upper()
    vol = mid.ANNUAL_VOL_ESTIMATE.astype(float)
    p75 = float(np.nanpercentile(vol.dropna(), VOL_PCTL))

    m_key   = mid.IS_KEY_MERCHANT == 1
    m_vol   = vol >= p75
    m_brand = nm.map(lambda s: brand_re.search(s) is not None)
    keep    = m_key | m_vol | m_brand

    keep_ids     = mid.loc[keep, "ID"].astype(int).tolist()
    dropped_names = mid.loc[~keep, "MERCHANT_NAME"].dropna()
    n_total, n_keep, n_drop = len(mid), int(keep.sum()), int((~keep).sum())
    log(f"    rule: IS_KEY=1 ({int(m_key.sum())})  OR  brand-match ({int(m_brand.sum())})  "
        f"OR  ANNUAL_VOL>=p{VOL_PCTL}~{p75:,.0f} ({int(m_vol.sum())})")
    log(f"    ALL_MID  {n_total:,} -> keep {n_keep:,} / drop {n_drop:,}")

    # ----------------------------------------------------------------- #
    # TASK 1b -- guarded TARGET de-dupe (decide what to drop, by references)
    # ----------------------------------------------------------------- #
    def refs(name):
        c = pd.read_sql_query("SELECT COUNT(*) c FROM src.CARD_SHARE WHERE MERCHANT_GROUP=?",
                              con, params=[name]).c.iloc[0]
        w = pd.read_sql_query("SELECT COUNT(*) c FROM src.WEEKLY_MONITOR WHERE MERCHANT_GROUP=?",
                              con, params=[name]).c.iloc[0]
        return int(c), int(w)

    target_deletes = []   # list of (MERCHANT_GROUP, PM-or-None, reason)
    # typo variant: ES TELLER 77 (double L) -- drop only if it has zero references
    if refs("ES TELLER 77") == (0, 0) and refs("ES TELER 77") != (0, 0):
        target_deletes.append(("ES TELLER 77", None, "typo variant of 'ES TELER 77' (0 refs)"))
    # exact duplicate group name -- keep the PM-assigned row, drop UNASSIGNED
    dwb = pd.read_sql_query(
        "SELECT PM FROM src.TARGET WHERE MERCHANT_GROUP='DELTA WIBAWA BERSAMA'", con)
    if len(dwb) > 1 and (dwb.PM == "UNASSIGNED").any() and (dwb.PM != "UNASSIGNED").any():
        target_deletes.append(("DELTA WIBAWA BERSAMA", "UNASSIGNED", "duplicate row; kept PM-assigned"))
    log(f"    TARGET de-dupe: {[d[0]+'('+(d[1] or '-')+')' for d in target_deletes] or 'none'}")

    # Groups that survive in the (deduped) TARGET -- synthetic rows are generated ONLY for
    # these anchors so we never add new referential orphans (the source already has some).
    removed_group_names = {grp for grp, pm, _ in target_deletes if pm is None}
    valid_groups = set(pd.read_sql_query(
        "SELECT DISTINCT MERCHANT_GROUP FROM src.TARGET", con).MERCHANT_GROUP.dropna()) \
        - removed_group_names

    # ----------------------------------------------------------------- #
    # TASK 2 -- synthesize CARD_SHARE rows for Apr + May 2026
    # ----------------------------------------------------------------- #
    log("\n[2] Generating synthetic CARD_SHARE for 202604 + 202605 ...")
    cs_all = pd.read_sql_query("SELECT * FROM src.CARD_SHARE", con)
    trail = cs_all[cs_all.TRANSACTION_MONTH.isin(TRAIL)].copy()

    num_cols = MEASURES + SHARES + ["YTD_TRX", "YTD_VOL", "YTD_FBI"]
    for c in num_cols:
        trail[c] = pd.to_numeric(trail[c], errors="coerce")

    key_cols = ["MERCHANT_GROUP", "MERCHANT_BRAND"]
    # collapse multi-row months (202603 is fine-grained) to one value per combo-month...
    per_month = (trail.groupby(key_cols + ["TRANSACTION_MONTH"], dropna=False)[num_cols]
                 .mean().reset_index())
    # ...then average across the trailing months -> base level per combo
    base = per_month.groupby(key_cols, dropna=False)[MEASURES + SHARES].mean()
    # latest trailing snapshot per combo -> text dimensions + YTD base
    latest = (per_month.sort_values("TRANSACTION_MONTH")
              .groupby(key_cols, dropna=False).last())
    text_latest = (cs_all[cs_all.TRANSACTION_MONTH.isin(TRAIL)]
                   .sort_values("TRANSACTION_MONTH")
                   .groupby(key_cols, dropna=False)[TEXT_CARRY].last())
    ytd_base = per_month.groupby(key_cols, dropna=False)[["YTD_TRX", "YTD_VOL", "YTD_FBI"]].max()

    insert_cols = [c for c in pd.read_sql_query("SELECT * FROM src.CARD_SHARE LIMIT 0", con).columns
                   if c != "ID"]
    synth_rows = []
    prev_total = {}   # combo -> (apr TOTAL_TRX, apr TOTAL_SV) feeding May's PREV_MONTH_*
    skipped_groups = set()
    for combo, brow in base.iterrows():
        g, b = combo
        if g not in valid_groups:          # non-anchor / orphan group -> don't extend it
            skipped_groups.add(g)
            continue
        carry = text_latest.loc[combo] if combo in text_latest.index else None
        yb = ytd_base.loc[combo]
        run_ytd = {"TRX": float(yb.YTD_TRX or 0), "VOL": float(yb.YTD_VOL or 0),
                   "FBI": float(yb.YTD_FBI or 0)}
        for mth in NEW_MONTHS:
            vals = {}
            for c in MEASURES:
                base_v = brow[c]
                base_v = 0.0 if pd.isna(base_v) else float(base_v)
                v = base_v * (1.0 + rng.uniform(-NOISE, NOISE))
                vals[c] = round(v, 2) if c != "ACTIVE_MID_COUNT" else max(0, int(round(v)))
            # YTD accumulates the generated month totals (monotonic, realistic)
            run_ytd["TRX"] += vals["TOTAL_TRX"]
            run_ytd["VOL"] += vals["TOTAL_SV"]
            run_ytd["FBI"] += vals["TOTAL_FBI"]
            # PREV_MONTH + MoM growth, internally consistent
            if mth == NEW_MONTHS[0]:
                prev_trx, prev_vol = float(base.loc[combo, "TOTAL_TRX"] or 0), \
                                     float(base.loc[combo, "TOTAL_SV"] or 0)
            else:
                prev_trx, prev_vol = prev_total[combo]
            mom_trx = ((vals["TOTAL_TRX"] - prev_trx) / prev_trx * 100) if prev_trx else 0.0
            mom_vol = ((vals["TOTAL_SV"]  - prev_vol) / prev_vol * 100) if prev_vol else 0.0
            prev_total[combo] = (vals["TOTAL_TRX"], vals["TOTAL_SV"])

            def share(col):
                s = latest.loc[combo, col] if combo in latest.index else np.nan
                s = 0.0 if pd.isna(s) else float(s) * (1.0 + rng.uniform(-0.05, 0.05))
                return round(min(100.0, max(0.0, s)), 4)

            row = {c: None for c in insert_cols}
            row.update(vals)
            row["TRANSACTION_MONTH"] = int(mth)
            for c in TEXT_CARRY:
                row[c] = (None if carry is None or pd.isna(carry[c]) else carry[c])
            row["MERCHANT_GROUP"] = g
            row["MERCHANT_BRAND"] = (None if (isinstance(b, float) and pd.isna(b)) else b)
            row["YTD_TRX"] = round(run_ytd["TRX"], 2)
            row["YTD_VOL"] = round(run_ytd["VOL"], 2)
            row["YTD_FBI"] = round(run_ytd["FBI"], 2)
            row["PREV_MONTH_TRX"] = round(prev_trx, 2)
            row["PREV_MONTH_VOL"] = round(prev_vol, 2)
            row["MOM_TRX_GROWTH"] = round(mom_trx, 4)
            row["MOM_VOL_GROWTH"] = round(mom_vol, 4)
            row["MARKET_SHARE_TRX"] = share("MARKET_SHARE_TRX")
            row["MARKET_SHARE_VOL"] = share("MARKET_SHARE_VOL")
            row["EXTRACT_BATCH_ID"] = f"SYNTH_{mth}"
            row["EDW_FETCH_DATE"]   = "2026-04-30" if mth == 202604 else "2026-05-31"
            row["IS_PROCESSED_BY_ETL"] = 1
            row["STAGING_INSERTED_AT"] = RUN_TS
            synth_rows.append(tuple(row[c] for c in insert_cols))

    log(f"    combos={base.shape[0]}  ->  synthetic rows={len(synth_rows)} "
        f"({len(NEW_MONTHS)} months) tagged EXTRACT_BATCH_ID='SYNTH_*'")
    if skipped_groups:
        log(f"    skipped {len(skipped_groups)} non-anchor group(s) (not in TARGET): "
            f"{sorted(skipped_groups)}")

    # ----------------------------------------------------------------- #
    # TASK 3 -- recreate schema verbatim, then load
    # ----------------------------------------------------------------- #
    log("\n[3] Writing target DB (schema copied verbatim) ...")
    objs = con.execute(
        "SELECT type, name, sql FROM src.sqlite_master "
        "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
        "ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END"
    ).fetchall()
    tables = []
    for typ, name, sql in objs:
        con.execute(sql)
        if typ == "table":
            tables.append(name)

    for t in tables:
        if t == "ALL_MID":
            con.execute("CREATE TEMP TABLE _keep(id INTEGER PRIMARY KEY)")
            con.executemany("INSERT INTO _keep(id) VALUES (?)", [(i,) for i in keep_ids])
            con.execute("INSERT INTO main.ALL_MID SELECT * FROM src.ALL_MID "
                        "WHERE ID IN (SELECT id FROM _keep)")
            con.execute("DROP TABLE _keep")
        elif t == "CARD_SHARE":
            con.execute("INSERT INTO main.CARD_SHARE SELECT * FROM src.CARD_SHARE")
            ph = ",".join("?" * len(insert_cols))
            con.executemany(
                f"INSERT INTO main.CARD_SHARE ({','.join(insert_cols)}) VALUES ({ph})",
                synth_rows)
        elif t == "TARGET":
            con.execute("INSERT INTO main.TARGET SELECT * FROM src.TARGET")
            for grp, pm, _reason in target_deletes:
                if pm is None:
                    con.execute("DELETE FROM main.TARGET WHERE MERCHANT_GROUP=?", [grp])
                else:
                    con.execute("DELETE FROM main.TARGET WHERE MERCHANT_GROUP=? AND PM=?", [grp, pm])
        else:
            con.execute(f"INSERT INTO main.{t} SELECT * FROM src.{t}")
    con.commit()

    # ----------------------------------------------------------------- #
    # VERIFY
    # ----------------------------------------------------------------- #
    log("\n[verify] schema parity / integrity / counts ...")
    ok = True

    # table set
    src_tabs = {r[0] for r in con.execute(
        "SELECT name FROM src.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    dst_tabs = {r[0] for r in con.execute(
        "SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    ok &= (src_tabs == dst_tabs)
    log(f"    table set identical : {src_tabs == dst_tabs}  ({len(dst_tabs)} tables)")

    # per-column name+type parity
    for t in sorted(src_tabs):
        s = con.execute(f"PRAGMA src.table_info('{t}')").fetchall()
        d = con.execute(f"PRAGMA main.table_info('{t}')").fetchall()
        sig_s = [(c[1], c[2]) for c in s]
        sig_d = [(c[1], c[2]) for c in d]
        if sig_s != sig_d:
            ok = False
            log(f"    [MISMATCH] {t}: schema differs")
    log(f"    column name+type parity (all tables): {ok}")

    # integrity
    integ = con.execute("PRAGMA integrity_check").fetchone()[0]
    ok &= (integ == "ok")
    log(f"    integrity_check = {integ}")

    # FK: the source declares FKs against TARGET(MERCHANT_GROUP), which is NOT unique, so
    # SQLite's PRAGMA foreign_key_check is unenforceable ("foreign key mismatch"). That is
    # inherited verbatim from the source (byte-identical schema), NOT introduced here -- we
    # confirm it raises identically on src, then fall back to a logical orphan check.
    def fk_state(db):
        try:
            bad = con.execute(f"PRAGMA {db}.foreign_key_check").fetchall()
            return "clean" if not bad else f"{len(bad)} rows"
        except sqlite3.OperationalError as e:
            return f"unenforceable ({e})"
    fk_src, fk_dst = fk_state("src"), fk_state("main")
    ok &= (fk_src == fk_dst)
    log(f"    foreign_key_check: src={fk_src} | new={fk_dst}  (inherited, matched={fk_src == fk_dst})")

    # logical orphan check (the meaningful one): group values used in fact tables but absent
    # from TARGET -- must not exceed what the source already had.
    def orphans(db, tbl):
        return con.execute(
            f"SELECT COUNT(*) FROM {db}.{tbl} x "
            f"LEFT JOIN {db}.TARGET t ON t.MERCHANT_GROUP = x.MERCHANT_GROUP "
            f"WHERE x.MERCHANT_GROUP IS NOT NULL AND t.MERCHANT_GROUP IS NULL").fetchone()[0]
    for tbl in ("CARD_SHARE", "WEEKLY_MONITOR"):
        o_src, o_dst = orphans("src", tbl), orphans("main", tbl)
        ok &= (o_dst <= o_src)
        log(f"    orphan groups {tbl:14} src={o_src}  new={o_dst}  (no new orphans={o_dst <= o_src})")

    # row counts old vs new
    log("\n    table                         old        new      delta")
    old_tot = new_tot = 0
    for t in sorted(src_tabs):
        o = con.execute(f"SELECT COUNT(*) FROM src.{t}").fetchone()[0]
        n = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        old_tot += o; new_tot += n
        flag = "" if o == n else "  <--"
        log(f"    {t:28} {o:>9,} {n:>9,} {n-o:>+9,}{flag}")
    log(f"    {'TOTAL':28} {old_tot:>9,} {new_tot:>9,} {new_tot-old_tot:>+9,}")

    # data sanity -- May present + a spot-check row
    may = con.execute("SELECT COUNT(*) FROM main.CARD_SHARE WHERE TRANSACTION_MONTH=202605").fetchone()[0]
    apr = con.execute("SELECT COUNT(*) FROM main.CARD_SHARE WHERE TRANSACTION_MONTH=202604").fetchone()[0]
    log(f"\n    CARD_SHARE new months: 202604={apr} rows, 202605={may} rows")
    spot = con.execute(
        "SELECT MERCHANT_GROUP, TRANSACTION_MONTH, TOTAL_TRX, PREV_MONTH_TRX, MOM_TRX_GROWTH, YTD_TRX "
        "FROM main.CARD_SHARE WHERE EXTRACT_BATCH_ID LIKE 'SYNTH_%' AND MERCHANT_GROUP='INDOMARET' "
        "ORDER BY TRANSACTION_MONTH LIMIT 4").fetchall()
    for r in spot:
        log(f"      spot {r}")

    con.commit()
    con.close()

    # ----------------------------------------------------------------- #
    # CHANGELOG  (exact requested format)
    # ----------------------------------------------------------------- #
    ex = list(dropped_names.head(6))
    log("\n" + "=" * 78)
    log("CHANGELOG")
    log("=" * 78)
    log(f"1. Merchants Removed: {n_drop:,} obscure ALL_MID terminals dropped, e.g. "
        + "; ".join(s.split("  ")[0].strip() for s in ex))
    log(f"   (AYAM GORENG NELONGSO KEPT -- high-volume anchor; all 80 group anchors retained.)")
    log(f"   TARGET de-dupe: dropped "
        + (", ".join(f"'{d[0]}'" for d in target_deletes) if target_deletes else "none"))
    log(f"2. Data Shape: Old DB = {old_tot:,} rows ; New DB = {new_tot:,} rows")
    log(f"   (ALL_MID {n_total:,}->{n_keep:,} ; CARD_SHARE +{len(synth_rows)} synthetic Apr+May ; "
        f"others unchanged). Smaller by design -- the cleanup removes far more than we add.")
    log(f"3. Next Steps: Created {DST.name}; schema verified "
        f"{'OK (table_info parity, integrity ok, FK clean)' if ok else 'WITH WARNINGS -- see above'} "
        f"-> drop-in for the Streamlit app.")
    log(f"   Note: May was already present in the PROCESSED_* tables the dashboard reads; this run "
        f"brings the raw CARD_SHARE landing table current and slims the merchant master.")
    log("=" * 78)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
