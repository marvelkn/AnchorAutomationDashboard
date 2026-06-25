#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""build_staging_160626.py — mid-month (as-of 2026-06-16) demo staging DB.
Extend the last good snapshot (staging_250526.db) with June-to-date activity at
the schema's native weekly+monthly grain. Copy everything verbatim, then append
June only to the tables the dashboard reads. FBI is always a fixed positive
multiple of VOL per merchant (no inverse anomaly). June MTD ~50-55% by day-overlap.
"""
from __future__ import annotations
import sys, pathlib, datetime as dt
import numpy as np, pandas as pd, sqlite3
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

_DBDIR = pathlib.Path(__file__).resolve().parent
SRC = _DBDIR / "staging_250526.db"
DST = _DBDIR / "staging_160626.db"
SNAPSHOT_DATE = dt.date(2026, 6, 16)
RUN_TS = dt.datetime(2026, 6, 16, 16, 30, 0).strftime("%Y-%m-%d %H:%M:%S")
SEED = 160626; YEAR = 2026
BASELINE_MONTH = 202605; JUNE_MONTH = 202606
NEW_WEEKS = [22, 23, 24]
DAILY_NOISE = 0.04; WEEK_NOISE = 0.05
WEEKDAY_HEAVY = {"TRANSPORTATION","FUEL & GAS","HEALTHCARE","FINTECH","TRAVEL AGENCY"}
WEEKEND_HEAVY = {"FOOD & BEVERAGE","ENTERTAINMENT","TOURISM & ENTERTAINMENT","HOSPITALITY","RETAIL"}
TRX_C=["TRX_DEBIT_ONUS","TRX_DEBIT_OFFUS","TRX_CREDIT_OFFUS","TRX_CREDIT_ONUS","TRX_QRIS_ONUS","TRX_QRIS_OFFUS"]
VOL_C=["VOL_DEBIT_ONUS","VOL_DEBIT_OFFUS","VOL_CREDIT_OFFUS","VOL_CREDIT_ONUS","VOL_QRIS_ONUS","VOL_QRIS_OFFUS"]
FBI_C=["FBI_DEBIT_ONUS","FBI_DEBIT_OFFUS","FBI_CREDIT_OFFUS","FBI_CREDIT_ONUS","FBI_QRIS_ONUS","FBI_QRIS_OFFUS"]

def log(m=""): print(m, flush=True)
def week_start(wn, year=YEAR): return dt.date(year,1,1)+dt.timedelta(days=(wn-1)*7)
def weekday_profile(segment):
    seg=(segment or "").upper()
    if seg in WEEKDAY_HEAVY: p=np.array([1.12,1.12,1.12,1.12,1.10,0.78,0.64])
    elif seg in WEEKEND_HEAVY: p=np.array([0.88,0.90,0.92,0.96,1.10,1.40,1.34])
    else: p=np.array([1.05,1.05,1.05,1.03,1.08,0.90,0.84])
    return p/p.mean()

def main():
    rng=np.random.default_rng(SEED)
    if not SRC.exists(): log(f"ABORT: source not found: {SRC}"); return 2
    if DST.exists(): log(f"ABORT: target exists: {DST}"); return 2
    log("="*78); log(f"ETL build -> {DST.name}  (as-of {SNAPSHOT_DATE})"); log(f"source: {SRC.name}  run: {RUN_TS}"); log("="*78)
    con=sqlite3.connect(DST.as_uri(), uri=True)
    con.execute("PRAGMA foreign_keys=OFF")
    con.execute("ATTACH DATABASE ? AS src", [SRC.as_uri()+"?mode=ro"])

    log("\n[1] Cloning schema + all tables verbatim ...")
    objs=con.execute("SELECT type,name,sql FROM src.sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END").fetchall()
    tables=[]
    for typ,name,sql in objs:
        con.execute(sql)
        if typ=="table": tables.append(name)
    for t in tables: con.execute(f"INSERT INTO main.{t} SELECT * FROM src.{t}")
    con.commit(); log(f"    copied {len(tables)} tables (history preserved)")

    log("\n[2] Building per-merchant June baselines ...")
    wk=pd.read_sql_query("SELECT * FROM main.WEEKLY_MONITOR WHERE YEAR=?", con, params=[YEAR])
    active=sorted(wk.MERCHANT_GROUP.dropna().unique().tolist())
    w21=wk[wk.WEEK_NUM==21].sort_values("ID").groupby("MERCHANT_GROUP").last()
    trail=wk[wk.WEEK_NUM.between(18,21)].groupby("MERCHANT_GROUP").agg(
        avgvol=("WEEKLY_VOL","mean"),avgtrx=("WEEKLY_TRX","mean"),avgfbi=("WEEKLY_FBI","mean"),
        avgmid=("WEEKLY_ACTIVE_MID","mean"),avgterm=("ACTIVE_TERMINAL_COUNT","mean"))
    pcm_may=(pd.read_sql_query("SELECT * FROM main.PROCESSED_CARD_MONTHLY WHERE YEAR=? AND TRX_MONTH=?",con,params=[YEAR,BASELINE_MONTH])
             .sort_values("TOTAL_SV").drop_duplicates("MERCHANT_GROUP",keep="last").set_index("MERCHANT_GROUP"))
    pmw=pd.read_sql_query("SELECT rowid AS rid, MERCHANT_GROUP, DIMENSI, YTD, FY FROM main.PROCESSED_MONITORING_WEEKLY WHERE W22 IS NULL", con)
    cur=con.cursor()
    JUNE_DAYS=[dt.date(2026,6,d) for d in range(1,17)]
    SPAN_DAYS=[week_start(22)+dt.timedelta(days=i) for i in range(20)]
    span_index={d:i for i,d in enumerate(SPAN_DAYS)}
    june_metrics={}; skipped=[]
    for m in active:
        if m in pcm_may.index:
            may_sv=float(pcm_may.at[m,"TOTAL_SV"] or 0); may_trx=float(pcm_may.at[m,"TOTAL_TRX"] or 0); may_fbi=float(pcm_may.at[m,"TOTAL_FBI"] or 0)
        elif m in trail.index:
            may_sv=float(trail.at[m,"avgvol"] or 0)*31.0/7.0; may_trx=float(trail.at[m,"avgtrx"] or 0)*31.0/7.0; may_fbi=float(trail.at[m,"avgfbi"] or 0)*31.0/7.0
        else: skipped.append(m); continue
        if may_sv<=0: skipped.append(m); continue
        seg=(w21.at[m,"SEGMENT"] if m in w21.index else None) or "RETAIL"
        fbi_rate=float(np.clip(may_fbi/may_sv if may_sv else 0.007,0.0005,0.025))
        ticket=float(may_trx and may_sv/may_trx or 50000.0); ticket=float(np.clip(ticket,5000.0,5_000_000.0))
        weekly_base=float(trail.at[m,"avgvol"]) if (m in trail.index and pd.notna(trail.at[m,"avgvol"]) and float(trail.at[m,"avgvol"])>0) else may_sv/4.345
        avgtrx_wk=float(trail.at[m,"avgtrx"]) if (m in trail.index and pd.notna(trail.at[m,"avgtrx"]) and float(trail.at[m,"avgtrx"])>0) else 0.0
        ticket_wk=(weekly_base/avgtrx_wk) if avgtrx_wk>0 else ticket
        daily_base=weekly_base/7.0; prof=weekday_profile(seg); week_lvl=rng.uniform(1-WEEK_NOISE,1+WEEK_NOISE)
        sv_daily=np.zeros(len(SPAN_DAYS))
        for i,d in enumerate(SPAN_DAYS):
            f=prof[d.weekday()]*rng.uniform(1-DAILY_NOISE,1+DAILY_NOISE); sv_daily[i]=daily_base*week_lvl*f
        def week_sv(wn):
            s=week_start(wn)
            return float(sum(sv_daily[span_index[s+dt.timedelta(days=k)]] for k in range(7)
                             if (s+dt.timedelta(days=k)) in span_index and (s+dt.timedelta(days=k))<=SNAPSHOT_DATE))
        w_sv={wn:week_sv(wn) for wn in NEW_WEEKS}
        w_trx={wn:max(1,int(round(w_sv[wn]/ticket_wk))) for wn in NEW_WEEKS}
        w_fbi={wn:round(w_sv[wn]*fbi_rate,2) for wn in NEW_WEEKS}
        june_frac=float(rng.uniform(0.515,0.545))
        j_sv=round(may_sv*june_frac,2); j_trx=max(1,int(round(may_trx*june_frac))); j_fbi=round(may_fbi*june_frac,2)
        june_metrics[m]=dict(seg=seg,fbi_rate=fbi_rate,ticket=ticket,w_sv=w_sv,w_trx=w_trx,w_fbi=w_fbi,
            j_sv=round(j_sv,2),j_trx=j_trx,j_fbi=j_fbi,may_sv=may_sv,
            mid=int(round(float(w21.at[m,"WEEKLY_ACTIVE_MID"]) if m in w21.index and pd.notna(w21.at[m,"WEEKLY_ACTIVE_MID"]) else (trail.at[m,"avgmid"] if m in trail.index else 10))),
            term=int(round(float(w21.at[m,"ACTIVE_TERMINAL_COUNT"]) if m in w21.index and pd.notna(w21.at[m,"ACTIVE_TERMINAL_COUNT"]) else (trail.at[m,"avgterm"] if m in trail.index else 12))),
            pm=(w21.at[m,"PM_NAME"] if m in w21.index else None) or "UNASSIGNED",
            region=(w21.at[m,"REGION"] if m in w21.index else None) or "JABODETABEK",
            channel=(w21.at[m,"CHANNEL"] if m in w21.index else None) or "DIRECT",
            mtype=(w21.at[m,"MERCHANT_TYPE"] if m in w21.index else None) or "ANCHOR",
            cum_vol=float(w21.at[m,"CUMULATIVE_YTD_VOL"]) if m in w21.index and pd.notna(w21.at[m,"CUMULATIVE_YTD_VOL"]) else 0.0,
            cum_trx=float(w21.at[m,"CUMULATIVE_YTD_TRX"]) if m in w21.index and pd.notna(w21.at[m,"CUMULATIVE_YTD_TRX"]) else 0.0,
            prev_vol=float(w21.at[m,"WEEKLY_VOL"]) if m in w21.index and pd.notna(w21.at[m,"WEEKLY_VOL"]) else 0.0,
            prev_trx=float(w21.at[m,"WEEKLY_TRX"]) if m in w21.index and pd.notna(w21.at[m,"WEEKLY_TRX"]) else 0.0)
    log(f"    {len(june_metrics)} merchants modelled ({len(skipped)} skipped: {skipped or 'none'})")

    log("\n[3] Appending WEEKLY_MONITOR W22-W24 ...")
    wm_cols=[c[1] for c in con.execute("PRAGMA table_info(WEEKLY_MONITOR)") if c[1]!="ID"]
    n_week=0
    for m,d in june_metrics.items():
        cum_vol,cum_trx=d["cum_vol"],d["cum_trx"]; prev_vol,prev_trx=d["prev_vol"],d["prev_trx"]
        for wn in NEW_WEEKS:
            ws=week_start(wn); we=ws+dt.timedelta(days=6); we_eff=min(we,SNAPSHOT_DATE)
            vol,trx,fbi=d["w_sv"][wn],d["w_trx"][wn],d["w_fbi"][wn]
            cum_vol+=vol; cum_trx+=trx
            wow_vol=round((vol-prev_vol)/prev_vol,4) if prev_vol>0 else None
            wow_trx=round((trx-prev_trx)/prev_trx,4) if prev_trx>0 else None
            row={"MERCHANT_GROUP":m,"PM_NAME":d["pm"],"YEAR":YEAR,"WEEK_NUM":wn,
                "WEEKLY_TRX":float(trx),"WEEKLY_VOL":round(vol,2),"WEEKLY_FBI":fbi,
                "EXTRACT_BATCH_ID":f"WEEK_BATCH_{ws.strftime('%Y%m%d')}","SOURCE_SYSTEM":"EDW_ORACLE_PROD",
                "EDW_FETCH_DATE":RUN_TS,"IS_PROCESSED_BY_ETL":1,"WEEKLY_ACTIVE_MID":d["mid"],
                "REGION":d["region"],"CHANNEL":d["channel"],
                "WEEKLY_AVG_TRX_PER_MID":round(trx/d["mid"],4) if d["mid"] else None,
                "WEEK_START_DATE":ws.isoformat(),"WEEK_END_DATE":we_eff.isoformat(),
                "WOW_TRX_GROWTH":wow_trx,"WOW_VOL_GROWTH":wow_vol,
                "CUMULATIVE_YTD_TRX":round(cum_trx,2),"CUMULATIVE_YTD_VOL":round(cum_vol,2),
                "ACTIVE_TERMINAL_COUNT":d["term"],"SEGMENT":d["seg"],"MERCHANT_TYPE":d["mtype"],"STAGING_INSERTED_AT":RUN_TS}
            ph=",".join("?"*len(wm_cols))
            cur.execute(f"INSERT INTO WEEKLY_MONITOR ({','.join(wm_cols)}) VALUES ({ph})",[row.get(c) for c in wm_cols])
            n_week+=1; prev_vol,prev_trx=vol,trx
    log(f"    inserted {n_week} weekly rows")

    log("\n[4] Patching PROCESSED_MONITORING_WEEKLY ...")
    n_pmw=0
    for r in pmw.itertuples():
        m=r.MERCHANT_GROUP
        if m not in june_metrics: continue
        d=june_metrics[m]
        if r.DIMENSI=="VOL": vals={wn:round(d["w_sv"][wn],2) for wn in NEW_WEEKS}
        elif r.DIMENSI=="TRX": vals={wn:int(d["w_trx"][wn]) for wn in NEW_WEEKS}
        elif r.DIMENSI=="FBI": vals={wn:d["w_fbi"][wn] for wn in NEW_WEEKS}
        else: continue
        new_ytd=float(r.YTD or 0)+sum(vals.values())
        cur.execute("UPDATE PROCESSED_MONITORING_WEEKLY SET W22=?,W23=?,W24=?,YTD=?,FY=? WHERE rowid=?",
                    [vals[22],vals[23],vals[24],round(new_ytd,2),round(new_ytd,2),r.rid]); n_pmw+=1
    log(f"    patched {n_pmw} rows")

    log("\n[5] Appending PROCESSED_CARD_MONTHLY + PROCESSED_CARD_HISTORY (202606) ...")
    pcm_cols=[c[1] for c in con.execute("PRAGMA table_info(PROCESSED_CARD_MONTHLY)")]
    sv_parts=["SV_DEBIT_ONUS","SV_DEBIT_OFFUS","SV_CREDIT_OFFUS","SV_QRIS_ONUS","SV_QRIS_OFFUS"]
    trx_parts=["TRX_DEBIT_ONUS","TRX_DEBIT_OFFUS","TRX_CREDIT_OFFUS","TRX_QRIS_ONUS","TRX_QRIS_OFFUS"]
    fbi_parts=["FBI_DEBIT_ONUS","FBI_DEBIT_OFFUS","FBI_CREDIT_OFFUS","FBI_QRIS_ONUS","FBI_QRIS_OFFUS"]
    n_pcm=n_pch=0
    for m,d in june_metrics.items():
        if m not in pcm_may.index: continue
        may=pcm_may.loc[m]; anchor=may.get("MERCHANT_ANCHOR",m) or m; msv=float(may["TOTAL_SV"] or 0)
        def split(parts,tn,to):
            props=[float(may.get(p,0) or 0)/to for p in parts] if to else [1.0/len(parts)]*len(parts)
            return [round(tn*pr,2) for pr in props]
        sv_vals=split(sv_parts,d["j_sv"],msv); trx_vals=split(trx_parts,d["j_trx"],float(may["TOTAL_TRX"] or 0)); fbi_vals=split(fbi_parts,d["j_fbi"],float(may["TOTAL_FBI"] or 0))
        rm={c:0 for c in pcm_cols}
        rm.update({"MERCHANT_GROUP":m,"MERCHANT_ANCHOR":anchor,"TRX_MONTH":JUNE_MONTH,"YEAR":YEAR,
                   "TOTAL_TRX":float(d["j_trx"]),"TOTAL_SV":d["j_sv"],"TOTAL_FBI":d["j_fbi"]})
        for c,v in zip(sv_parts,sv_vals): rm[c]=v
        for c,v in zip(trx_parts,trx_vals): rm[c]=v
        for c,v in zip(fbi_parts,fbi_vals): rm[c]=v
        ph=",".join("?"*len(pcm_cols))
        cur.execute(f"INSERT INTO PROCESSED_CARD_MONTHLY ({','.join(pcm_cols)}) VALUES ({ph})",[rm[c] for c in pcm_cols]); n_pcm+=1
        cur.execute("INSERT INTO PROCESSED_CARD_HISTORY (MERCHANT_GROUP,MERCHANT_ANCHOR,TRX_MONTH,YEAR,TOTAL_SV,TOTAL_TRX,TOTAL_FBI) VALUES (?,?,?,?,?,?,?)",
                    [m,anchor,JUNE_MONTH,YEAR,d["j_sv"],float(d["j_trx"]),d["j_fbi"]]); n_pch+=1
    log(f"    inserted {n_pcm} monthly + {n_pch} history rows")

    log("\n[6] Bumping PROCESSED_CARD_SHARE + PROCESSED_MONITORING ...")
    n_pcs=0
    pcs=pd.read_sql_query("SELECT rowid AS rid, MERCHANT_GROUP, TOTAL_SV, TOTAL_TRX, TOTAL_FBI, RASIO_ONUS, N_BULAN FROM main.PROCESSED_CARD_SHARE", con)
    for r in pcs.itertuples():
        m=r.MERCHANT_GROUP
        if m not in june_metrics: continue
        d=june_metrics[m]; ratio=float(r.RASIO_ONUS or 0.10)
        new_sv=float(r.TOTAL_SV or 0)+d["j_sv"]; new_trx=float(r.TOTAL_TRX or 0)+d["j_trx"]; new_fbi=float(r.TOTAL_FBI or 0)+d["j_fbi"]
        cur.execute("UPDATE PROCESSED_CARD_SHARE SET TOTAL_SV=?,TOTAL_TRX=?,TOTAL_FBI=?,SV_ONUS=?,N_BULAN=?,BULAN_TERAKHIR=? WHERE rowid=?",
                    [round(new_sv,2),round(new_trx,2),round(new_fbi,2),round(new_sv*ratio,2),int((r.N_BULAN or 0)+1),JUNE_MONTH,r.rid]); n_pcs+=1
    n_pmon=0
    pmon=pd.read_sql_query("SELECT rowid AS rid, MERCHANT_GROUP, YTD FROM main.PROCESSED_MONITORING", con)
    for r in pmon.itertuples():
        m=r.MERCHANT_GROUP
        if m not in june_metrics: continue
        cur.execute("UPDATE PROCESSED_MONITORING SET YTD=? WHERE rowid=?",[round(float(r.YTD or 0)+june_metrics[m]["j_sv"],2),r.rid]); n_pmon+=1
    log(f"    updated {n_pcs} card_share + {n_pmon} monitoring rows")

    log("\n[7] Appending raw CARD_SHARE 202606 (lineage) ...")
    cs_cols=[c for c in pd.read_sql_query("SELECT * FROM main.CARD_SHARE LIMIT 0", con).columns if c!="ID"]
    cs_all=pd.read_sql_query("SELECT * FROM main.CARD_SHARE", con); cs_all=cs_all[cs_all.TRANSACTION_MONTH<JUNE_MONTH]
    latest_m=cs_all.sort_values("TRANSACTION_MONTH").groupby(["MERCHANT_GROUP","MERCHANT_BRAND"],dropna=False).last()
    n_cs=0
    for (g,b),base in latest_m.iterrows():
        if g not in june_metrics: continue
        d=june_metrics[g]; scale=(d["j_sv"]/float(base["TOTAL_SV"])) if base.get("TOTAL_SV") else 0.53
        row={c:None for c in cs_cols}
        for c in ["MERCHANT_GROUP","MERCHANT_BRAND","SOURCE_SYSTEM","REGION","CHANNEL","SEGMENT","MERCHANT_TYPE"]: row[c]=base.get(c)
        row["MERCHANT_GROUP"]=g; row["TRANSACTION_MONTH"]=JUNE_MONTH
        for c in TRX_C+VOL_C+FBI_C:
            v=base.get(c); row[c]=round(float(v)*scale,2) if pd.notna(v) else None
        row["TOTAL_TRX"]=float(d["j_trx"]); row["TOTAL_SV"]=d["j_sv"]; row["TOTAL_FBI"]=d["j_fbi"]; row["ACTIVE_MID_COUNT"]=d["mid"]
        row["PREV_MONTH_TRX"]=round(float(base.get("TOTAL_TRX") or 0),2); row["PREV_MONTH_VOL"]=round(float(base.get("TOTAL_SV") or 0),2)
        pmt,pmv=row["PREV_MONTH_TRX"],row["PREV_MONTH_VOL"]
        row["MOM_TRX_GROWTH"]=round((row["TOTAL_TRX"]-pmt)/pmt*100,4) if pmt else 0.0
        row["MOM_VOL_GROWTH"]=round((row["TOTAL_SV"]-pmv)/pmv*100,4) if pmv else 0.0
        row["YTD_TRX"]=round(float(base.get("YTD_TRX") or 0)+row["TOTAL_TRX"],2)
        row["YTD_VOL"]=round(float(base.get("YTD_VOL") or 0)+row["TOTAL_SV"],2)
        row["YTD_FBI"]=round(float(base.get("YTD_FBI") or 0)+row["TOTAL_FBI"],2)
        for c in ["MARKET_SHARE_TRX","MARKET_SHARE_VOL"]: row[c]=float(base.get(c)) if pd.notna(base.get(c)) else None
        row["EXTRACT_BATCH_ID"]="MONTH_BATCH_20260616_MTD"; row["EDW_FETCH_DATE"]=SNAPSHOT_DATE.isoformat()
        row["IS_PROCESSED_BY_ETL"]=1; row["STAGING_INSERTED_AT"]=RUN_TS
        ph=",".join("?"*len(cs_cols))
        cur.execute(f"INSERT INTO CARD_SHARE ({','.join(cs_cols)}) VALUES ({ph})",[row.get(c) for c in cs_cols]); n_cs+=1
    log(f"    inserted {n_cs} raw CARD_SHARE rows")

    log("\n[8] APP_METADATA freshness ...")
    cur.execute("UPDATE APP_METADATA SET value=? WHERE key='LAST_DATA_UPDATE'",[RUN_TS])
    cur.execute("INSERT OR REPLACE INTO APP_METADATA(key,value) VALUES('NEW_DATA_SIGNAL','1')")
    con.commit()

    log("\n[verify] ...")
    ok=True
    src_tabs={r[0] for r in con.execute("SELECT name FROM src.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    dst_tabs={r[0] for r in con.execute("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    ok&=(src_tabs==dst_tabs); log(f"    table set identical: {src_tabs==dst_tabs} ({len(dst_tabs)})")
    for t in sorted(src_tabs):
        s=[(c[1],c[2]) for c in con.execute(f"PRAGMA src.table_info('{t}')")]
        dd=[(c[1],c[2]) for c in con.execute(f"PRAGMA main.table_info('{t}')")]
        if s!=dd: ok=False; log(f"    [MISMATCH] {t}")
    log(f"    column parity: {ok}")
    integ=con.execute("PRAGMA integrity_check").fetchone()[0]; ok&=(integ=="ok"); log(f"    integrity: {integ}")
    bad=con.execute("SELECT COUNT(*) FROM main.WEEKLY_MONITOR WHERE WEEK_END_DATE > ?",[SNAPSHOT_DATE.isoformat()]).fetchone()[0]
    ok&=(bad==0)
    maxd=con.execute("SELECT MAX(WEEK_END_DATE) FROM main.WEEKLY_MONITOR WHERE YEAR=2026").fetchone()[0]
    log(f"    rows beyond {SNAPSHOT_DATE}: {bad} (max WEEK_END_DATE={maxd})")
    j_wm=con.execute("SELECT COUNT(*) FROM main.WEEKLY_MONITOR WHERE YEAR=2026 AND WEEK_NUM IN (22,23,24)").fetchone()[0]
    j_pcm=con.execute("SELECT COUNT(*) FROM main.PROCESSED_CARD_MONTHLY WHERE TRX_MONTH=202606").fetchone()[0]
    log(f"    June rows: WEEKLY(W22-24)={j_wm} PCM(202606)={j_pcm}")
    chk=pd.read_sql_query("SELECT WEEKLY_VOL v, WEEKLY_FBI f FROM main.WEEKLY_MONITOR WHERE YEAR=2026 AND WEEK_NUM IN (22,23,24)", con)
    bad_fbi=int(((chk.v>0)&(chk.f<=0)).sum()+((chk.v<=0)&(chk.f>0)).sum()); ok&=(bad_fbi==0)
    log(f"    new-week FBI/VOL sign anomalies: {bad_fbi}")
    agg=con.execute("SELECT (SELECT SUM(TOTAL_SV) FROM main.PROCESSED_CARD_MONTHLY WHERE TRX_MONTH=202606),(SELECT SUM(mx) FROM (SELECT MERCHANT_GROUP, MAX(TOTAL_SV) mx FROM main.PROCESSED_CARD_MONTHLY WHERE TRX_MONTH=202605 AND MERCHANT_GROUP IN (SELECT MERCHANT_GROUP FROM main.PROCESSED_CARD_MONTHLY WHERE TRX_MONTH=202606) GROUP BY MERCHANT_GROUP))").fetchone()
    pct=(agg[0]/agg[1]*100) if agg[1] else 0
    log(f"    June MTD vs May (agg SV): {pct:.1f}% (target ~50-55%)")
    log("\n    table                         old        new      delta")
    for t in sorted(src_tabs):
        o=con.execute(f"SELECT COUNT(*) FROM src.{t}").fetchone()[0]; n=con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        if o!=n: log(f"    {t:28} {o:>9,} {n:>9,} {n-o:>+9,}")
    con.commit(); con.close()
    log("\n"+"="*78); log(f"DONE -> {DST.name}  schema {'OK' if ok else 'WARN'}; through {SNAPSHOT_DATE}"); log("="*78)
    return 0 if ok else 1

if __name__=="__main__":
    raise SystemExit(main())
