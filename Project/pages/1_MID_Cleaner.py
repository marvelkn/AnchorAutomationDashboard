import streamlit as st
import pandas as pd
import re
from collections import Counter
import io
import os
import shutil
from datetime import datetime

st.title("🧹 ALL MID Cleaner (Ultimate Pipeline)")
st.markdown("""
This tool processes raw MID data using the advanced 3-step classification pipeline:
1. **Anchor Identification** (Regex pattern matching)
2. **Smart Retail Classification** (Learning from Master File)
3. **Accurate Dataset Merger** (`keep_better` strategy)
""")

# ==========================================
# PATHS & BACKUP SETUP
# ==========================================
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_MID   = os.path.join(BASE_DIR, "data", "master", "master_mid.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "data", "master", "backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

if not os.path.exists(PATH_MID):
    st.error("❌ Master MID File not found! Please upload it via the **⚙️ Global Settings** page first.")
    st.stop()

# ── Current master status ──
col_info, col_dl = st.columns([3, 1])
with col_info:
    mtime = datetime.fromtimestamp(os.path.getmtime(PATH_MID)).strftime("%d %b %Y %H:%M")
    msize = os.path.getsize(PATH_MID) // 1024
    st.success(f"✅ Master loaded — last updated **{mtime}** · {msize} KB")
with col_dl:
    with open(PATH_MID, "rb") as f:
        st.download_button("⬇️ Download Current Master", f,
                           file_name="master_mid_current.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── Rollback panel ──
backup_files = sorted(
    [b for b in os.listdir(BACKUP_DIR) if b.endswith(".xlsx")],
    reverse=True
)
with st.expander(f"🔁 Rollback / Restore Previous Master ({len(backup_files)} backups available)"):
    if not backup_files:
        st.info("No backups yet. Backups are created automatically each time you process new data.")
    else:
        for bfile in backup_files[:10]:  # show max 10
            bpath = os.path.join(BACKUP_DIR, bfile)
            bsize = os.path.getsize(bpath) // 1024
            btime = datetime.fromtimestamp(os.path.getmtime(bpath)).strftime("%d %b %Y %H:%M")
            rb1, rb2, rb3 = st.columns([4, 1, 1])
            rb1.markdown(f"📄 `{bfile}` — **{btime}** ({bsize} KB)")
            with open(bpath, "rb") as bf:
                rb2.download_button("⬇️ Download", bf, file_name=bfile,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"dl_{bfile}")
            if rb3.button("♻️ Restore", key=f"restore_{bfile}"):
                shutil.copy2(bpath, PATH_MID)
                st.success(f"✅ Restored master from backup: `{bfile}`. Reload the page to confirm.")
                st.session_state.pop('mid_result', None)
                st.rerun()

# ── Show download result banner if processing just finished ──
if 'mid_result' in st.session_state:
    res = st.session_state['mid_result']
    st.markdown("---")
    st.success(f"🎉 Processing complete! Master updated at {res['timestamp']}. Download your files below:")
    rc1, rc2, rc3 = st.columns(3)
    rc1.download_button(
        label="📥 Download NEW Master (.xlsx)",
        data=res['excel_bytes'],
        file_name=res['excel_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary", key="mid_dl_new"
    )
    rc2.download_button(
        label="🔙 Download BACKUP",
        data=res['backup_bytes'],
        file_name=res['backup_name'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="mid_dl_bak"
    )
    rc3.download_button(
        label="📄 Download CSV",
        data=res['csv_bytes'],
        file_name=res['csv_name'],
        mime="text/csv", key="mid_dl_csv"
    )
    if st.button("❌ Dismiss", key="mid_dismiss"):
        del st.session_state['mid_result']
        st.rerun()

st.markdown("---")
new_data_file = st.file_uploader("⬆️ Upload NEW Raw Data (To Classify & Merge with Master)", type=["csv", "xlsx", "xls"])

if new_data_file:
    if st.button("🚀 Run Classification & Merge Pipeline", type="primary"):
        
        try:
            # ==========================================
            # LOAD MASTER
            # ==========================================
            with st.spinner("Loading Master Reference..."):
                df_master_raw = pd.read_excel(PATH_MID)
                df_master = df_master_raw.copy()
                df_master.columns = [str(c).strip().upper() for c in df_master.columns]
                
                # Extract Brand to Group mapping for Step 1
                if 'MERCHANT_GROUP' in df_master.columns and 'MERCHANT_BRAND' in df_master.columns:
                    df_ref = df_master[['MERCHANT_GROUP', 'MERCHANT_BRAND']].copy()
                else:
                    df_ref = df_master.iloc[:, :2].copy()
                    df_ref.columns = ['MERCHANT_GROUP', 'MERCHANT_BRAND']
                    
                df_ref = df_ref[df_ref['MERCHANT_GROUP'].notna() & (df_ref['MERCHANT_GROUP'] != 'MERCHANT_GROUP')]
                
                brand_to_group = {}
                for idx, row in df_ref.iterrows():
                    brand = str(row['MERCHANT_BRAND']).strip()
                    group = str(row['MERCHANT_GROUP']).strip()
                    if brand and group and brand != 'nan':
                        brand_to_group[brand.upper()] = group
                        
                # Extract Retail Brand to Group mapping for Step 2
                retail_brand_map = {}
                if 'SEGMEN' in df_master.columns:
                    retail_master = df_master[df_master['SEGMEN'] == 'RETAIL']
                    for idx, row in retail_master.iterrows():
                        brand = str(row['MERCHANT_BRAND']).strip().upper()
                        group = str(row['MERCHANT_GROUP']).strip().upper()
                        if brand and group and brand not in ['NAN', 'MERCHANT RETAIL', '']:
                            if brand not in retail_brand_map:
                                retail_brand_map[brand] = {'group': group}

            # ==========================================
            # LOAD NEW DATA
            # ==========================================
            with st.spinner("Loading New Data..."):
                if new_data_file.name.lower().endswith('.csv'):
                    try:
                        df_new = pd.read_csv(new_data_file, encoding='utf-8')
                    except UnicodeDecodeError:
                        df_new = pd.read_csv(new_data_file, encoding='latin-1')
                else:
                    df_new = pd.read_excel(new_data_file)
                    
                df_new.columns = [c.strip().upper() for c in df_new.columns]
                for col in ['SEGMEN', 'MERCHANT_BRAND', 'MERCHANT_GROUP']:
                    if col not in df_new.columns:
                        df_new[col] = None

            # ========================================================================
            # STEP 1: SMART ANCHOR MATCHER
            # ========================================================================
            def match_anchor(name):
                if pd.isna(name): return None, None
                n = str(name).upper().strip()
                
                # ALFAMART / ALFA GROUP
                if re.match(r'^9A[A-Z]{2}\s+(D\+D|DAN\+DAN)', n) or re.search(r'\bD\+D\b', n) or re.search(r'\bDAN\+DAN\b', n) or re.match(r'^DANDAN\s+', n) or re.search(r'\bD\s*\+\s*D\b', n) or re.match(r'^9A[A-Z0-9]{2}\s+D\s*\+\s*D', n):
                    return 'DAN+DAN', 'ALFA GROUP'
                if re.match(r'^CI\d{2}ALFAMART', n) or re.search(r'\bKSB\s+', n) or re.search(r'\bCV[A-Z]{2,5}\s+KS[BO]', n) or re.match(r'^[0-9A-Z]{4}\s+CV[A-Z]{2,5}\s+', n) or re.search(r'\bKSO\b', n) or re.search(r'\bBACKUP\b', n) or 'ALFA - ART' in n or 'ALFAMRT' in n or re.match(r'^BACK\s*-\s*P\s+[A-Z0-9]+\s+FRC', n) or re.match(r'^BACKUP\s+[A-Z0-9]+\s+FRC', n):
                    return 'ALFAMART', 'ALFA GROUP'
                if re.search(r'\bMIDI\b', n) and not re.search(r'\b(MIDIAN|MIDIS)\b', n):
                    return 'ALFAMART', 'ALFA GROUP'
                if re.match(r'^MIDI\s+\w{3,5}\s+', n) or re.match(r'^MIDI\s+-\s+[A-Z0-9]{4}', n) or re.match(r'^MIDI\s+[A-Z]\d{3}[A-Z0-9]', n) or re.match(r'^[A-Z]{2}\d{2}\s+MIDI', n) or re.match(r'^S[A-Z]\d[A-Z0-9]\s+(-\s*)?MIDI', n) or re.match(r'^S[A-Z]Z\d+\s+MIDI', n) or re.match(r'^SM\d{2}[A-Z]?\s+(-\s*)?MIDI', n) or re.match(r'^SMZ\d\s+(-\s*)?MIDI', n) or re.match(r'^[A-Z]{2}\d{2,4}\s+ALFAMART', n) or re.match(r'^\d{4}[A-Z]?\s+(ALFAMART|MIDI)', n) or re.search(r'\bALFAMART\b', n) or re.search(r'\bALFAMIDI\b', n):
                    return 'ALFAMART', 'ALFA GROUP'
                
                # INDOMARET
                if 'POINT CAFE' in n or 'POINT COFFEE' in n or 'POINT REST AREA' in n or 'POINT LAUNDRY' in n or re.match(r'^IDM\s+PC\s+[A-Z0-9]{4}', n):
                    return 'POINT COFFEE', 'INDOMARET'
                if re.match(r'^(INDOMARET|IDM)\s+[A-Z0-9]{4}', n) or (re.search(r'\bINDOMARET\b', n) and not re.search(r'\b(PENDOPO|JURAGAN|PURBA)\b', n)):
                    return 'INDOMARET', 'INDOMARET'
                
                # MAP GROUP
                if re.match(r'^SC[A-Z0-9]{2}\s+(SBUX|STARBUCKS)', n) or re.search(r'\b(SBUX|STARBUCKS)\b', n):
                    return 'STARBUCKS', 'MAP GROUP'
                if re.match(r'^\d{3}\s+DAILY\s*FOODHALL', n) or re.match(r'^M0[0-9]{2}\s+(DAILY\s*)?FOODHALL', n) or re.search(r'\b(DAILY\s*)?FOODHALL\b', n) or re.search(r'\bDF\s+[A-Z]{3,}', n):
                    return 'FOODHALL', 'MAP GROUP'
                if re.match(r'^RA\d{2}\s+SEPHORA', n) or re.search(r'\bSEPHORA\b', n): return 'SEPHORA', 'MAP GROUP'
                if re.match(r'^WY[A-Z0-9]{1,2}\s+SUBWAY', n) or re.match(r'^WY\d{2}\s*SUBWAY', n) or re.search(r'\bSUBWAY\b', n): return 'SUBWAY', 'MAP GROUP'
                if re.match(r'^CV[A-Z0-9]{2}\s+CV\b', n) or 'CONVERS' in n or 'CONVERSE' in n: return 'CONVERSE', 'MAP GROUP'
                if 'STEVE MADDEN' in n or 'STEVEMADDEN' in n or re.match(r'^ED\d{2}\s+STEVE\b', n): return 'STEVE MADDEN', 'MAP GROUP'
                if 'FITFLOP' in n: return 'FITFLOP', 'MAP GROUP'
                if re.match(r'^H0\d{2}\s+HOKA', n) or re.search(r'\bHOKA\b', n): return 'HOKA', 'MAP GROUP'
                if 'LEGO' in n: return 'LEGO', 'MAP GROUP'
                if re.match(r'^AD\d{2}', n) or 'ADIDAS' in n: return 'ADIDAS', 'MAP GROUP'
                if re.match(r'^A\d{3}\s+ANTA', n): return 'ANTA', 'MAP GROUP'
                if 'KENNETH COLE' in n: return 'KENNETH COLE', 'MAP GROUP'
                if 'CLARKS' in n: return 'CLARKS', 'MAP GROUP'
                if re.match(r'^DL\d{2}\s+ALDO', n): return 'ALDO', 'MAP GROUP'
                if re.match(r'^(LC|BF)\d{2}[A-Z]?\s+LACOSTE', n) or re.search(r'\bLACOSTE\b', n): return 'LACOSTE', 'MAP GROUP'
                if 'CALVINKLEIN' in n or re.search(r'\bCK\s', n) or 'CALVIN KLEIN' in n: return 'CALVIN KLEIN', 'MAP GROUP'
                if re.match(r'^DM\d{2}', n) or 'DOC MARTENS' in n or 'DR.MARTENS' in n or 'DOCMART' in n or 'DR MARTENS' in n: return 'DR MARTENS', 'MAP GROUP'
                if re.match(r'^PU\d{2}\s+PUMA', n) or re.search(r'\bPUMA\b', n): return 'PUMA', 'MAP GROUP'
                if 'STACCATO' in n or re.match(r'^SA\d{2}\s+(STACCATO|SA)\b', n) or re.match(r'^SA[A-Z0-9]{2}\s', n) or 'SA JKTPREMIUM' in n: return 'STACCATO', 'MAP GROUP'
                if 'SWAROVSKI' in n: return 'SWAROVSKI', 'MAP GROUP'
                if re.match(r'^SK[A-Z]\d\s+(SK|SKECHERS)', n) or re.search(r'\bSKECHERS\b', n): return 'SKECHERS', 'MAP GROUP'
                if re.match(r'^AF\d{2,4}', n): return 'ATHLETE FOOT', 'MAP GROUP'
                if re.match(r'^AS\d{2}\s*ASTEC', n) or 'ASTEC' in n: return 'ASTEC', 'MAP GROUP'
                if re.match(r'^AX\d{2,4}', n) or 'ASICS' in n: return 'ASICS', 'MAP GROUP'
                if re.match(r'^PX\d{2}\s+PAZZION', n): return 'PAZZION', 'MAP GROUP'
                if re.match(r'^CX[A-Z0-9]{2}\s+CX\b', n) or 'CROCS' in n: return 'CROCS', 'MAP GROUP'
                if re.match(r'^PY[A-Z0-9]{2}\s+PY\b', n) or 'PAYLESS' in n or 'PAYLEES' in n: return 'PAYLESS', 'MAP GROUP'
                if re.match(r'^UN\d{2}\s+(FLYING\s*TIGER|FLY.*TIGER)', n) or re.match(r'^UN\d{2}\s+FTC', n) or re.search(r'\b(FLYING|FLAYING)\s*TIGER\b', n) or re.search(r'\bFTC\b', n): return 'FLAYING TIGER', 'MAP GROUP'
                if re.match(r'^(S0|KS[A-Z])\d{1,2}\s+.*SOGO', n) or re.search(r'\bSOGO\b', n): return 'SOGO', 'MAP GROUP'
                if re.match(r'^LG\d{2}\s+SMIGGLE', n) or re.search(r'\bSMIGGLE\b', n): return 'SMIGGLE', 'MAP GROUP'
                if re.match(r'^(M\d{3}|NPI)\s+DIGIMAP', n) or re.search(r'\bDIGIMAP\b', n): return 'DIGIMAP', 'MAP GROUP'
                if re.match(r'^QF\d{2}\s+DIGIPLUS', n): return 'DIGIPLUS', 'MAP GROUP'
                if re.match(r'^MAA\d\s+(BAZAAR\s+)?MAA', n): return 'MAXMARA', 'MAP GROUP'
                if re.match(r'^KS[A-Z]\d\s+(KIDZ?|KS)\s+STATION', n) or re.match(r'^BC[A-Z]{2}\s+(BZR\s+)?(KID[SZ]|KS)\b', n) or 'KIDZ GRANDCITY' in n or 'KIDS AEON' in n or 'KS CENTER' in n or 'KS CAMBRIDGE' in n: return 'KIDZ STATION', 'MAP GROUP'
                if 'PLANET SPORTS' in n or 'PLANET SPORT' in n or 'PLANETSPORT' in n or re.match(r'^PS\d{2}\s+PS\b', n): return 'PLANET SPORTS', 'MAP GROUP'
                if re.match(r'^BZ[A-Z0-9]{2}', n) or 'BZR SS' in n or 'BAZAAR SS' in n or 'SPORTSTATION' in n or re.match(r'^SS[A-Z0-9]{2}\s', n) or 'SPORTS STATION' in n or 'SPORT STATION' in n or 'BZR SP' in n or re.match(r'^SS[A-Z]{2}\s+SS\b', n): return 'SPORTS STATION', 'MAP GROUP'
                if re.match(r'^FQ\d{2,4}', n): return 'FOOTLOCKER', 'MAP GROUP'
                
                # KAWAN LAMA
                if 'EYESOUL' in n or 'EYESEOUL' in n: return 'EYESOUL', 'KAWAN LAMA'
                if 'IE LP' in n or 'HCIR IE' in n or 'INFORMA ELECTRONIC' in n or ('HCIR' in n and 'IE' in n) or re.match(r'^QR\s+J4\d{2}', n): return 'INFORMA ELECTRONIC', 'KAWAN LAMA'
                if 'HCIR' in n and 'IE' not in n: return 'INFORMA', 'KAWAN LAMA'
                if 'INF ' in n and 'FESTIVA' in n: return 'INFORMA', 'KAWAN LAMA'
                if re.match(r'^J\d{3}[A-Z]?\s+(PAM\s+|PAMERAN\s+|OUTLET\s+)?INFORMA', n) or re.match(r'^QR\s+J\d{3}[A-Z]*\s*(HCIR\s*)?INFORMA', n) or re.match(r'^J3[0-9][A-Z0-9]\s+(INFORMA|PAM|PAMERAN)', n) or re.match(r'^QR\s+J5\d{2}', n) or re.match(r'^INFORMA\s+WELLNESS', n) or re.search(r'\bINFORMA\s+(WELLNESS|LP|PURI|DUTA|OUTLET)', n): return 'INFORMA', 'KAWAN LAMA'
                if 'INFORMA' in n:
                    if 'ELECTRONIC' in n or ' IE ' in n: return 'INFORMA ELECTRONIC', 'KAWAN LAMA'
                    return 'INFORMA', 'KAWAN LAMA'
                if re.match(r'^FD\d{2}\s+GO!', n) or re.match(r'^FD\d{2}\s+GGC', n) or re.search(r'\bGGC\b', n) or re.search(r'\bGO\s*!?\s*GO\s*!?\s*CURRY\b', n) or 'GOGOCURRY' in n: return 'GO! GO! CURRY', 'KAWAN LAMA'
                if re.match(r'^A\d{3}\s+PENDOPO', n) or re.match(r'^IDMTI\d{1,2}[A-Z]{2,5}\s+PENDOPO', n): return 'PENDOPO', 'KAWAN LAMA'
                if re.match(r'^(FA|GDC)\d{2,4}', n) or re.search(r'\b(GDC|GINDACO)\b', n): return 'GINDACO', 'KAWAN LAMA'
                if re.match(r'^(QR\s+)?A\d{3}\s+(AZKO|ACE)', n) or re.search(r'\bAZKO\b', n) or re.search(r'\bACE\b', n): return 'AZKO', 'KAWAN LAMA'
                if re.match(r'^T\d{3}\s+.*TOYS', n) or re.search(r'\bTOYS\s+KINGDOM', n): return 'TOYS KINGDOM', 'KAWAN LAMA'
                if re.match(r'^(QR\s+)?F\d{3}[A-Z]?\s+CHATIME', n) or re.search(r'\bCHATIME\b', n): return 'CHATIME', 'KAWAN LAMA'
                if re.match(r'^A\d{3}\s+ATARU', n) or 'ATARU' in n: return 'ATARU', 'KAWAN LAMA'
                if 'SELMA' in n: return 'SELMA', 'KAWAN LAMA'

                # MITRA10
                if re.match(r'^MITRA\s*10\b', n) or re.search(r'\bMITRA\s*10\b', n): return 'MITRA10', 'MITRA10'

                # STANDALONE BRANDS
                if 'DWIDAYA' in n: return 'DWIDAYA TOUR', 'DWIDAYA'
                if re.search(r'\bBEARD\s+PAPAS?\b', n): return 'BEARD PAPA', 'BEARD PAPA'
                if 'BANBAN' in n: return 'BANBAN', 'BANBAN'
                if re.search(r'\bHOKBEN\b', n): return 'HOKBEN', 'HOKBEN'
                if re.search(r'\bHOP\s+HOP\b', n): return 'HOP HOP', 'HOP HOP'
                if re.search(r'\bOPTIK\s+MELAWAI\b', n): return 'OPTIK MELAWAI', 'OPTIK MELAWAI'
                if 'YOSHINOYA' in n: return 'YOSHINOYA', 'YOSHINOYA'
                if re.match(r'^SOLARIA\b', n) or re.search(r'\bSOLARIA\b', n): return 'SOLARIA', 'SOLARIA'
                if re.match(r'^SOUR\s+SALLY', n) or re.search(r'\bSOUR\s+SALLY\b', n): return 'SOUR SALLY', 'SOUR SALLY'
                if re.match(r'^SHIHLIN\b', n) or re.search(r'\bSHIHLIN\b', n): return 'SHIHLIN', 'SHIHLIN'
                if re.match(r'^HOKKAIDO', n): return 'HOKKAIDO BAKED CHEESE', 'HOKKAIDO BAKED CHEESE'
                if re.match(r'^EKA\s+HOSPITAL', n) or re.search(r'\bEKA\s+HOSPITAL\b', n): return 'EKA HOSPITAL', 'EKA HOSPITAL'
                if re.match(r'^IKEA\s+', n): return 'IKEA', 'IKEA'
                if re.match(r'^MIXUE\b', n): return 'MIXUE', 'MIXUE'

                # ANCOL
                if re.match(r'^LOKET\s+(ATLANTIS|DHOLPIN|FAST\s+TRACK|JBL|MP\s+PARK|OCEAN|PREMIUM|SEAWORLD)', n) or re.match(r'^PUTRI\s+DUYUNG', n) or re.match(r'^MERCH\s+', n) or re.search(r'\b(DUFAN|ANCOL)\b', n): return 'ANCOL', 'ANCOL'

                # PERTAMINA RETAIL
                if re.match(r'^BRIGHT\b', n) or re.match(r'^BRIGHT[A-Z]', n): return 'BRIGHT STORE', 'PERTAMINA RETAIL'
                if re.match(r'^SPBU\s+\d', n) or re.match(r'^SPBU\s+PERTAMINA', n): return 'SPBU PERTAMINA', 'PERTAMINA RETAIL'

                # PIZZA HUT
                if re.match(r'^PHD\s+[A-Z]{3,}', n) and not re.search(r'\b(LAUNDRY|PHDM)', n): return 'PIZZA HUT RESTAURANT', 'PIZZA HUT'
                if re.match(r'^PIZZA\s*HUT\b', n): return 'PIZZA HUT RESTAURANT', 'PIZZA HUT'

                # STEVEN GROUP
                if re.match(r'^BK\d{2,3}', n) or re.match(r'^BK[A-Z0-9]{1,2}', n) or re.match(r'^BK\s+', n) or re.match(r'^BK[A-Z]\d\s+BURGER\s+KING', n) or re.search(r'\bBURGER\s+KING\b', n): return 'BURGER KING', 'STEVEN GROUP'
                if re.match(r'^SUSHI\s+TEI\b', n) or re.search(r'\bSUSHI\s+TEI\b', n): return 'SUSHI TEI', 'STEVEN GROUP'
                if re.match(r'^YOGURT\s+REPUBLIC\b', n) or re.search(r'\bYOGURT\s+REPUBLIC\b', n): return 'YOGURT REPUBLIC', 'STEVEN GROUP'

                # CHAMP RESTO
                if 'RAACHAA' in n or 'RAA CHA' in n or 'RAA C' in n or 'RAACHA' in n: return 'RAACHA', 'CHAMP RESTO'
                if re.search(r'\bGOKANA\b', n): return 'GOKANA', 'CHAMP RESTO'
                if re.match(r'^MONSIEUR\s+SPOON\b', n) or re.search(r'\bMONSIEUR\s+SPOON\b', n): return 'MONSIEUR SPOON', 'CHAMP RESTO'

                # LOTTE GROUP
                if re.search(r'\bLOTTE\s+MART\b', n): return 'LOTTE MART', 'LOTTE GROUP'
                if re.search(r'\bLOTTE\s+GROSIR\b', n): return 'LOTTE GROSIR', 'LOTTE GROUP'

                return None, None

            with st.spinner("Step 1: Parsing Anchors..."):
                new_anchor = 0
                updated_anchor = 0
                
                for i, row in df_new.iterrows():
                    if pd.notna(row['SEGMEN']) and row['SEGMEN'] == 'ANCHOR' and pd.notna(row['MERCHANT_BRAND']) and pd.notna(row['MERCHANT_GROUP']):
                        continue
                    
                    brand, group = match_anchor(row['MERCHANT_NAME'])
                    if brand and group:
                        was_anchor = (pd.notna(row['SEGMEN']) and row['SEGMEN'] == 'ANCHOR')
                        df_new.at[i, 'SEGMEN'] = 'ANCHOR'
                        df_new.at[i, 'MERCHANT_BRAND'] = brand
                        df_new.at[i, 'MERCHANT_GROUP'] = group
                        if was_anchor:
                            updated_anchor += 1
                        else:
                            new_anchor += 1

                st.info(f"Step 1 Complete: {new_anchor} NEW Anchors, {updated_anchor} UPDATED Anchors.")

            # ========================================================================
            # STEP 2: SMART RETAIL MATCHER
            # ========================================================================
            def match_retail_brand(merchant_name):
                if pd.isna(merchant_name): return None, None
                name_upper = str(merchant_name).upper().strip()
                for brand, info in retail_brand_map.items():
                    if len(brand) < 3: continue
                    pattern = r'\b' + re.escape(brand) + r'\b'
                    if re.search(pattern, name_upper):
                        return brand, info['group']
                return None, None

            with st.spinner("Step 2: Parsing Smart Retail..."):
                empty_mask = df_new['SEGMEN'].isna()
                matched_brands = 0
                default_retail = 0
                
                for idx in df_new[empty_mask].index:
                    merchant_name = df_new.at[idx, 'MERCHANT_NAME']
                    df_new.at[idx, 'SEGMEN'] = 'RETAIL'
                    brand, group = match_retail_brand(merchant_name)
                    if brand and group:
                        df_new.at[idx, 'MERCHANT_BRAND'] = brand
                        df_new.at[idx, 'MERCHANT_GROUP'] = group
                        matched_brands += 1
                    else:
                        df_new.at[idx, 'MERCHANT_BRAND'] = 'MERCHANT RETAIL'
                        df_new.at[idx, 'MERCHANT_GROUP'] = 'MERCHANT RETAIL'
                        default_retail += 1

                st.info(f"Step 2 Complete: {matched_brands} Mapped Retail, {default_retail} Default Retail.")

            # ========================================================================
            # STEP 3: DATASET MERGER (KEEP BETTER)
            # ========================================================================
            with st.spinner("Step 3: Merging & Deduplicating Data..."):
                df1 = df_master.copy()
                df2 = df_new.copy()
                
                if 'MERCHANT_ID' not in df1.columns or 'MERCHANT_ID' not in df2.columns:
                    st.error("MERCHANT_ID column missing in one of the datasets!")
                    st.stop()
                    
                set1 = set(df1['MERCHANT_ID'].dropna())
                set2 = set(df2['MERCHANT_ID'].dropna())
                overlap = set1.intersection(set2)
                
                if len(overlap) == 0:
                    df_merged = pd.concat([df1, df2], ignore_index=True)
                else:
                    df1_unique = df1[~df1['MERCHANT_ID'].isin(overlap)].copy()
                    df2_unique = df2[~df2['MERCHANT_ID'].isin(overlap)].copy()
                    df1_overlap = df1[df1['MERCHANT_ID'].isin(overlap)].copy()
                    df2_overlap = df2[df2['MERCHANT_ID'].isin(overlap)].copy()
                    
                    df_kept_duplicates = []
                    kept_count_1 = 0
                    kept_count_2 = 0
                    
                    # Convert to dictionaries for faster lookup
                    # Drop internal duplicates safely before converting to dictionaries
                    df1_overlap = df1_overlap.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
                    df2_overlap = df2_overlap.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
                    
                    df1_overlap_dict = df1_overlap.set_index('MERCHANT_ID').to_dict('index')
                    df2_overlap_dict = df2_overlap.set_index('MERCHANT_ID').to_dict('index')
                    
                    for mid in overlap:
                        row1 = df1_overlap_dict[mid]
                        row2 = df2_overlap_dict[mid]
                        
                        score1 = sum([
                            1 if pd.notna(row1.get('SEGMEN')) and str(row1.get('SEGMEN')).strip() != '' else 0,
                            1 if pd.notna(row1.get('MERCHANT_BRAND')) and str(row1.get('MERCHANT_BRAND')).strip() != '' else 0,
                            1 if pd.notna(row1.get('MERCHANT_GROUP')) and str(row1.get('MERCHANT_GROUP')).strip() != '' else 0,
                        ])
                        
                        score2 = sum([
                            1 if pd.notna(row2.get('SEGMEN')) and str(row2.get('SEGMEN')).strip() != '' else 0,
                            1 if pd.notna(row2.get('MERCHANT_BRAND')) and str(row2.get('MERCHANT_BRAND')).strip() != '' else 0,
                            1 if pd.notna(row2.get('MERCHANT_GROUP')) and str(row2.get('MERCHANT_GROUP')).strip() != '' else 0,
                        ])
                        
                        row_to_keep = row2.copy() # fallback
                        row_to_keep['MERCHANT_ID'] = mid
                        
                        if score2 > score1:
                            kept_count_2 += 1
                        elif score1 > score2:
                            row_to_keep = row1.copy()
                            row_to_keep['MERCHANT_ID'] = mid
                            kept_count_1 += 1
                        else:
                            if pd.notna(row2.get('SEGMEN')) and row2.get('SEGMEN') == 'ANCHOR':
                                kept_count_2 += 1
                            elif pd.notna(row1.get('SEGMEN')) and row1.get('SEGMEN') == 'ANCHOR':
                                row_to_keep = row1.copy()
                                row_to_keep['MERCHANT_ID'] = mid
                                kept_count_1 += 1
                            else:
                                kept_count_2 += 1
                                
                        df_kept_duplicates.append(row_to_keep)
                        
                    df_kept_duplicates = pd.DataFrame(df_kept_duplicates)
                    df_merged = pd.concat([df1_unique, df2_unique, df_kept_duplicates], ignore_index=True)
                
                # Format to prevent exponential notation or weird formats
                df_merged['MERCHANT_ID'] = df_merged['MERCHANT_ID'].astype(str)
                
                # Ultimate Safety Check: Drop final duplicates (keeping the first occurrence)
                initial_count = len(df_merged)
                df_merged = df_merged.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
                duplicates_dropped = initial_count - len(df_merged)
                
                df_merged = df_merged.sort_values(by='MERCHANT_ID')

            st.success("✅ Ultimate Cleaning and Merger Pipeline Complete!")
            if duplicates_dropped > 0:
                st.warning(f"⚠️ Cleaned {duplicates_dropped} internal duplicates that existed within the original datasets.")
            else:
                st.success("✅ Flawless execution. Zero duplicate `MERCHANT_ID`s in final dataset.")
            
            st.metric("Total Output Records", f"{len(df_merged):,}")
            
            # Display distribution
            anchor_total = len(df_merged[df_merged['SEGMEN'] == 'ANCHOR'])
            retail_total = len(df_merged[df_merged['SEGMEN'] == 'RETAIL'])
            
            st.write("### Data Distribution")
            st.info(f"**ANCHOR**: {anchor_total:,} records")
            st.info(f"**RETAIL**: {retail_total:,} records")
            
            # ── Backup old master BEFORE overwriting ──
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"master_mid_backup_{timestamp}.xlsx"
            backup_path = os.path.join(BACKUP_DIR, backup_name)
            shutil.copy2(PATH_MID, backup_path)
            
            # ── Save new merged file as master ──
            import io as _io
            excel_buf = _io.BytesIO()
            df_merged.to_excel(excel_buf, index=False)
            excel_buf.seek(0)
            excel_bytes = excel_buf.read()
            
            # Write to disk
            with open(PATH_MID, 'wb') as f:
                f.write(excel_bytes)
            
            with open(backup_path, 'rb') as bf:
                backup_bytes = bf.read()
            
            csv_bytes = df_merged.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            
            # Store in session_state and rerun so the page refreshes with new status
            st.session_state['mid_result'] = {
                'timestamp': timestamp,
                'excel_bytes': excel_bytes,
                'excel_name': f"master_mid_{timestamp}.xlsx",
                'backup_bytes': backup_bytes,
                'backup_name': backup_name,
                'csv_bytes': csv_bytes,
                'csv_name': f"MID_MERGED_{timestamp}.csv",
            }
            st.rerun()

        except Exception as e:
            st.error(f"Error encountered: {e}")
