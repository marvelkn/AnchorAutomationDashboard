import pandas as pd
import re
import os
import shutil
from datetime import datetime

def run_mid_cleaner(df_new, path_mid, backup_dir):
    """
    Executes the 3-Step Anchor Classification Pipeline on the new fetched DataFrame
    and merges it seamlessly with the Master Excel list.
    Returns:
    - total_new (int)
    - total_merged (int)
    """
    if len(df_new) == 0:
        return 0, 0

    df_master_raw = pd.read_excel(path_mid)
    df_master = df_master_raw.copy()
    df_master.columns = [str(c).strip().upper() for c in df_master.columns]
    
    # Fill necessary columns
    df_new.columns = [c.strip().upper() for c in df_new.columns]
    for col in ['SEGMENT', 'MERCHANT_BRAND', 'MERCHANT_GROUP']:
        if col not in df_new.columns:
            df_new[col] = None

    # Step 1: Mapping Logic
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
        if re.match(r'^SC[A-Z0-9]{2}\s+(SBUX|STARBUCKS)', n) or re.search(r'\b(SBUX|STARBUCKS)\b', n): return 'STARBUCKS', 'MAP GROUP'
        if re.match(r'^\d{3}\s+DAILY\s*FOODHALL', n) or re.match(r'^M0[0-9]{2}\s+(DAILY\s*)?FOODHALL', n) or re.search(r'\b(DAILY\s*)?FOODHALL\b', n) or re.search(r'\bDF\s+[A-Z]{3,}', n): return 'FOODHALL', 'MAP GROUP'
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

    # Classify only the rows not already a complete ANCHOR record, then assign
    # column-wise in one shot — avoids the per-cell .at[] writes of iterrows().
    already_anchor = (
        (df_new['SEGMENT'] == 'ANCHOR')
        & df_new['MERCHANT_BRAND'].notna()
        & df_new['MERCHANT_GROUP'].notna()
    )
    need = df_new[~already_anchor]
    res = need['MERCHANT_NAME'].map(match_anchor)          # Series of (brand, group)
    hit = res[res.map(lambda t: bool(t[0]) and bool(t[1]))]
    if len(hit) > 0:
        df_new.loc[hit.index, 'SEGMENT']        = 'ANCHOR'
        df_new.loc[hit.index, 'MERCHANT_BRAND'] = [t[0] for t in hit]
        df_new.loc[hit.index, 'MERCHANT_GROUP'] = [t[1] for t in hit]

    # Step 2: Extract Retail Brand to Group mapping for Step 2
    retail_brand_map = {}
    if 'SEGMENT' in df_master.columns:
        retail_master = df_master[df_master['SEGMENT'] == 'RETAIL']
        for idx, row in retail_master.iterrows():
            brand = str(row['MERCHANT_BRAND']).strip().upper()
            group = str(row['MERCHANT_GROUP']).strip().upper()
            if brand and group and brand not in ['NAN', 'MERCHANT RETAIL', '']:
                if brand not in retail_brand_map:
                    retail_brand_map[brand] = {'group': group}

    # Precompile each brand's word-boundary regex ONCE, instead of recompiling
    # `\b<brand>\b` for every brand on every merchant-name lookup.
    retail_patterns = [
        (re.compile(r'\b' + re.escape(brand) + r'\b'), brand, info['group'])
        for brand, info in retail_brand_map.items() if len(brand) >= 3
    ]

    def match_retail_brand(merchant_name):
        if pd.isna(merchant_name): return None, None
        name_upper = str(merchant_name).upper().strip()
        for pattern, brand, group in retail_patterns:
            if pattern.search(name_upper):
                return brand, group
        return None, None

    empty_mask = df_new['SEGMENT'].isna()
    for idx in df_new[empty_mask].index:
        merchant_name = df_new.at[idx, 'MERCHANT_NAME']
        df_new.at[idx, 'SEGMENT'] = 'RETAIL'
        brand, group = match_retail_brand(merchant_name)
        if brand and group:
            df_new.at[idx, 'MERCHANT_BRAND'] = brand
            df_new.at[idx, 'MERCHANT_GROUP'] = group
        else:
            df_new.at[idx, 'MERCHANT_BRAND'] = 'MERCHANT RETAIL'
            df_new.at[idx, 'MERCHANT_GROUP'] = 'MERCHANT RETAIL'

    # Step 3: DATASET MERGER (KEEP BETTER)
    df1 = df_master.copy()
    df2 = df_new.copy()

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
        df1_overlap = df1_overlap.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
        df2_overlap = df2_overlap.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
        
        df1_overlap_dict = df1_overlap.set_index('MERCHANT_ID').to_dict('index')
        df2_overlap_dict = df2_overlap.set_index('MERCHANT_ID').to_dict('index')
        
        for mid in overlap:
            row1 = df1_overlap_dict[mid]
            row2 = df2_overlap_dict[mid]
            
            score1 = sum([
                1 if pd.notna(row1.get('SEGMENT')) and str(row1.get('SEGMENT')).strip() != '' else 0,
                1 if pd.notna(row1.get('MERCHANT_BRAND')) and str(row1.get('MERCHANT_BRAND')).strip() != '' else 0,
                1 if pd.notna(row1.get('MERCHANT_GROUP')) and str(row1.get('MERCHANT_GROUP')).strip() != '' else 0,
            ])
            
            score2 = sum([
                1 if pd.notna(row2.get('SEGMENT')) and str(row2.get('SEGMENT')).strip() != '' else 0,
                1 if pd.notna(row2.get('MERCHANT_BRAND')) and str(row2.get('MERCHANT_BRAND')).strip() != '' else 0,
                1 if pd.notna(row2.get('MERCHANT_GROUP')) and str(row2.get('MERCHANT_GROUP')).strip() != '' else 0,
            ])
            
            row_to_keep = row2.copy() # fallback
            row_to_keep['MERCHANT_ID'] = mid
            
            if score2 > score1: pass
            elif score1 > score2:
                row_to_keep = row1.copy()
                row_to_keep['MERCHANT_ID'] = mid
            else:
                if pd.notna(row2.get('SEGMENT')) and row2.get('SEGMENT') == 'ANCHOR': pass
                elif pd.notna(row1.get('SEGMENT')) and row1.get('SEGMENT') == 'ANCHOR':
                    row_to_keep = row1.copy()
                    row_to_keep['MERCHANT_ID'] = mid
                    
            df_kept_duplicates.append(row_to_keep)
            
        df_kept_duplicates = pd.DataFrame(df_kept_duplicates)
        df_merged = pd.concat([df1_unique, df2_unique, df_kept_duplicates], ignore_index=True)

    df_merged['MERCHANT_ID'] = df_merged['MERCHANT_ID'].astype(str)
    df_merged = df_merged.drop_duplicates(subset=['MERCHANT_ID'], keep='first')
    df_merged = df_merged.sort_values(by='MERCHANT_ID')

    # Backup logic
    if backup_dir:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"master_mid_backup_{timestamp}.xlsx")
        if os.path.exists(path_mid):
            shutil.copy2(path_mid, backup_path)

    # Save Excel natively
    df_merged.to_excel(path_mid, index=False)
    
    # Optional logic: save the `df_merged` also tracking to SQL here if needed.
    
    return len(df_new), len(df_merged)
