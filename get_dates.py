import sqlite3
c = sqlite3.connect('Project/database/staging.db')

tables = ['raw_edw_mid', 'raw_edw_card_share', 'raw_edw_weekly']
for t in tables:
    print(f"\n--- Table: {t} ---")
    
    try:
        # Check distinct EDW_FETCH_DATE
        res = c.execute(f"SELECT DISTINCT EDW_FETCH_DATE FROM {t} ORDER BY EDW_FETCH_DATE DESC LIMIT 5").fetchall()
        print(f"EDW_FETCH_DATE samples: {[r[0] for r in res]}")
    except Exception as e:
        print("Error fetching EDW_FETCH_DATE:", e)
        
    try:
        if t == 'raw_edw_card_share':
            res = c.execute(f"SELECT DISTINCT TRANSACTION_MONTH FROM {t} ORDER BY TRANSACTION_MONTH DESC LIMIT 5").fetchall()
            print(f"TRANSACTION_MONTH samples: {[r[0] for r in res]}")
        elif t == 'raw_edw_weekly':
            res = c.execute(f"SELECT DISTINCT YEAR, WEEK_NUM FROM {t} ORDER BY YEAR DESC, WEEK_NUM DESC LIMIT 5").fetchall()
            print(f"YEAR/WEEK_NUM samples: {res}")
    except Exception as e:
        pass
