import sqlite3
c = sqlite3.connect('C:/Users/Lenovo/Documents/UMN/Semester 6 Magang/Project Magang/AnchorAutomationDashboard/Project/database/staging.db')
cursor = c.cursor()
for t in ['master_mid', 'raw_edw_card_share', 'raw_edw_mid', 'raw_edw_weekly']:
    print(f"Schema for {t}:")
    schema = c.execute(f"PRAGMA table_info('{t}')").fetchall()
    for s in schema:
        print(f"  {s[1]} ({s[2]})")
