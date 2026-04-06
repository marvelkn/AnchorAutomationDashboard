import sqlite3
c = sqlite3.connect('Project/database/staging.db')
for t in c.execute("SELECT name FROM sqlite_master WHERE type='table'"):
    name = t[0]
    print('\n======= Table:', name)
    for col in c.execute(f"PRAGMA table_info('{name}')"):
        print(f"  {col[1]} ({col[2]})")
