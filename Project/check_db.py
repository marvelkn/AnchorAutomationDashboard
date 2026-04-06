import sqlite3
c = sqlite3.connect('C:/Users/Lenovo/Documents/UMN/Semester 6 Magang/Project Magang/AnchorAutomationDashboard/Project/database/staging.db')
cursor = c.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in staging.db:")
for row in tables:
    print(f"- {row[0]}")
    
    # Let's get the schema for the first few tables that look like raw data to understand column names
    if any(keyword in row[0].lower() for keyword in ['edc', 'qris', 'mid', 'raw', 'base']):
        print(f"  Schema for {row[0]}:")
        schema = c.execute(f"PRAGMA table_info('{row[0]}')").fetchall()
        for s in schema:
            print(f"    {s[1]} ({s[2]})")
