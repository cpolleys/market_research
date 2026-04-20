import sqlite3

db_name = 'biotech.db'

def get_conn():
    return sqlite3.connect(db_name)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trials (
        nct_id TEXT,
        company TEXT,
        title TEXT,
        phase TEXT,
        status TEXT,
        last_updated TEXT,
        snapshot_date TEXT
    )
    """)
    
    conn.commit()
    conn.close()
    
def safe(x):
    if x is None:
        return None
    if isinstance(x, list):
        return ','.join(map(str, x))
    if isinstance(x, dict):
        return str(x)
    return x
    
def insert_trials(trials, company):
    conn = get_conn()
    cur = conn.cursor()

    for t in trials:
        
        cur.execute("""
        INSERT INTO trials VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            safe(t['nct_id']),
            safe(company),
            safe(t['title']),
            safe(t['phase']),
            safe(t['status']),
            safe(t['last_updated']),
            safe(t['snapshot_date'])
        ))

    conn.commit()
    conn.close()
    
def init_company_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS company_map (
        raw_name TEXT PRIMARY KEY,
        normalized_name TEXT,
        ticker TEXT,
        confidence REAL,
        source TEXT,
        last_seen TEXT
    )
    """)

    conn.commit()
    conn.close()