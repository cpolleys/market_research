import sqlite3

db_name = 'biotech.db'

def get_conn():
    return sqlite3.connect(db_name)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE trials (
        nct_id TEXT,
        company TEXT,
        sponsor TEXT,
        title TEXT,
        phase TEXT,
        fda_regulated INTEGER,
        status TEXT,
        study_type TEXT,
        conditions TEXT,
        interventions TEXT,
        enrollment INTEGER,
        start_date TEXT,
        primary_completion_date TEXT,
        primary_outcomes TEXT,
        secondary_outcomes TEXT,
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
        INSERT INTO trials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            safe(t.get("nct_id")),
            safe(company),
            safe(t.get("sponsor")),
            safe(t.get("title")),
            safe(t.get("phase")),
            safe(t.get("fda_regulated")),
            safe(t.get("status")),
            safe(t.get("study_type")),
            safe(t.get("conditions")),
            safe(t.get("interventions")),
            safe(t.get("enrollment")),
            safe(t.get("start_date")),
            safe(t.get("primary_completion_date")),
            safe(t.get("primary_outcomes")),
            safe(t.get("secondary_outcomes")),
            safe(t.get("last_updated")),
            safe(t.get("snapshot_date"))
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