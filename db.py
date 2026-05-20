import sqlite3
from datetime import datetime

db_name = 'biotech.db'

tracked_fields = ('status', 'enrollment', 'primary_completion_date')


def get_conn():
    return sqlite3.connect(db_name)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nct_id TEXT NOT NULL,
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
            snapshot_date TEXT NOT NULL,
            UNIQUE(nct_id, snapshot_date)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_nct_snapshot ON trials(nct_id, snapshot_date)")

    conn.commit()
    conn.close()


def init_landscape_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS disease_landscape (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nct_id TEXT NOT NULL,
            condition_searched TEXT NOT NULL,
            sponsor TEXT,
            company TEXT,
            phase TEXT,
            status TEXT,
            enrollment INTEGER,
            primary_completion_date TEXT,
            snapshot_date TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    
def init_market_sizing_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_sizing (
            condition TEXT PRIMARY KEY,
            annual_us_patients INTEGER,
            eligible_patient_rate REAL,
            estimated_drug_price INTEGER,
            addressable_market REAL,
            trial_count INTEGER,
            sponsor_count INTEGER,
            total_enrollment INTEGER,
            data_source TEXT,
            rationale TEXT,
            snapshot_date TEXT
        )
    """)
    conn.commit()
    conn.close()


def init_publications_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nct_id TEXT NOT NULL,
            pmid TEXT NOT NULL,
            title TEXT,
            journal TEXT,
            pub_date TEXT,
            first_seen TEXT NOT NULL,
            last_checked TEXT NOT NULL,
            UNIQUE(nct_id, pmid)
        )
    """)

    # Track last check time per nct_id even when no publication is found
    cur.execute("""
        CREATE TABLE IF NOT EXISTS publication_checks (
            nct_id TEXT PRIMARY KEY,
            last_checked TEXT NOT NULL
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_nct ON publications(nct_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_seen ON publications(first_seen)")

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


def _get_last_snapshot(cur, nct_id):
    """Return the most recent tracked fields for a trial, or None if never seen."""
    cur.execute("""
        SELECT status, enrollment, primary_completion_date
        FROM trials
        WHERE nct_id = ?
        ORDER BY snapshot_date DESC
        LIMIT 1
    """, (nct_id,))
    return cur.fetchone()


def _has_changed(last, trial):
    """Return True if any tracked field differs from the last snapshot."""
    last_status, last_enrollment, last_pcd = last

    status_changed = safe(trial.get('status')) != last_status

    new_enrollment = safe(trial.get('enrollment'))
    if last_enrollment is not None:
        enrollment_changed = new_enrollment != str(last_enrollment)
    else:
        enrollment_changed = new_enrollment is not None

    pcd_changed = safe(trial.get('primary_completion_date')) != last_pcd

    return status_changed or enrollment_changed or pcd_changed


def insert_trials(trials, company):
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.utcnow().date().isoformat()

    inserted = 0
    skipped = 0

    for t in trials:
        nct_id = safe(t.get('nct_id'))
        if not nct_id:
            continue

        last = _get_last_snapshot(cur, nct_id)

        if last is not None and not _has_changed(last, t):
            skipped += 1
            continue

        cur.execute("""
            INSERT OR IGNORE INTO trials
                (nct_id, company, sponsor, title, phase, fda_regulated, status,
                 study_type, conditions, interventions, enrollment, start_date,
                 primary_completion_date, primary_outcomes, secondary_outcomes,
                 last_updated, snapshot_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nct_id,
            safe(company),
            safe(t.get('sponsor')),
            safe(t.get('title')),
            safe(t.get('phase')),
            safe(t.get('fda_regulated')),
            safe(t.get('status')),
            safe(t.get('study_type')),
            safe(t.get('conditions')),
            safe(t.get('interventions')),
            safe(t.get('enrollment')),
            safe(t.get('start_date')),
            safe(t.get('primary_completion_date')),
            safe(t.get('primary_outcomes')),
            safe(t.get('secondary_outcomes')),
            safe(t.get('last_updated')),
            today
        ))

        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    conn.close()

    return inserted, skipped


def was_recently_checked(nct_id, days=7):
    """Return True if this nct_id was checked for publications within the last N days."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT last_checked FROM publication_checks
        WHERE nct_id = ?
    """, (nct_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return False

    last = datetime.fromisoformat(row[0]).date()
    today = datetime.utcnow().date()
    return (today - last).days < days


def mark_checked(nct_id):
    """Record that this nct_id was checked for publications today."""
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.utcnow().date().isoformat()

    cur.execute("""
        INSERT OR REPLACE INTO publication_checks (nct_id, last_checked)
        VALUES (?, ?)
    """, (nct_id, today))

    conn.commit()
    conn.close()


def insert_publication(nct_id, pub):
    """
    Insert a publication if it hasn't been seen before.
    Returns True if it was new, False if already existed.
    """
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.utcnow().date().isoformat()

    cur.execute("""
        INSERT OR IGNORE INTO publications
            (nct_id, pmid, title, journal, pub_date, first_seen, last_checked)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        nct_id,
        pub['pmid'],
        pub.get('title'),
        pub.get('journal'),
        pub.get('pub_date'),
        today,
        today
    ))

    is_new = cur.rowcount == 1
    conn.commit()
    conn.close()

    return is_new


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


def insert_landscape_trials(trials, condition):
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.utcnow().date().isoformat()

    for t in trials:
        nct_id = safe(t.get('nct_id'))

        cur.execute(
            "SELECT 1 FROM disease_landscape WHERE nct_id = ? AND condition_searched = ? AND snapshot_date = ?",
            (nct_id, condition, today)
        )
        if cur.fetchone():
            continue

        cur.execute("""
            INSERT INTO disease_landscape
                (nct_id, condition_searched, sponsor, company, phase, status,
                 enrollment, primary_completion_date, snapshot_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nct_id,
            safe(condition),
            safe(t.get('sponsor')),
            safe(t.get('company')),
            safe(t.get('phase')),
            safe(t.get('status')),
            safe(t.get('enrollment')),
            safe(t.get('primary_completion_date')),
            today
        ))

    conn.commit()
    conn.close()
