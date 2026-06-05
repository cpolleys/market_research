from db import get_conn
from datetime import datetime, date, timedelta

def detect_changes():
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.utcnow().date().isoformat()

    query = """
    WITH ranked AS (
        SELECT
            nct_id,
            company,
            status,
            primary_completion_date,
            snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY nct_id ORDER BY snapshot_date DESC) AS rn
        FROM trials
    )
    SELECT
        curr.nct_id,
        curr.company,
        curr.status   AS new_status,
        prev.status   AS old_status,
        curr.primary_completion_date   AS new_pcd,
        prev.primary_completion_date   AS old_pcd
    FROM ranked curr
    JOIN ranked prev
        ON curr.nct_id = prev.nct_id
        AND curr.rn = 1
        AND prev.rn = 2
    WHERE (curr.status != prev.status
        OR curr.primary_completion_date != prev.primary_completion_date)
      AND curr.snapshot_date = ?
    """

    cur.execute(query, (today,))
    results = cur.fetchall()
    conn.close()

    return results

def detect_new_publications(days=30):
    """Return all publications first seen today."""
    conn = get_conn()
    cur = conn.cursor()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    cur.execute("""
        SELECT p.nct_id, t.company, p.pmid, p.title, p.journal, p.pub_date
        FROM publications p
        JOIN (
            SELECT nct_id, company FROM trials GROUP BY nct_id
        ) t ON p.nct_id = t.nct_id
        WHERE p.first_seen = date('now')
        AND p.pub_date >= ?
    """, (cutoff,))
 
    results = cur.fetchall()
    conn.close()
 
    return results

def generate_signals(changes):
    signals = []
    for nct_id, company, new_status, old_status, new_pcd, old_pcd in changes:
        
        if new_status != old_status:
            if new_status == 'COMPLETED':
                signals.append(f'{company}: Trial {nct_id} completed')
 
            elif new_status == 'TERMINATED':
                signals.append(f'{company}: Trial {nct_id} terminated')
 
            elif new_status in {'SUSPENDED', 'WITHDRAWN'}:
                signals.append(
                    f'{company}: Trial {nct_id} status changed '
                    f'{old_status} → {new_status}'
                )
 
        # Completion date slippage signal
        if new_pcd and old_pcd and new_pcd > old_pcd:
            signals.append(
                f'{company}: Trial {nct_id} completion date slipped '
                f'{old_pcd} → {new_pcd}'
            )
            
    return signals

def generate_publication_signals(publications):
    signals = []
    for nct_id, company, pmid, title, journal, pub_date in publications:
        signals.append(
            f'{company}: New publication for {nct_id} — '
            f'"{title}" ({journal}, {pub_date}) PMID:{pmid}'
        )
    return signals