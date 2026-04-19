from db import get_conn

def detect_changes():
    conn = get_conn()
    cur = conn.cursor()

    query = """
    SELECT t1.nct_id, t1.company, t1.status, t2.status
    FROM trials t1
    JOIN trials t2
    ON t1.nct_id = t2.nct_id
    WHERE t1.snapshot_date > t2.snapshot_date
    AND t1.status != t2.status
    """

    cur.execute(query)
    results = cur.fetchall()
    conn.close()

    return results

def generate_signals(changes):
    signals = []
    for nct_id, company, new_status, old_status in changes:
        if new_status == 'Completed':
            signals.append(f'{company}: Trial {nct_id} completed')

        if new_status == 'Terminated':
            signals.append(f'{company}: Trial {nct_id} terminated')

    return signals