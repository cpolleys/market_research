from db import get_conn

def detect_changes():
    conn = get_conn()
    cur = conn.cursor()

    query = """
    WITH ranked AS (
        SELECT
            nct_id,
            company,
            status,
            snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY nct_id ORDER BY snapshot_date DESC) AS rn
        FROM trials
    )
    SELECT
        curr.nct_id,
        curr.company,
        curr.status   AS new_status,
        prev.status   AS old_status
    FROM ranked curr
    JOIN ranked prev
        ON curr.nct_id = prev.nct_id
        AND curr.rn = 1
        AND prev.rn = 2
    WHERE curr.status != prev.status
    """

    cur.execute(query)
    results = cur.fetchall()
    conn.close()

    return results

def generate_signals(changes):
    signals = []
    for nct_id, company, new_status, old_status in changes:
        if new_status == 'COMPLETED':
            signals.append(f'{company}: Trial {nct_id} completed')
 
        if new_status == 'TERMINATED':
            signals.append(f'{company}: Trial {nct_id} terminated')
 
        noteworthy = {'SUSPENDED', 'WITHDRAWN'}
        if new_status in noteworthy:
            signals.append(
                f'{company}: Trial {nct_id} status changed '
                f'{old_status} → {new_status}'
            )

    return signals