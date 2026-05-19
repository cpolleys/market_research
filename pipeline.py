from scraper import fetch_trials, fetch_trials_by_condition
from db import (init_db, init_company_table, insert_trials,
                init_landscape_table, insert_landscape_trials,
                init_publications_table, insert_publication, get_conn)
from mappings import resolve_company_sec, fetch_sec_tickers, get_biotech_universe, clean_name, sponsor_aliases
from signals import detect_changes, generate_signals, detect_new_publications, generate_publication_signals
from pubmed import get_latest_publication
from datetime import datetime
 
def _get_nct_ids_for_publication_check():
    """
    Return NCT IDs eligible for publication check:
      - Phase 3 or 4 (any status)
      - Phase 1 or 2 that are COMPLETED or TERMINATED
    Deduped to one row per nct_id using the most recent snapshot.
    """
    conn = get_conn()
    cur = conn.cursor()
 
    cur.execute("""
        WITH latest AS (
            SELECT nct_id, company, phase, status,
                   ROW_NUMBER() OVER (PARTITION BY nct_id ORDER BY snapshot_date DESC) AS rn
            FROM trials
        )
        SELECT nct_id, company, phase, status
        FROM latest
        WHERE rn = 1
          AND (
              phase IN ('PHASE3', 'PHASE4')
              OR (phase IN ('PHASE1', 'PHASE2') AND status IN ('COMPLETED', 'TERMINATED'))
          )
    """)
 
    results = cur.fetchall()
    conn.close()
    return results    
    
def run():
    init_db()
    init_company_table()
    init_publications_table()
    
    universe = get_biotech_universe()
    
    company_data = fetch_sec_tickers()
    
    num_companies = len(universe)
    total_inserted = 0
    total_skipped = 0
    count = 0
    
    for c in universe:
        company = clean_name(c["name"])
        search_name = sponsor_aliases.get(company, company)
        trials = fetch_trials(search_name)

        for trial in trials:
            sponsor = trial['sponsor']
            ticker = resolve_company_sec(sponsor, company_data)
            trial['company'] = ticker if ticker else sponsor
        
        inserted, skipped = insert_trials(trials, company)
        total_inserted += inserted
        total_skipped += skipped
        
        count += 1
        
        print(f'[{count}/{num_companies}] {company}: {inserted} new snapshots, {skipped} unchanged')
 
    print(f'\nDone. {total_inserted} total snapshots written, {total_skipped} trials unchanged.')
        
    changes = detect_changes()
    signals = generate_signals(changes)

    print('\n=== SIGNALS ===')
    if not signals:
        print("No changes detected.")
    else:
        for s in signals:
            print(s)
    
    # --- Publication signals ---
    print('\n=== CHECKING PUBLICATIONS ===')
    eligible = _get_nct_ids_for_publication_check()
    print(f'Checking {len(eligible)} trials for new publications...')
 
    pub_found = 0
    for nct_id, company, phase, status in eligible:
        pub = get_latest_publication(nct_id)
        if pub:
            is_new = insert_publication(nct_id, pub)
            if is_new:
                pub_found += 1
 
    print(f'Found {pub_found} new publications.')
 
    new_pubs = detect_new_publications()
    pub_signals = generate_publication_signals(new_pubs)
 
    print('\n=== PUBLICATION SIGNALS ===')
    if not pub_signals:
        print('No new publications detected.')
    else:
        for s in pub_signals:
            print(s)
 
def run_landscape(conditions):
    init_landscape_table()
    
    for condition in conditions:
        print(f'Fetching landscape for {condition}')
        trials = fetch_trials_by_condition(condition)
        insert_landscape_trials(trials, condition)
        print(f'Inserted {len(trials)} trials for {condition}')

if __name__ == '__main__':
    run()