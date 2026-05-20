from scraper import fetch_trials, fetch_trials_by_condition
from db import (init_db, init_company_table, insert_trials,
                init_landscape_table, insert_landscape_trials,
                init_publications_table, insert_publication,
                was_recently_checked, mark_checked, get_conn)
from mappings import (resolve_company_sec, fetch_sec_tickers, get_biotech_universe,
                      clean_name, sponsor_aliases)
from signals import (detect_changes, generate_signals,
                     detect_new_publications, generate_publication_signals)
from pubmed import get_latest_publication
from datetime import datetime, date


def _get_nct_ids_for_publication_check():
    """
    Return NCT IDs eligible for publication check:
      - Phase 3 or 4 (any status)
      - Phase 1 or 2 that are COMPLETED or TERMINATED
      - Only trials with primary_completion_date within the last 2 years or in the future
    Deduped to one row per nct_id using the most recent snapshot.
    """
    conn = get_conn()
    cur = conn.cursor()

    two_years_ago = date(date.today().year - 2, date.today().month, date.today().day).isoformat()

    cur.execute("""
        WITH latest AS (
            SELECT nct_id, company, phase, status, primary_completion_date,
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
          AND (
              primary_completion_date IS NULL
              OR primary_completion_date >= ?
          )
    """, (two_years_ago,))

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

    for count, c in enumerate(universe, start=1):
        company = clean_name(c['name'])
        search_name = sponsor_aliases.get(company, company)
        trials = fetch_trials(search_name)

        for trial in trials:
            sponsor = trial['sponsor']
            ticker = resolve_company_sec(sponsor, company_data)
            trial['company'] = ticker if ticker else sponsor

        inserted, skipped = insert_trials(trials, company)
        total_inserted += inserted
        total_skipped += skipped

        print(f'[{count}/{num_companies}] {company}: {inserted} new snapshots, {skipped} unchanged')

    print(f'\nDone. {total_inserted} total snapshots written, {total_skipped} trials unchanged.')

    # --- Trial status signals ---
    changes = detect_changes()
    signals = generate_signals(changes)

    print('\n=== TRIAL SIGNALS ===')
    if not signals:
        print('No trial status changes detected.')
    else:
        for s in signals:
            print(s)

    # --- Publication signals ---
    print('\n=== CHECKING PUBLICATIONS ===')
    eligible = _get_nct_ids_for_publication_check()
    print(f'Checking {len(eligible)} trials for new publications...')

    pub_found = 0
    skipped_checks = 0

    for i, (nct_id, company, phase, status) in enumerate(eligible, start=1):

        # Skip if checked within the last 7 days
        if was_recently_checked(nct_id, days=7):
            skipped_checks += 1
            continue

        pub = get_latest_publication(nct_id)
        mark_checked(nct_id)

        if pub and pub.get('pmid'):
            is_new = insert_publication(nct_id, pub)
            if is_new:
                pub_found += 1

        if i % 100 == 0:
            print(f'  [{i}/{len(eligible)}] checked... {pub_found} new publications so far '
                  f'({skipped_checks} skipped as recently checked)')

    print(f'Done. {pub_found} new publications found, {skipped_checks} trials skipped as recently checked.')

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
