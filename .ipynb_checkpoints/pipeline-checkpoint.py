from scraper import fetch_trials, fetch_trials_by_condition
from db import init_db, init_company_table, insert_trials, get_conn, init_landscape_table, insert_landscape_trials
from mappings import resolve_company_sec, fetch_sec_tickers, get_biotech_universe, clean_name
from signals import detect_changes, generate_signals

    
def run():
    init_db()
    init_company_table()
    conn = get_conn()
    
    universe = get_biotech_universe()
    
    company_data = fetch_sec_tickers()
    
    num_companies = len(universe)
    count = 0
    
    for c in universe:
        company = clean_name(c["name"])
        trials = fetch_trials(company)

        for trial in trials:
            sponsor = trial['sponsor']
            ticker = resolve_company_sec(sponsor, company_data)
            trial['company'] = ticker if ticker else sponsor
                
        insert_trials(trials, company)
        
        count += 1
        print(f'Scraped company {count} / {num_companies}')
        
    changes = detect_changes()
    signals = generate_signals(changes)

    print('\n=== SIGNALS ===')
    if not signals:
        print("No changes detected.")
    else:
        for s in signals:
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