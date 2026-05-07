from scraper import fetch_trials
from db import init_db, init_company_table, insert_trials, get_conn
from mappings import resolve_company_sec, fetch_sec_tickers, get_biotech_universe, clean_name
from signals import detect_changes, generate_signals

    
def run():
    init_db()
    init_company_table()
    conn = get_conn()
    
    #universe = get_biotech_universe()
    universe = {'Moderna', 'Pfizer', 'Regeneron'}
    
    company_data = fetch_sec_tickers()
    
    num_companies = len(universe)
    count = 0
    
    for company in universe:
    #for c in universe:
        #company = clean_name(c["name"])
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

if __name__ == '__main__':
    run()