from scraper import fetch_trials
from db import init_db, init_company_table, insert_trials, get_conn
from mappings import resolve_company_sec, fetch_sec_tickers
from signals import detect_changes, generate_signals

companies = [
    'Moderna',
    'Pfizer',
    'Regeneron'
]

def run():
    init_db()
    init_company_table()
    conn = get_conn()
    
    company_data = fetch_sec_tickers()
    
    for company in companies:
        trials = fetch_trials(company)

        for trial in trials:
            sponsor = trial['sponsor']
            ticker = resolve_company_sec(sponsor, company_data)


            for company in companies:
                trials = fetch_trials(company)
                insert_trials(trials, company)
                trial['company'] = ticker if ticker else sponsor
                
        insert_trials(trials, company)
        
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