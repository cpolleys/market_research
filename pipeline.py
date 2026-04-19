from scraper import fetch_trials
from db import init_db, insert_trials
from mappings import load_mappings
from signals import detect_changes, generate_signals

COMPANIES = [
    'Moderna',
    'Pfizer',
    'Regeneron'
]

def run():
    init_db()
    mappings = load_mappings()

    for company in COMPANIES:
        trials = fetch_trials(company)
        insert_trials(trials, company)

    changes = detect_changes()
    signals = generate_signals(changes)

    print('\n=== SIGNALS ===')
    for s in signals:
        print(s)


if __name__ == '__main__':
    run()