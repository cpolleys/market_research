import json
import re
from difflib import get_close_matches
import requests
from datetime import datetime
from db import get_conn
import pandas as pd


SEC_HEADERS = {
    'User-Agent': "Christopher Polleys christopher.polleys@gmail.com"
}

def fetch_sec_tickers():
    url = 'https://www.sec.gov/files/company_tickers.json'
    
    r = requests.get(url, headers=SEC_HEADERS)
    data = r.json()
    
    companies = []

    for _, entry in data.items():
        companies.append({
            'ticker': entry['ticker'],
            'name': entry['title'].lower(),
            'cik': entry['cik_str']
        })

    return companies

def find_ticker_sec(company_name, companies):
    name = company_name.lower()

    names = [c["name"] for c in companies]

    match = get_close_matches(name, names, n=1, cutoff=0.85)

    if match:
        matched_name = match[0]
        for c in companies:
            if c['name'] == matched_name:
                return c['ticker']

    return None

def load_mappings():
    with open('mappings.json') as f:
        return json.load(f)
    
def normalize_name(name):
    if not name:
        return None

    name = name.lower()
    name = re.sub(r'\b(inc|corp|ltd|llc|plc|incorporated)\b\.?', '', name)
    name = re.sub(r'[^\w\s]', '', name)
    
    return name.strip()

def company_map(name, mappings):
    norm = normalize_name(name)
    return mappings.get(norm)

def get_cached_ticker(conn, raw_name):
    cur = conn.cursor()
    cur.execute(
        'SELECT ticker FROM company_map WHERE raw_name = ?',
        (raw_name,)
    )
    row = cur.fetchone()
    return row[0] if row else None

def fuzzy_match(conn, name):
    cur = conn.cursor()
    cur.execute('SELECT raw_name FROM company_map')

    known = [r[0] for r in cur.fetchall()]
    match = get_close_matches(name, known, n=1, cutoff=0.9)

    return match[0] if match else None



def resolve_company_sec(raw_name, companies):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute('SELECT ticker FROM company_map WHERE raw_name = ?', (raw_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    ticker = find_ticker_sec(raw_name, companies)

    cur.execute("""
        INSERT OR REPLACE INTO company_map
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        raw_name,
        raw_name.lower(),
        ticker,
        1.0 if ticker else 0.0,
        'sec',
        datetime.utcnow().isoformat()
    ))

    conn.commit()
    conn.close()
    
    return ticker

def save_sec_data(data):
    with open(SEC_FILE, 'w') as f:
        json.dump(data, f)


def load_sec_data():
    if os.path.exists(SEC_FILE):
        with open(SEC_FILE) as f:
            return json.load(f)
    return None


def get_sec_data():
    data = load_sec_data()

    if data:
        return data

    data = fetch_sec_tickers()
    save_sec_data(data)
    return data

def get_xbi_holdings():
    url = "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"
    df = pd.read_excel(url, skiprows=4)
    print(df)
    return df["Ticker"].dropna().tolist()


def get_ibb_holdings():
    url = "https://www.ishares.com/us/products/239699/ishares-nasdaq-biotechnology-etf/1467271812596.ajax?fileType=csv&fileName=IBB_holdings&dataType=fund"
    df = pd.read_csv(url, skiprows=9)
    return df["Ticker"].dropna().tolist()


