import json
import re
from difflib import get_close_matches
import requests
from datetime import datetime
from db import get_conn
import pandas as pd
from io import StringIO


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

def keep_table(df):
    header_idx = df.astype(str).apply(
        lambda row: row.str.contains("Currency", case=False).any(),
        axis=1
    ).idxmax()
    df.columns = df.loc[header_idx]
    df = df.loc[header_idx + 1:].reset_index(drop=True)
    df = df[
        df.astype(str).apply(
            lambda row: row.str.contains("USD", case=False).any(),
            axis=1
        )
    ].reset_index(drop=True)
    
    return df

def is_valid_ticker(t):
    return t.isalpha() and 1 <= len(t) <= 5
    

def get_xbi_holdings():
    url = "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"
    df = pd.read_excel(url)
    keep_df = keep_table(df)
    
    tickers = keep_df["Ticker"].astype(str).str.strip().str.upper()
    names = keep_df["Name"].astype(str).str.strip()
    
    records = [
        {"ticker": t, "name": n}
        for t, n in zip(tickers, names)
        if is_valid_ticker(t)
    ]
    
    return records


def get_ibb_holdings():
    url = "https://www.ishares.com/us/products/239699/ishares-nasdaq-biotechnology-etf/1467271812596.ajax?fileType=csv&fileName=IBB_holdings&dataType=fund"
    text = requests.get(url).text
    lines = text.splitlines()
    header_idx = next(i for i, line in enumerate(lines) if "Ticker" in line)
    df = pd.read_csv(StringIO("\n".join(lines[header_idx:])))
    keep_df = df[
        df.astype(str).apply(
            lambda row: row.str.contains("USD", case=False).any(),
            axis=1
        )
    ].reset_index(drop=True)
    
    tickers = keep_df["Ticker"].astype(str).str.strip().str.upper()
    names = keep_df["Name"].astype(str).str.strip()

    records = []

    records = [
        {"ticker": t, "name": n}
        for t, n in zip(tickers, names)
        if is_valid_ticker(t)
    ]

    return records

def get_biotech_universe():
    xbi = get_xbi_holdings()
    ibb = get_ibb_holdings()

    combined = xbi + ibb
    
    seen = {}
    for c in combined:
        seen[c["ticker"]] = c["name"]

    return [{"ticker": t, "name": n} for t, n in seen.items()]

def clean_name(name):
    suffixes = ["inc", "corp", "ltd", "plc", "co", ",", "adr", "class a", "nv", "ag", "clas", "group i", "sa", "series a", "cvr", "representing", "represent", "holdings", ]

    name = name.lower()
    for s in suffixes:
        name = name.replace(s, "")

    return name.strip()


