import json
import re
import os
from difflib import get_close_matches
import requests
from datetime import datetime
from db import get_conn
import pandas as pd
from io import StringIO


SEC_HEADERS = {
    'User-Agent': "Christopher Polleys christopher.polleys@gmail.com"
}

# SIC codes covering biotech, pharma, and medtech
SIC_CODES = [
    '2836',  # Pharmaceutical preparations
    '2835',  # Diagnostic substances
    '2830',  # Drugs (broad)
    '8731',  # Biotech R&D
    '3841',  # Surgical & medical instruments
    '3845',  # Electromedical equipment
    '3826',  # Laboratory analytical instruments
    '3827',  # Optical instruments
]


def fetch_sec_tickers():
    """Fetch all public company tickers from SEC. Used for sponsor -> ticker resolution."""
    url = 'https://www.sec.gov/files/company_tickers.json'
    r = requests.get(url, headers=SEC_HEADERS, timeout=15)
    data = r.json()

    companies = []
    for _, entry in data.items():
        companies.append({
            'ticker': entry['ticker'],
            'name': entry['title'].lower(),
            'cik': entry['cik_str']
        })

    return companies

def _fetch_ciks_for_sic(sic):
    ciks = set()
    start = 0

    while True:
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar"
            f"?action=getcompany&SIC={sic}&type=10-K&dateb=&owner=include"
            f"&count=100&start={start}&output=atom"
        )
        try:
            r = requests.get(url, headers=SEC_HEADERS, timeout=15)
            found = re.findall(r'<cik>(.*?)</cik>', r.text)

            if not found:
                break

            ciks.update(c.strip().zfill(10) for c in found)

            if len(found) < 100:
                break

            start += 100

        except Exception as e:
            print(f'Warning: failed fetching SIC {sic} at start={start}: {e}')
            break

    return ciks


def get_sec_universe():
    all_ciks = set()
    for sic in SIC_CODES:
        ciks = _fetch_ciks_for_sic(sic)
        all_ciks.update(ciks)
        print(f'SIC {sic}: {len(ciks)} companies')

    print(f'Total unique CIKs: {len(all_ciks)}')

    ticker_data = fetch_sec_tickers()
    cik_to_entry = {
        str(c['cik']).zfill(10): c
        for c in ticker_data
    }

    results = []
    for cik in all_ciks:
        entry = cik_to_entry.get(cik)
        if entry and is_valid_ticker(entry['ticker']):
            results.append({
                'ticker': entry['ticker'],
                'name': clean_name(entry['name'])
            })

    print(f'SEC universe: {len(results)} companies with valid tickers')
    return results


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
        conn.close()
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
    return t.isalpha() and 1 <= len(t) <= 5 and not t.endswith(('W', 'U', 'R'))


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


excluded_tickers = {"XTSLA", "SGAFT", "USD"}


def get_biotech_universe():
    """
    Build the full universe from SEC SIC codes plus XBI holdings,
    deduplicated by ticker.
    """
    sec = get_sec_universe()
    xbi = get_xbi_holdings()

    combined = sec + xbi

    seen = {}
    for c in combined:
        ticker = c.get('ticker', '').upper()
        if not ticker or ticker in excluded_tickers:
            continue
        seen[ticker] = c['name']

    return [{"ticker": t, "name": n} for t, n in seen.items()]


def clean_name(name):
    suffixes = [
        "inc", "corp", "ltd", "plc", "co", ",", "adr", "class a", "nv", "ag",
        "clas", "group i", "sa", "series a", "cvr", "representing", "represent",
        "holdings", "strategies", "se", "interna", "american", "depositary shares",
        "rep", "lt", "one non-v", "n v", "sponsored", "in", "ads", "class", "holding"
    ]

    name = name.lower().strip()
    while True:
        original = name
        for s in suffixes:
            name = re.sub(rf'\b{s}\b\.?$', '', name).strip()

        name = re.sub(r'[,\.\-]+$', '', name).strip()
        if name == original:
            break

    return name.strip()
