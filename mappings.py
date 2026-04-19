import json
import re
from difflib import get_close_matches
import requests


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

def get_cached_ticker(conn, raw_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT ticker FROM company_map WHERE raw_name = ?",
        (raw_name,)
    )
    row = cur.fetchone()
    return row[0] if row else None

def fuzzy_match(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT raw_name FROM company_map")

    known = [r[0] for r in cur.fetchall()]
    match = get_close_matches(name, known, n=1, cutoff=0.9)

    return match[0] if match else None

def map_company(name, mappings):
    norm = normalize_name(name)
    return mappings.get(norm)