import json
import re

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


def map_company(name, mappings):
    norm = normalize_name(name)
    return mappings.get(norm)