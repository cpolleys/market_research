import requests
import json
import time
import pandas as pd
from db import get_conn, safe
from datetime import datetime


def estimate_market_size(condition):
    """
    Uses Claude API to estimate market size for a given condition.
    Returns a dict with patient population, pricing, and rationale.
    """
    response = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={'Content-Type': 'application/json'},
        json={
            'model': 'claude-sonnet-4-20250514',
            'max_tokens': 1000,
            'messages': [{
                'role': 'user',
                'content': f"""For the condition "{condition}", provide a market sizing estimate.
                
Respond ONLY with a JSON object, no preamble or markdown. Format:
{{
    "annual_us_patients": <integer, annual US incidence>,
    "eligible_patient_rate": <float 0-1, fraction likely eligible for a new drug>,
    "estimated_drug_price": <integer, estimated annual cost per patient in USD>,
    "data_source": "<source used to estimate patient population>",
    "rationale": "<2-3 sentence explanation of estimates>"
}}"""
            }]
        }
    )

    data = response.json()
    text = data['content'][0]['text']

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # strip markdown fences if model adds them despite instructions
        clean = text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean)


def run_market_sizing():
    """
    Runs market sizing for all unique conditions in the disease_landscape table
    and stores results in the market_sizing table.
    """
    conn = get_conn()

    conditions = pd.read_sql_query("""
        SELECT DISTINCT condition_searched
        FROM disease_landscape
    """, conn)['condition_searched'].tolist()

    results = []

    for condition in conditions:
        print(f'Sizing market for {condition}...')

        # pull trial context from existing landscape table
        trials = pd.read_sql_query("""
            SELECT COUNT(*) as trial_count,
                   SUM(enrollment) as total_enrollment,
                   COUNT(DISTINCT sponsor) as sponsor_count
            FROM disease_landscape
            WHERE condition_searched = ?
            AND phase IN ('PHASE3', 'PHASE4')
            AND status IN ('RECRUITING', 'ACTIVE_NOT_RECRUITING')
        """, conn, params=[condition]).iloc[0]

        try:
            market = estimate_market_size(condition)

            addressable_market = (
                market['annual_us_patients'] *
                market['eligible_patient_rate'] *
                market['estimated_drug_price']
            )

            result = {
                'condition': condition,
                'annual_us_patients': market['annual_us_patients'],
                'eligible_patient_rate': market['eligible_patient_rate'],
                'estimated_drug_price': market['estimated_drug_price'],
                'addressable_market': addressable_market,
                'trial_count': int(trials['trial_count']),
                'sponsor_count': int(trials['sponsor_count']),
                'total_enrollment': int(trials['total_enrollment'] or 0),
                'data_source': market['data_source'],
                'rationale': market['rationale'],
                'snapshot_date': datetime.utcnow().date().isoformat()
            }

            results.append(result)
            _insert_market_sizing(conn, result)
            print(f'  Addressable market: ${addressable_market:,.0f}')

        except Exception as e:
            print(f'  Failed for {condition}: {e}')

        time.sleep(0.5)

    conn.close()

    if results:
        df = pd.DataFrame(results).sort_values('addressable_market', ascending=False)
        return df
    return pd.DataFrame()


def _insert_market_sizing(conn, result):
    """Insert or replace a market sizing result into the database."""
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO market_sizing (
            condition, annual_us_patients, eligible_patient_rate,
            estimated_drug_price, addressable_market, trial_count,
            sponsor_count, total_enrollment, data_source, rationale,
            snapshot_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        safe(result['condition']),
        result['annual_us_patients'],
        result['eligible_patient_rate'],
        result['estimated_drug_price'],
        result['addressable_market'],
        result['trial_count'],
        result['sponsor_count'],
        result['total_enrollment'],
        safe(result['data_source']),
        safe(result['rationale']),
        safe(result['snapshot_date'])
    ))
    conn.commit()
