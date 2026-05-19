import requests
import time


EUROPEPMC_URL = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'

RATE_LIMIT_SLEEP = 0.2


def get_latest_publication(nct_id):
    """
    Search Europe PMC for the most recent publication mentioning nct_id.
    Returns a dict with pmid, title, journal, pub_date — or None if not found.
    """
    try:
        r = requests.get(
            EUROPEPMC_URL,
            params={
                'query': nct_id,
                'format': 'json',
                'pageSize': 5,
                'sort': 'P_PDATE_D desc'
            },
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
 
        results = data.get('resultList', {}).get('result', [])
        if not results:
            return None
 
        # Most recent result is first due to sort
        top = results[0]
 
        # Build a full date from available fields
        pub_date = top.get('firstPublicationDate') or top.get('pubYear')
 
        return {
            'pmid':    top.get('pmid'),
            'title':   top.get('title'),
            'journal': top.get('journalTitle'),
            'pub_date': pub_date
        }
 
    except Exception as e:
        print(f'  Europe PMC error for {nct_id}: {e}')
        return None
 
    finally:
        time.sleep(RATE_LIMIT_SLEEP)
