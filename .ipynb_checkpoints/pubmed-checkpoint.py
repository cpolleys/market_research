import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time


EUROPEPMC_URL = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
RATE_LIMIT_SLEEP = 0.1


def _make_session(retries=3, backoff_factor=1.5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods={"GET"},
        raise_on_status=False
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

_SESSION = _make_session()


def get_latest_publication(nct_id):
    """
    Search Europe PMC for the most recent publication mentioning nct_id.
    Returns a dict with pmid, title, journal, pub_date — or None if not found.
    """
    try:
        r = _SESSION.get(
            EUROPEPMC_URL,
            params={
                'query': nct_id,
                'format': 'json',
                'pageSize': 5,
                'sort': 'P_PDATE_D desc'
            },
            timeout=15          # increased from 10 to give retries more room
        )
        r.raise_for_status()
        data = r.json()

        results = data.get('resultList', {}).get('result', [])
        if not results:
            return None

        top = results[0]
        pub_date = top.get('firstPublicationDate') or top.get('pubYear')

        return {
            'pmid':     top.get('pmid'),
            'title':    top.get('title'),
            'journal':  top.get('journalTitle'),
            'pub_date': pub_date
        }

    except requests.exceptions.Timeout:
        print(f'  Europe PMC timeout for {nct_id} (skipping)')
        return None
    except requests.exceptions.HTTPError as e:
        print(f'  Europe PMC HTTP error for {nct_id}: {e} (skipping)')
        return None
    except Exception as e:
        print(f'  Europe PMC error for {nct_id}: {e}')
        return None

    finally:
        time.sleep(RATE_LIMIT_SLEEP)