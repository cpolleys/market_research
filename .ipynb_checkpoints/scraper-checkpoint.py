import requests
from datetime import datetime

base_url = 'https://clinicaltrials.gov/api/v2/studies'

def fetch_trials(company_name):
    params = {
        "query.term": company,
        "pageSize": 100,
        "format": "json"
    }
    
    response = requests.get(base_url, params=params)
    data = response.json()
    
    trials = []
    
    for study in data.get('studies', []):
        info = study.get('protocolSection', {})
        
        trials.append({
            'nct_id': info.get('identificationModule', {}).get('nctId'),
            'title': info.get('identificationModule', {}).get('briefTitle'),
            'phase': info.get('designModule', {}).get('phases'),
            'status': info.get('statusModule', {}).get('overallStatus'),
            'sponsor': info.get('sponsorCollaboratorsModule', {}).get('leadSponsor', {}).get('name'),
            'last_updated': ps.get('statusModule', {}).get('lastUpdatePostDateStruct', {}).get('date'),
            'snapshot_date': datetime.utcnow().isoformat()
        })
    
    return trials