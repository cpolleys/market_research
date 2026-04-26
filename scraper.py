import requests
from datetime import datetime

base_url = 'https://clinicaltrials.gov/api/v2/studies'

def fetch_trials(company_name):
    params = {
        "query.term": company_name,
        "pageSize": 100,
        "format": "json"
    }
    
    response = requests.get(base_url, params=params)
    data = response.json()
    
    trials = []
    
    for study in data.get('studies', []):
        info = study.get('protocolSection', {})
        
        phase = info.get('designModule', {}).get('phases')
        if isinstance(phase, list):
            phase = ",".join(phase)
            
        conditions = info.get('conditionsModule', {}).get('conditions')
        if isinstance(conditions, list):
            conditions = ",".join(conditions)
            
        interventions_raw = info.get('armsInterventionsModule', {}).get('interventions', [])
        interventions = None
        if interventions_raw:
            interventions = ",".join([i.get('name', '') for i in interventions_raw if i.get('name')])
            
        outcomes_module = info.get('outcomesModule', {})
       
        primary_raw = outcomes_module.get('primaryOutcomes', [])
        primary_outcomes = None
        if primary_raw:
            primary_outcomes = " | ".join([
                o.get("measure", "") for o in primary_raw if o.get("measure")
            ])
        
        secondary_raw = outcomes_module.get('secondaryOutcomes', [])
        secondary_outcomes = None
        if secondary_raw:
            secondary_outcomes = " | ".join([
                o.get("measure", "") for o in secondary_raw if o.get("measure")
            ])
            
        fda_flag = info.get("oversightModule", {}).get("isFdaRegulatedDrug")
        if fda_flag is True:
            fda_flag = 1
        elif fda_flag is False:
            fda_flag = 0
        else:
            fda_flag = None

        
        trials.append({
            'nct_id': info.get('identificationModule', {}).get('nctId'),
            'title': info.get('identificationModule', {}).get('briefTitle'),
            'phase': phase,
            'fda_regulated': fda_flag,
            'status': info.get('statusModule', {}).get('overallStatus'),
            'sponsor': info.get('sponsorCollaboratorsModule', {}).get('leadSponsor', {}).get('name'),
            'study_type': info.get('designModule', {}).get('studyType'),
            'conditions': conditions,
            'interventions': interventions,
            'enrollment': info.get('designModule', {}).get('enrollmentInfo', {}).get('count'),
            'start_date': info.get('statusModule', {}).get('startDateStruct', {}).get('date'),
            'primary_completion_date': info.get('statusModule', {}).get('primaryCompletionDateStruct', {}).get('date'),
            'primary_outcomes': primary_outcomes,
            'secondary_outcomes': secondary_outcomes,
            'last_updated': info.get('statusModule', {}).get('lastUpdatePostDateStruct', {}).get('date'),
            'snapshot_date': datetime.utcnow().isoformat()
        })
    
    return trials