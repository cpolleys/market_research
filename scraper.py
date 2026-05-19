import requests
from datetime import datetime
import calendar
import time

base_url = 'https://clinicaltrials.gov/api/v2/studies'


def normalize_date(date_str):
    """
    Normalize a date string to YYYY-MM-DD.
    - YYYY-MM-DD: returned as-is
    - YYYY-MM: last day of that month is imputed
    - YYYY: December 31 of that year is imputed
    - None or unrecognized: returned as None
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # Already full date
    if len(date_str) == 10:
        return date_str

    # Year-month only: impute last day of month
    if len(date_str) == 7:
        try:
            year, month = int(date_str[:4]), int(date_str[5:7])
            last_day = calendar.monthrange(year, month)[1]
            return f'{year:04d}-{month:02d}-{last_day:02d}'
        except ValueError:
            return None

    # Year only: impute Dec 31
    if len(date_str) == 4:
        try:
            return f'{int(date_str):04d}-12-31'
        except ValueError:
            return None

    return None


def _parse_study(info):
    """Parse a single study's protocolSection into a trial dict."""
    phase = info.get('designModule', {}).get('phases')
    if isinstance(phase, list):
        phase = ','.join(phase)

    conditions = info.get('conditionsModule', {}).get('conditions')
    if isinstance(conditions, list):
        conditions = ','.join(conditions)

    interventions_raw = info.get('armsInterventionsModule', {}).get('interventions', [])
    interventions = None
    if interventions_raw:
        interventions = ','.join([i.get('name', '') for i in interventions_raw if i.get('name')])

    outcomes_module = info.get('outcomesModule', {})

    primary_raw = outcomes_module.get('primaryOutcomes', [])
    primary_outcomes = None
    if primary_raw:
        primary_outcomes = ' | '.join([o.get('measure', '') for o in primary_raw if o.get('measure')])

    secondary_raw = outcomes_module.get('secondaryOutcomes', [])
    secondary_outcomes = None
    if secondary_raw:
        secondary_outcomes = ' | '.join([o.get('measure', '') for o in secondary_raw if o.get('measure')])

    fda_flag = info.get('oversightModule', {}).get('isFdaRegulatedDrug')
    if fda_flag is True:
        fda_flag = 1
    elif fda_flag is False:
        fda_flag = 0
    else:
        fda_flag = None

    raw_pcd = info.get('statusModule', {}).get('primaryCompletionDateStruct', {}).get('date')

    return {
        'nct_id': info.get('identificationModule', {}).get('nctId'),
        'snapshot_date': datetime.utcnow().date().isoformat(),
        'title': info.get('identificationModule', {}).get('briefTitle'),
        'phase': phase,
        'fda_regulated': fda_flag,
        'status': info.get('statusModule', {}).get('overallStatus'),
        'sponsor': info.get('sponsorCollaboratorsModule', {}).get('leadSponsor', {}).get('name'),
        'study_type': info.get('designModule', {}).get('studyType'),
        'conditions': conditions,
        'interventions': interventions,
        'enrollment': info.get('designModule', {}).get('enrollmentInfo', {}).get('count'),
        'start_date': normalize_date(info.get('statusModule', {}).get('startDateStruct', {}).get('date')),
        'primary_completion_date': normalize_date(raw_pcd),
        'primary_outcomes': primary_outcomes,
        'secondary_outcomes': secondary_outcomes,
        'last_updated': normalize_date(info.get('statusModule', {}).get('lastUpdatePostDateStruct', {}).get('date'))
    }


def fetch_trials(company_name):
    trials = []
    next_token = None

    while True:
        params = {'query.spons': company_name, 'pageSize': 1000, 'format': 'json'}
        if next_token:
            params['pageToken'] = next_token

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        for study in data.get('studies', []):
            info = study.get('protocolSection', {})
            trials.append(_parse_study(info))

        next_token = data.get('nextPageToken')
        if not next_token:
            break
        time.sleep(0.5)

    return trials


def fetch_trials_by_condition(condition):
    trials = []
    next_token = None

    while True:
        params = {
            'query.cond': condition,
            'query.intr': 'drug',
            'aggFilters': 'phase:3 4',
            'pageSize': 1000,
            'format': 'json'
        }
        if next_token:
            params['pageToken'] = next_token

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        for study in data.get('studies', []):
            info = study.get('protocolSection', {})
            trials.append(_parse_study(info))

        next_token = data.get('nextPageToken')
        if not next_token:
            break
        time.sleep(0.5)

    return trials
