"""Constants used in the brewery ETL process."""

# Folders constants
DLH_PATH = '/opt/airflow/data/dlh'
LANDING_PATH = '/opt/airflow/data/brewery-landing'
BRONZE_PATH = f'{DLH_PATH}/01-bronze'
SILVER_PATH = f'{DLH_PATH}/02-silver'
GOLD_PATH = f'{DLH_PATH}/03-gold'
QUARANTINE_PATH = f'{DLH_PATH}/99-quarantine'

# API constants
API_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
API_PER_PAGE_LIMIT = 200
API_TIMEOUT = 120

# ETL constants
KEY_FIELDS = ['id', 'brewery_type', 'state', 'city', 'country']
STRING_COLUMNS = ['id', 'brewery_type', 'state', 'city', 'country']
STANDARD_BREWERY_TYPES = {
    'micro': 'micro',
    'nano': 'nano',
    'regional': 'regional',
    'brewpub': 'brewpub',
    'large': 'large',
    'planning': 'planning',
    'bar': 'bar',
    'contract': 'contract',
    'proprietor': 'proprietor',
    'closed': 'closed'
}  #I know that this doesn't make sense, I kept as an example of transformation!