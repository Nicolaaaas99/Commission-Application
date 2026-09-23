"""Evolution SDK configuration. Pulled from environment variables (.env)."""
import os
from dotenv import load_dotenv

load_dotenv()

DLL_PATH = os.environ.get('EVO_DLL_PATH')
SERVER = os.environ.get('EVO_SERVER')
COMMON_DATABASE = os.environ.get('EVO_COMMON_DATABASE', 'EvolutionCommon')
COMPANY_DATABASE = os.environ.get('EVO_COMPANY_DATABASE')
USERNAME = os.environ.get('EVO_USERNAME')
PASSWORD = os.environ.get('EVO_PASSWORD')
LICENSE_KEY = os.environ.get('EVO_LICENSE_KEY')
LICENSE_CODE = os.environ.get('EVO_LICENSE_CODE')
AGENT_CODE = os.environ.get('EVO_AGENT_CODE')

COMPANY_DATABASES = {
    1: 'Anderson',
    2: 'NAI_Life',
    3: 'NAI_MedicalAid',
    4: 'NAI_ShortTerm',
}


def get_company_database(company_id):
    """Resolve a company_id to its Evolution database name."""
    db = COMPANY_DATABASES.get(int(company_id))
    if not db:
        raise ValueError(f"No Evolution database configured for company_id {company_id}")
    return db
