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
