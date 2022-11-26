import yaml
import json
from dotenv import load_dotenv
from os import getenv 
# Read Secrets from ENV variable
load_dotenv()
REST_KEY = getenv("REST_KEY")
GSHEET_SECRET = json.loads(getenv("GSHEET_SECRET"))
# Read main config yaml
_config_path = getenv("CONFIG_PATH")
with open(_config_path, "r") as config_file:
    CONFIG = yaml.safe_load(config_file)
# Store config components for convenience
GSHEET_CONFIG = CONFIG["gsheet"]
API_CONFIG = CONFIG["companies_house"]
REQUEST_TYPES = CONFIG["request_types"]
LOGGER_CONFIG = CONFIG["logger"]
CACHE = CONFIG["cache"]

print(GSHEET_CONFIG)
print(API_CONFIG)
print(REQUEST_TYPES)
print(LOGGER_CONFIG)
print(CACHE)


