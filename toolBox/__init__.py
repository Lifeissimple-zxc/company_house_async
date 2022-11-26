import yaml
import json
from dotenv import load_dotenv
from os import getenv
from sys import platform
from pathlib import PureWindowsPath
#Read platform - needed for windows-specific steps
IS_WINDOWS = "win" in str(platform).lower()
# Read Secrets from ENV variable
load_dotenv()
REST_KEY = getenv("REST_KEY")
GSHEET_SECRET = json.loads(getenv("GSHEET_SECRET"))
# Read main config yaml
_config_path = PureWindowsPath(getenv("CONFIG_PATH")) if IS_WINDOWS else getenv("CONFIG_PATH") 
with open(_config_path, "r") as config_file:
    CONFIG = yaml.safe_load(config_file)
# Store config components for convenience
# Gsheet
_gsheet_config = CONFIG["gsheet"]
LEAD_SHEET_SCHEMA = _gsheet_config["lead_sheet_schema"]
GSHEET_ID = _gsheet_config["sheet_id"]
BENCHMARK_SHEETNAMES = _gsheet_config["benchmark_sheets"].split(",")
GSHEET_CONTROL_PANEL_NAME = _gsheet_config["tab_names"]["control_panel"]
GSHEET_LEAD_TABLE_NAME = _gsheet_config["tab_names"]["leads"]
 
_api_config = CONFIG["companies_house"]
REST_URL =_api_config["base_url"]
SEARCH_URL = _api_config["search_url"]
RATE = _api_config["rate"]
LIMIT = _api_config["limit"]

REQUEST_TYPES = CONFIG["request_types"]

_loggerConfig = CONFIG["logger"]
LOG_FOLDER = _loggerConfig["folder"]
LOG_FILE_NAME = _loggerConfig["file_name"]
LOG_FORMAT = _loggerConfig["format"]
TIMEZONE = _loggerConfig["timezone"]

CACHE = CONFIG["cache"]
