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
_configPath = PureWindowsPath(getenv("CONFIG_PATH")) if IS_WINDOWS else getenv("CONFIG_PATH") 
with open(_configPath, "r") as configFile:
    CONFIG = yaml.safe_load(configFile)
# Store config components for convenience
# Gsheet
_gsheetConfig = CONFIG["gsheet"]
LEAD_SHEET_SCHEMA = _gsheetConfig["lead_sheet_schema"]
GSHEET_ID = _gsheetConfig["sheet_id"]
BENCHMARK_SHEETNAMES = _gsheetConfig["benchmark_sheets"].split(",")
GSHEET_CONTROL_PANEL_NAME = _gsheetConfig["tab_names"]["control_panel"]
GSHEET_LEAD_TABLE_NAME = _gsheetConfig["tab_names"]["leads"]
 
_apiConfig = CONFIG["companies_house"]
REST_URL =_apiConfig["base_url"]
SEARCH_URL = _apiConfig["search_url"]
RATE = _apiConfig["rate"]
LIMIT = _apiConfig["limit"]

REQUEST_TYPES = list(CONFIG["request_types"].values())

_loggerConfig = CONFIG["logger"]
LOG_FOLDER = _loggerConfig["folder"]
LOG_FILE_NAME = _loggerConfig["file_name"]
LOG_FORMAT = _loggerConfig["format"]
LOG_DB = _loggerConfig["db"]
LOG_DB_TABLE_NAME = _loggerConfig["db_table"]
TIMEZONE = _loggerConfig["timezone"]

CACHE = CONFIG["cache"]

DISCORD_CONFIG = CONFIG["discord"]
