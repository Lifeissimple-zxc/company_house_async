import asyncio
import os
import re
import pandas as pd
import json
import logging
import sys
from pytz import timezone
from leadManager import LeadManager
from utils import utilMaster
from asyncUtils import performTasks
from connector import Connector
from dotenv import load_dotenv
from aiohttp import BasicAuth
from sheetManager import sheetManager
from datetime import datetime as dt
from constants import (
    SEARCH_URL,
    LIMIT,
    RATE,
    LOG_FORMAT,
    LOG_FILE_NAME,
    LOG_FOLDER,
    TIMEZONE,
    SHEET_SCHEMA_PATH
)
from logging import handlers
from customLogger import customLogger

#Read ENV variables - pass to a separrate function or file?
load_dotenv()
REST_URL = os.getenv("REST_BASE_URL")
REST_KEY = os.getenv("REST_KEY")
GSHEET_SECRET = json.loads(os.getenv("GSHEET_SECRET"))
GSHEET_ID = os.environ.get("GSHEET_ID")
BENCHMARK_SHEETNAMES = os.getenv("BENCHMARK_SHEETNAMES").split(",")
GSHEET_CONTROL_PANEL_NAME = os.getenv("GSHEET_CONTROL_PANEL_NAME")
GSHEET_LEAD_TABLE_NAME = os.getenv("GSHEET_LEAD_TABLE_NAME")
#Instantiate utils class
utils = utilMaster()
#Create logger dir, TO-DO: check if error can be handled here
utils.softDirCreate(LOG_FOLDER)
#Configure logger
logging.basicConfig(
    level = logging.INFO,
    format = LOG_FORMAT,
    handlers = [
        handlers.TimedRotatingFileHandler(
            when = "midnight",
            utc = True,
            filename = os.path.join(LOG_FOLDER, LOG_FILE_NAME)
        ),
        logging.StreamHandler() #this should write to console?
    ]
)
#Set logger timezone
logging.Formatter.converter = lambda *args: dt.now(tz=timezone(TIMEZONE)).timetuple()
#Perform logger assignment
utils.assignLogger(logging.getLogger("mainLogger"))
utils.logger.info("Instantiated logger and assigned it to utils instance")
#Read YAML
YAML_CONTENTS = utils.readYaml(SHEET_SCHEMA_PATH)
if YAML_CONTENTS is None:
    pass #TO-DO smth here if yaml is not read
LEAD_SHEET_SCHEMA = YAML_CONTENTS["leadSheetColumns"]

#Test params - remove?
#  TEST_PARAMS = {'headers': {'company_status': 'active,open', 'sic_codes': '56101,56102,56103'}, 'days_back': 30}
#Prepare to run async steps
if "win" in str(sys.platform).lower():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
    utils.logger.info("Event policy set, this is a windows-specific step!")

#Init lead manager
manager = LeadManager()
utils.logger.info("Lead Manager Instantiated")

#Read Spreadsheet Data
sheetReader = sheetManager(
    logger = utils.logger,
    benchmarkSheets = BENCHMARK_SHEETNAMES,
    controlPanelSheetName = GSHEET_CONTROL_PANEL_NAME,
    leadsSheetName = GSHEET_LEAD_TABLE_NAME
)
utils.logger.info("Sheet Manager Instantiated")

searchParams, prepErr = sheetReader.prepareSeachInputs(sheetId = GSHEET_ID)
if prepErr is not None:
    utils.logger.error(f"Search params preparation error: {prepErr}")

utils.logger.info("Search inputs prepared")

if len(sheetReader.leadFrame) > 0:
    #Override days back parameter for performing search if lead table has entries
    utils.checkMaxDate(sheetReader.leadFrame, LEAD_SHEET_SCHEMA["dateCreated"], searchParams)
#filter dates for leads, then use getDaysDelta() to compute days_back
searchDates = utils.createSearchDates(searchParams["days_back"])
utils.logger.info("Generated search dates")
#init connector
connector = Connector(
    rate = RATE,
    limit = asyncio.Semaphore(LIMIT)
)
utils.logger.info("Connector instantiated")
#Generate Tasks for asyncio
tasks = []
for day in searchDates:
    params = utils.createParams(headerBase = searchParams["params"], day = day)
    paramsCopy = params.copy()
    tasks.append(
        connector.makeRequest(
        url = SEARCH_URL,
        auth = BasicAuth(REST_KEY, ""),
        params = paramsCopy,
        storage = manager.searchStorage
        )
    )

loop = asyncio.get_event_loop()
loop.run_until_complete(performTasks(tasks))
loop.close()
#Storage of the companies needs to be rewritten :()
print("Done with ASYNC")
print("########################################################################################")   
print("Results:")
print(manager.searchStorage)