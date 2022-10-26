import asyncio
import os
import re
import pandas as pd
import json
import logging
import sys
from pytz import timezone
from leadManager import LeadManager
from utils import createSearchDates, createParams, getDaysDelta, softDirCreate
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
    TIMEZONE
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
#Create logger dir, TO-DO: check if error can be handled here
softDirCreate(LOG_FOLDER)
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
recordKeeper = customLogger(logging.getLogger())
recordKeeper.logger.info("Logger instantiated")
#Test params - remove?
#  TEST_PARAMS = {'headers': {'company_status': 'active,open', 'sic_codes': '56101,56102,56103'}, 'days_back': 30}
#Preapre to run async programm - move to a separate function?
if "win" in str(sys.platform).lower():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
    recordKeeper.logger.info("Event policy set, this is a windows-specific step!")

#Init lead manager
manager = LeadManager()
recordKeeper.logger.info("Sheet Manager Instantiated")
#Read Spreadsheet Data
sheetReader = sheetManager(
    logger = recordKeeper,
    benchmarkSheets = BENCHMARK_SHEETNAMES,
    controlPanelSheetName = GSHEET_CONTROL_PANEL_NAME,
    leadsSheetName = GSHEET_LEAD_TABLE_NAME
)
#TO-DO: Steps here needs to be logged
sheetReader.connect()
sheetReader.openSheet(GSHEET_ID)
sheetReader.validateSheet()
sheetReader.readControlPanel()
countLeadsSheet = sheetReader.readLeadsTable()
searchParams = sheetReader.parseSearchParams()
recordKeeper.logger.info("Spreadsheet data read & parsed!")
exit()
#TO-DO: move this to a separate function?
if countLeadsSheet > 0:
    maxLeadCreatedStr = str(max(pd.to_datetime(sheetReader.sheetLeadsFrame["date_of_creation"])))[:10]
    maxLeadCreatedDate = dt.strptime(maxLeadCreatedStr, "%Y-%m-%d").date()
    #TO-DO: LOG step of adjusting days_back_parameter
    daysFromLastLead = getDaysDelta(lastLeadDate = maxLeadCreatedDate)
    searchParams["days_back"] = daysFromLastLead
    del maxLeadCreatedStr, maxLeadCreatedDate
#filter dates for leads, then use getDaysDelta() to compute days_back
searchDates = createSearchDates(searchParams["days_back"])
#init connector
connector = Connector(
    rate = RATE,
    limit = asyncio.Semaphore(LIMIT)
)
#Generate Tasks for asyncio
tasks = []
for day in searchDates:
    params = createParams(headerBase = searchParams["headers"], day = day)
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