import asyncio
import os
import re
import pandas as pd
import json
import logging
import sys
from uuid import uuid4
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
    SHEET_SCHEMA_PATH,
    CACHE_DB
)
from logging import handlers
from customLogger import customLogger
#Read platform - needed for windows-specific steps
IS_WINDOWS = "win" in str(sys.platform).lower()
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
#Generate search run metadata
searchMeta, err = utils.generateRunMetaData()
#Create logger dir, TO-DO: check if error can be handled here
utils.softDirCreate(LOG_FOLDER)
#Configure logger
#TO-DO: make a logging queue
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
utils.logger.info("##########################################################################\n")
utils.logger.info(f"Run ID {searchMeta['run_id']} starts...")
utils.logger.info("Instantiated logger and assigned it to utils instance")
#Read YAML
YAML_CONTENTS = utils.readYaml(SHEET_SCHEMA_PATH)
if YAML_CONTENTS is None:
    pass #TO-DO smth here if yaml is not read
LEAD_SHEET_SCHEMA = YAML_CONTENTS["leadSheetColumns"]

#Prepare to run async steps
if IS_WINDOWS:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
    utils.logger.info("Event policy set, this is a windows-specific step!")
#Init lead manager
manager = LeadManager(cache = CACHE_DB, logger = utils.logger)
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
connector = Connector(rate = RATE, limit = asyncio.Semaphore(LIMIT))
utils.logger.info("Connector instantiated")
#Generate Tasks for asyncio - base search
searchTasks = []
for day in searchDates:
    params = utils.createParams(headerBase = searchParams["params"], day = day)
    paramsCopy = params.copy()
    searchTasks.append(
        connector.makeRequest(
            url = SEARCH_URL,
            requestType = "search",
            logger = utils.logger,
            auth = BasicAuth(REST_KEY, ""),
            params = paramsCopy,
            storage = manager.searchStorage,
            toRetry = manager.toRetryList
        )
    )
utils.logger.info("Prepared search request tasks")
err = manager.processRetryCache(
    retryType = "search",
    taskList = searchTasks,
    connector = connector,
    auth = BasicAuth(REST_KEY, ""),
    dbClean = True
)
if err is not None:
    utils.logger.error(f"Error processing search retries: {err}")
# #Add seacrch retries from cache
# searchRetries = manager.getCachedRetries("search")
# #TO-DO: maybe keep it in a separate function?
# if len(searchRetries) > 0:
#     searchRetries = searchRetries["url"].values
#     for url in searchRetries:
#         searchTasks.append(
#             connector.makeRequest(
#                 url = url,
#                 requestType = "search",
#                 logger = utils.logger,
#                 auth = BasicAuth(REST_KEY, ""),
#                 storage = manager.searchStorage,
#                 toRetry = manager.toRetryList
#             )
#         )
#     manager.deleteRetryEntries(searchRetries)
#     utils.logger.info("Added search entries to retry from cache")
#Make search requests
loop = asyncio.get_event_loop()
loop.run_until_complete(performTasks(searchTasks))
#Log search success
utils.logger.info("Search completed. Saving to cache...")
#Cache results and 429s to later retry
colsToSave, err = sheetReader.getColsToKeep()
manager.cacheSearch(colsToSave, runMetaData = searchMeta)
manager.cacheRetries("search")
#Check what cache results needs to be appended to the sheet
cachedAppend = manager.getCachedToAppend(
    existingIds = sheetReader.leadFrame[LEAD_SHEET_SCHEMA["companyNumber"]].values,
    runMetaData = searchMeta
)
#Clean data before further processing
searchResults, tidyErr = manager.tidySearchResults(cachedAppend)
print(searchResults)
#CALL API for officers
#Generate request tasks
officerTasks, officerTaskUrls = [], []
for companyNumber in searchResults["company_number"]:
    officerUrl = f"{REST_URL}/company/{companyNumber}/officers"
    officerTasks.append(
        connector.makeRequest(
            url = officerUrl,
            requestType = "officers",
            logger = utils.logger,
            auth = BasicAuth(REST_KEY, ""),
            storage = manager.officerStorage,
            toRetry = manager.toRetryList,
            companyNumber = companyNumber
        )
    )
    officerTaskUrls.append(officerUrl)
# Add officer retries
err = manager.processRetryCache(
    retryType = "search",
    taskList = searchTasks,
    connector = connector,
    auth = BasicAuth(REST_KEY, ""),
    dbClean = True,
    taskUrlLog = officerTaskUrls
)
if err is not None:
    utils.logger.error(f"Error processing officer retries: {err}")
#TO-DO: maybe process officer tasks as a separate function?
print(len(officerTasks))
if IS_WINDOWS:
    officerTaskChunks = utils.splitToChunks(officerTasks, 60) if len(officerTasks) > 60 else [officerTasks]
utils.logger.info("Prepared tasks for officer requests")
#TO-DO: maybe put the below to a function
for chunk in officerTaskChunks:
    print("##################NEW CHUNK#####################")
    loop.run_until_complete(performTasks(chunk))
loop.close()
#https://stackoverflow.com/questions/47675410/python-asyncio-aiohttp-valueerror-too-many-file-descriptors-in-select-on-win
#Issue from the above
utils.logger.info("Officer data collected")
# Process officer data to get names only - already WIP in leadManager
# Update handling retries for officer as we need company id there
print(manager.officerStorage)
# Join data with searchResults
#Append to Gsheet: make sure that data to append is not duplicate
#Clean Cache
#Message to discord? Can it be made a part of logging?
#loop cleaning and closing


#TO-DO check if cache has records that are not in sheet manager: append & clean cache, should be done at the beginning!

