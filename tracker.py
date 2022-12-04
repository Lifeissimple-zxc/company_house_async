import asyncio
import pandas as pd
import logging
from uuid import uuid4
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import BasicAuth
from datetime import datetime as dt
from logging import handlers
from toolBox.asyncUtils import performTasks
from toolBox.leadManager import LeadManager
from toolBox.sheetManager import sheetManager
from toolBox.utils import utilMaster
from toolBox.recordKeeper import (
    dbHandler,
    discordHandler
)
from toolBox import (
    IS_WINDOWS,
    REST_KEY,
    REST_URL,
    SEARCH_URL,
    RATE,
    LIMIT,
    LEAD_SHEET_SCHEMA,
    GSHEET_ID,
    BENCHMARK_SHEETNAMES,
    GSHEET_CONTROL_PANEL_NAME,
    GSHEET_LEAD_TABLE_NAME,
    LOG_FOLDER,
    LOG_FORMAT,
    LOG_DB,
    LOG_DB_TABLE_NAME,
    TIMEZONE,
    CACHE,
    DISCORD_CONFIG,
    REQUEST_TYPES
)
# Instantiate utils
utils = utilMaster()
# Generate search run metadata
searchMeta = utils.generateRunMetaData()
RUN_ID = searchMeta["run_id"]
# Create logger dir 
err = utils.softDirCreate(LOG_FOLDER)
#Configure logger
# Configure our logging handlers
dbLogHandler = dbHandler(db = LOG_DB, table = LOG_DB_TABLE_NAME, runId = RUN_ID)
discordLogHander = discordHandler(
    webhook = DISCORD_CONFIG["webhook"],
    poc1 = DISCORD_CONFIG["poc_1"],
    poc2 = DISCORD_CONFIG["poc_2"],
    runId = RUN_ID
)
#TO-DO: make a logging queue, move to a sep file
logging.basicConfig(
    level = logging.INFO,
    format = LOG_FORMAT,
    handlers = [
        dbLogHandler,
        discordLogHander,
        logging.StreamHandler() #this should write to console?
    ]
)
# Set logger timezone
logging.Formatter.converter = lambda *args: dt.now(tz=timezone(TIMEZONE)).timetuple()
# Perform logger assignment
utils.assignLogger(logging.getLogger("mainLogger"))
utils.logger.info(f"Run ID {searchMeta['run_id']} starts...")
utils.logger.info("Instantiated logger and assigned it to utils instance")

# Prepare to run async steps
if IS_WINDOWS:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
    utils.logger.info("Event policy set, this is a windows-specific step!")
# Init lead manager
manager = LeadManager(
    rate = RATE,
    limit = LIMIT,
    logger = utils.logger,
    sleepTimeBuffer = 2,
    cache = CACHE["db"],
    cacheTable = CACHE["companies_table"],
    retryTable = CACHE["retries_table"],
    allowedRequestTypes = REQUEST_TYPES
)
utils.logger.info("Lead Manager Instantiated")

#Read Spreadsheet Data
sheetReader = sheetManager(
    logger = utils.logger,
    benchmarkSheets = BENCHMARK_SHEETNAMES,
    controlPanelSheetName = GSHEET_CONTROL_PANEL_NAME,
    leadsSheetName = GSHEET_LEAD_TABLE_NAME,
    sheetSecretVarName = "GSHEET_SECRET"
)
utils.logger.info("Sheet Manager Instantiated")
searchParams, e = sheetReader.prepareSeachInputs(
    sheetId = GSHEET_ID,
    workSheetsToRead = [sheetReader.controlPanelSheetName, sheetReader.leadsSheetName]
)
if e is not None:
    utils.logger.error(f"Got and error when preparing search inputs: {e}")
    exit()

utils.logger.info("Search inputs prepared")
leadFrame = getattr(sheetReader, f"{sheetReader.leadsSheetName}Frame")
if len(leadFrame) > 0:
    #Override days back parameter for performing search if lead table has entries
    utils.checkMaxDate(leadFrame, LEAD_SHEET_SCHEMA["dateCreated"], searchParams)
#filter dates for leads, then use getDaysDelta() to compute days_back
searchDates = utils.createSearchDates(searchParams["days_back"])
utils.logger.info("Generated search dates")
#Generate Tasks for asyncio - base search
searchTasks = []
for day in searchDates:
    params = utils.createParams(headerBase = searchParams["params"], day = day)
    paramsCopy = params.copy()
    searchTasks.append(
        manager.makeRequest(
            url = SEARCH_URL,
            requestType = "search",
            auth = BasicAuth(REST_KEY, ""),
            params = paramsCopy,
            storage = manager.searchStorage,
            toRetry = manager.toRetryList
        )
    )
utils.logger.info("Prepared search request tasks")
#Add search urls from cache
err = manager.processRetryCache(
    retryType = "search",
    taskList = searchTasks,
    auth = BasicAuth(REST_KEY, ""),
    dbClean = True
)
if err is not None:
    utils.logger.warning(f"Error processing search retries: {err}")
#Make search requests
loop = asyncio.get_event_loop()
loop.run_until_complete(performTasks(searchTasks))
#Log search success
utils.logger.info("Search completed. Saving to cache...")
#Cache results and 429s to later retry
colsToSave, err = sheetReader.getColsToKeep()
if err is not None:
    utils.logger.error(f"Error getting columns to save: {err}")
    exit()

manager.cacheSearch(colsToSave, runMetaData = searchMeta)
e = manager.cacheRetries("search")
if e is not None:
    utils.logger.warning(f"Erros with caching retries: {e}")

# Check what cache results needs to be appended to the sheet
sheetLeadIds = leadFrame["company_number"].astype(str)
cachedAppend = manager.getCachedToAppend(
    existingIds = sheetLeadIds,
    runMetaData = searchMeta
)

# Clean data before further processing
sheetCompanyNumbers = sheetLeadIds
searchResults, e = manager.tidySearchResults(cacheDf = cachedAppend, sheetCompanyNumbers = sheetCompanyNumbers)
if e is not None:
    utils.logger.error(f"Error processing search results: {e}")
    exit()

# CALL API for officers
# Generate request tasks, url task list is needed to avoid duplication
officerTasks, officerTaskUrls = [], []
for companyNumber in searchResults["company_number"]:
    officerUrl = f"{REST_URL}/company/{companyNumber}/officers"
    officerTasks.append(
        manager.makeRequest(
            url = officerUrl,
            requestType = "officers",
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
    auth = BasicAuth(REST_KEY, ""),
    dbClean = True,
    taskUrlLog = officerTaskUrls
)
if err is not None:
    # We warn that retries were not processed, but we do not stop the execution!
    utils.logger.warning(f"Error processing officer retries: {err}")
#TO-DO: maybe process officer tasks as a separate function?
if IS_WINDOWS:
    # https://stackoverflow.com/questions/47675410/python-asyncio-aiohttp-valueerror-too-many-file-descriptors-in-select-on-win
    # The above is why we split to chunks!
    officerTaskChunks = utils.splitToChunks(officerTasks, 60) if len(officerTasks) > 60 else [officerTasks]
utils.logger.info("Prepared tasks for officer requests")

for chunk in officerTaskChunks:
    loop.run_until_complete(performTasks(chunk))
loop.close()

utils.logger.info("Officer data collected")
officersCleaned = manager.tidyOfficerResults()
mergedData = pd.merge(searchResults, officersCleaned, on = "company_number", how = "left")
#Align column order
mergedData = mergedData[LEAD_SHEET_SCHEMA.values()]
utils.logger.info("Sheet update prepared")
sheetReader.appendToSheet(sheetLeads = sheetLeadIds, df = mergedData)
# Clean cache to avoid exta work during further runs
e = manager.cleanCacheTable()
if e is not None:
    utils.logger.warning(f"Error cleaning cache table: {e}")
# Logging queues
# leadManager refactoring: make cache functions live in a separate object - might not be needed? Maybe better to reorganize?
# loop cleaning and closing


