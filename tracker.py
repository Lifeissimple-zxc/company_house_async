import asyncio
import os
import pandas as pd
import json
from leadManager import LeadManager
from utils import createSearchDates, createParams, getDaysDelta, performTasks
from connector import Connector
from dotenv import load_dotenv
from aiohttp import BasicAuth
from sys import platform
from sheetManager import sheetManager
from datetime import datetime as dt
from constants import SEARCH_URL, LIMIT, RATE
#Read ENV variables
load_dotenv()
REST_URL = os.getenv("REST_BASE_URL")
REST_KEY = os.getenv("REST_KEY")
GSHEET_SECRET = os.getenv("GSHEET_SECRET")
GSHEET_ID = os.environ.get("GSHEET_ID")
BENCHMARK_SHEETNAMES = os.getenv("BENCHMARK_SHEETNAMES")
GSHEET_CONTROL_PANEL_NAME = os.getenv("GSHEET_CONTROL_PANEL_NAME")
GSHEET_LEAD_TABLE_NAME = os.getenv("GSHEET_LEAD_TABLE_NAME")

#Read secrets from .env strings
sheetSecrets = json.loads(GSHEET_SECRET)
benchmarkSheets = BENCHMARK_SHEETNAMES.split(",")
#Test params
TEST_PARAMS = {'headers': {'company_status': 'active,open', 'sic_codes': '56101,56102,56103'}, 'days_back': 30}
#Preapre to run async programm - move to a separate function?
if "win" in str(platform).lower():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
#Init lead manager
manager = LeadManager()

#Read Spreadsheet Data
sheetReader = sheetManager(
    sheetId = GSHEET_ID,
    sheetSecrets = sheetSecrets,
    benchmarkSheets = benchmarkSheets,
    controlPanelSheetName = GSHEET_CONTROL_PANEL_NAME,
    leadsSheetName = GSHEET_LEAD_TABLE_NAME
)
#TO-DO: Steps here needs to be logged
sheetReader.openSheet()
sheetReader.validateSheet()
sheetReader.readControlPanel()
countLeadsSheet = sheetReader.readLeadsTable()
searchParams = sheetReader.parseSearchParams()
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