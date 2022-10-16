import asyncio
import os
import pandas as pd
import json
from leadManager import LeadManager
from utils import createSearchDates, createParams, getDaysDelta
from connector import makeRequests
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
paramList = []
for day in searchDates:
    tmp = createParams(headerBase = searchParams["headers"], day = day)
    tmp_copy = tmp.copy()
    paramList.append(tmp_copy)
    del tmp, tmp_copy
urlList =  [SEARCH_URL] * len(paramList)

#Run async programm
if "win" in str(platform).lower():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!

asyncio.run(
    makeRequests(
        urlList = urlList,
        paramList = paramList,
        rate = RATE,
        limit = LIMIT,
        auth = BasicAuth(REST_KEY, ""),
        storage = manager.searchStorage
    )
)
#Storage of the companies needs to be rewritten :()
print("Done with ASYNC")
print("########################################################################################")   
print("Results:")