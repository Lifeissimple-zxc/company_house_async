import asyncio
import os
from utils import createSearchDates
from connector import Connector
from dotenv import load_dotenv
from aiohttp import BasicAuth
from sys import platform
#Read ENV variables
load_dotenv()
REST_URL = os.getenv("REST_BASE_URL")
REST_KEY = os.getenv("REST_KEY")
SEARCH_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
TEST_PARAMS = {'headers': {'company_status': 'active,open', 'sic_codes': '56101,56102,56103'}, 'days_back': 30}
searchDates = createSearchDates(TEST_PARAMS["days_back"])

LIMIT = 10
RATE = 5

limitSemaphore = asyncio.Semaphore(LIMIT)
#Init connector
connector = Connector(
    rate = RATE,
    limit = limitSemaphore
)
requestList = []
for day in searchDates:
    tmpHeader = TEST_PARAMS["headers"]
    tmpHeader["incorporated_from"] = str(day)
    tmpHeader["incorporated_to "] = str(day)
    requestList.append(
        connector.requestCompaniesSearch(url = SEARCH_URL, headers = tmpHeader, auth = BasicAuth(REST_KEY, ""))
    )
#Rewrite the above so that object would be created in a function?
asyncio.run(connector.createRequestQueue(requestList))

print(connector.searchStorage)
