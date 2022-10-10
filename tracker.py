import asyncio
import os
from utils import createSearchDates
from connector import Connector, sendRequests, mainFetch
from dotenv import load_dotenv
from aiohttp import BasicAuth
from sys import platform
#Read ENV variables
load_dotenv()
REST_URL = os.getenv("REST_BASE_URL")
REST_KEY = os.getenv("REST_KEY")
SEARCH_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
TEST_PARAMS = {'headers': {'company_status': 'active,open', 'sic_codes': '56101,56102,56103'}, 'days_back': 1}
searchDates = createSearchDates(TEST_PARAMS["days_back"])

LIMIT = 10
RATE = 5

#Init connector
connector = Connector(
    rate = RATE,
    limit = LIMIT
)

paramList = []
for day in searchDates:
    tmpHeader = TEST_PARAMS["headers"]
    tmpHeader["incorporated_from"] = str(day)
    tmpHeader["incorporated_to "] = str(day)
    paramList.append(tmpHeader)
urlList = [SEARCH_URL] * len(paramList)
#Rewrite the above so that object would be created in a function?
# asyncio.run(
#     sendRequests(
#         urlList = urlList, headerList = headerList,
#         rate = RATE, limit = LIMIT, auth = BasicAuth(REST_KEY, "")
#         )
# )
#Run async programm
if "win" in str(platform).lower():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) #windows-specific thing!
asyncio.run(
    mainFetch(
        urlList = urlList, paramList = paramList,
        rate = RATE, limit = LIMIT, auth = BasicAuth(REST_KEY, "")
    )
)

# print(connector.searchStorage)