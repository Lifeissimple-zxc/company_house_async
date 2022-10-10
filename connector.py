import aiohttp
import asyncio

class Connector:
    """
    Object aimed at facilitating the interaction with companies house API
    """
    def __init__(self, rate: int, limit: int):
        self.rate = rate,
        self.limit = limit,
        #Keep lists for now, iterate later
        self.searchStorage = []
        self.companyStorage = []
        self.officerStorage = []

    async def requestCompaniesSearch(self, url, headers, auth):
        """
        Function for performing advances company search.
        Params (headers) are expected to come from OPS Gsheet.
        """
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                async with session.get(url = url, auth = auth, headers = headers) as resp:
                    json = await resp.json()
                    status = resp.status
                    await asyncio.sleep(self.rate)
                    self.searchStorage.append(json)
    
    
    async def requestCompany():
        return
    
    
    async def requestCompanyOfficers():
        return

async def sendRequests(urlList, headerList, auth, rate, limit):
    limit = asyncio.Semaphore(limit)
    conn = Connector(rate = rate, limit = limit)
    tasks = []
    for url, headers in zip(urlList, headerList):
        tasks.append(conn.requestCompaniesSearch(url = url, headers = headers, auth = auth))
    await asyncio.gather(*tasks)
    return conn

##########Rewriting a new object for the sake of test: it works
class Fetch:
    def __init__(self, limit, rate):
        self.limit = limit
        self.rate = rate
        self.searchStorage = []

    async def make_request(self, url, auth, params):
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                async with session.get(url = url, auth = auth, params = params) as resp:
                    data_js = await resp.json(content_type = None) # this works
                    status = resp.status
                    rUrl = resp.url
                    print(f"Status: {status}, URL: {rUrl}")
                    await asyncio.sleep(self.rate)
                    self.searchStorage.append(data_js)
                    #We can do a return here?
async def mainFetch(urlList, paramList, rate, limit, auth):
    limit = asyncio.Semaphore(limit)
    fetcher = Fetch(
        rate = rate,
        limit = limit
    )
    tasks = []

    for url, params in zip(urlList, paramList):
        tasks.append(fetcher.make_request(url = url, auth = auth, params = params))
    results = await asyncio.gather(*tasks)
    return results