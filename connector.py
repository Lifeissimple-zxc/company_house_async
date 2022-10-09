import aiohttp
import asyncio

class Connector:
    """
    Object aimed at facilitating the interaction with companies house API
    """
    def __init__(self, rate: int, limit: asyncio.Semaphore):
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

    async def createRequestQueue(self, requestList):
        await asyncio.gather(*requestList)
