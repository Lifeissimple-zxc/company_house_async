import aiohttp
import asyncio
from attr import define

@define
class Connector:
    rate: int
    limit: asyncio.Semaphore

    async def makeRequest(self, url, auth, params, storage: list):
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                async with session.get(url = url, auth = auth, params = params) as resp:
                    await asyncio.sleep(self.rate)
                    dataJson  = await resp.json(content_type = None)
                    status = resp.status
                    rUrl = resp.url
                    #Check if data JSON is none - log this
                    if dataJson is not None:
                        storage += dataJson["items"]
                    #Log request result
                    print(f"Status: {status}, URL: {rUrl}")
                    #TO-DO: retries if we hit 429? Maybe store to cache and use a separate function to query those?

async def makeRequests(urlList, rate, limit, auth, storage, paramList = None):
    limit = asyncio.Semaphore(limit)
    fetcher = Connector(
        rate = rate,
        limit = limit
    )

    tasks = []

    if paramList is not None:
        for url, params in zip(urlList, paramList):
            tasks.append(fetcher.makeRequest(url = url, auth = auth, params = params, storage = storage))
    else:
        for url in urlList:
            tasks.append(fetcher.makeRequest(url = url, auth = auth, params = None, storage = storage))
    
    await asyncio.gather(*tasks)

    return True