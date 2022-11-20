import aiohttp
import asyncio
from logging import Logger

class Connector:
    """
    Class for ratelimiting
    """
    def __init__(self, rate: int, limit: asyncio.Semaphore):
        self.rate = rate
        self.limit = limit

    async def makeRequest(
        self,
        requestType: str,
        logger: Logger,
        url: str,
        auth: aiohttp.BasicAuth,
        storage: list,
        toRetry: list,
        params: dict = None,
        companyNumber: str = None) -> None:
        """
        Async function for making http requests to company house API
        """
        #TO-DO: error handling?
        if requestType not in ("search", "officers"):
            raise Exception(f"Wrong input for request task: {requestType}. Expected ('search', 'officers')")
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url = url, auth = auth, params = params)
                dataJson = await resp.json(content_type = None)
                rUrl = resp.url
                if (status := resp.status) != 200:
                    logger.warning(f"Got {status} for {rUrl}, sleeping for {self.rate} to retry")
                    #Handle 429 separately
                    if status == 429:
                        await asyncio.sleep(self.rate)
                        #Retry on the spot
                        retry = await session.get(
                            url = rUrl, auth = auth
                        )
                        if (retryStatus := retry.status) == 200:
                            logger.info(f"Successful retry for {rUrl}")
                            dataJson  = await retry.json(content_type = None)
                        else:
                            logger.warning(f"Got {retryStatus} for {rUrl} after retry")
                            if retryStatus == 429:
                                logger.warning(f"Saving {rUrl} to retry cache")
                                toRetry.append(rUrl)
                #Check if data JSON is none - log this
                if dataJson is not None:
                    logger.info(f"Saving valid response from {rUrl}...")
                    if requestType == "officers":
                        storage.append({
                            "companyNumber": companyNumber,
                            "data": dataJson.get("items", None)
                        })
                    else:
                        storage += dataJson.get("items", None)

#TO-DO: can this be implemented differently?
async def makeRequests(urlList, rate, limit, auth, storage, paramList = None):
    """
    Async function for making requests in bulk while controlling rate limits
    """
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
