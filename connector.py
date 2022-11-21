import aiohttp
import asyncio
from logging import Logger
from datetime import datetime

class Connector:
    """
    Class for ratelimiting
    """
    def __init__(self, rate: int, limit: asyncio.Semaphore, logger: Logger, sessionStart: datetime):
        self.rate = rate
        self.limit = limit
        self.logger = logger
        self.sessionStart = sessionStart

    @staticmethod
    def cacheForRetry(url, requestType: str, companyNumber: str = None):
        """
        Function for handling retry caching logic
        """
        assert requestType in ("search", "officers"), "Request type needs to be in (search, officers)"
        #TO-DO: exceptions
        if companyNumber is None:
            companyNumber = ""
        return {
            "url": url,
            "requestType": requestType,
            "companyNumber": companyNumber
        }

    def computeSleepTime(self, sessionStart: datetime, now: datetime, buffer: float = 1.5) -> int:
        """
        Computes how much time has passed since session start to estimate time to sleep to avoind getting 429
        If now < sessionStars, returns rate attrbiute from self
        """
        if now >= sessionStart:
            return self.rate - (now - sessionStart).seconds + buffer
        else:
            self.logger.warning("Now is less than session start, this is unexpected.")
            return self.rate
            

    async def makeRequest(
        self,
        requestType: str,
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
                print(self.limit._value)
                resp = await session.get(url = url, auth = auth, params = params)
                dataJson = await resp.json(content_type = None)
                rUrl = resp.url

                if self.limit.locked():
                    #Make it a function?
                    sleepTime = self.computeSleepTime(
                        sessionStart = self.sessionStart,
                        now = datetime.utcnow()
                        )
                    self.logger.warning(f"Hit RPS limit, sleeping for {sleepTime} seconds")
                    await asyncio.sleep(sleepTime)

                if (status := resp.status) != 200:
                    # If we still get 429
                    if status == 429:
                        sleepTime = self.computeSleepTime(
                            sessionStart = self.sessionStart,
                            now = datetime.utcnow()
                        )
                        self.logger.warning(f"Got {status} for {rUrl}, sleeping for {sleepTime} to retry")
                        await asyncio.sleep(sleepTime)
                        #Retry on the spot
                        retry = await session.get(
                            url = rUrl, auth = auth
                        )
                        if (retryStatus := retry.status) == 200:
                            self.logger.info(f"Successful retry for {rUrl}")
                            dataJson  = await retry.json(content_type = None)
                        else:
                            self.logger.warning(f"Got {retryStatus} for {rUrl} after retry")
                            if retryStatus == 429:
                                self.logger.warning(f"Saving {rUrl} to retry cache")
                                self.cacheForRetry(
                                    url = rUrl,
                                    requestType = requestType,
                                    companyNumber = companyNumber
                                )
                #Check if data JSON is none - log this
                if dataJson is not None:
                    self.logger.info(f"Saving valid response from {rUrl}")
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
