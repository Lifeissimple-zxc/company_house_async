import aiohttp
import asyncio
from logging import Logger
from datetime import datetime

class Connector:
    """
    Class for ratelimiting
    """
    def __init__(self, rate: int, limit: int, sleepTimeBuffer: int, logger: Logger):
        self.rate = rate
        self.limit = limit
        self.baseLimit = limit
        self.logger = logger
        # Timing variable for rate limiting
        self.checkpoint = datetime.utcnow()
        # Buffer for rate limiting
        self.sleepTimeBuffer = sleepTimeBuffer

    @staticmethod
    def cacheForRetry(url, requestType: str, toRetryList: list, companyNumber: str = None):
        """
        Function for handling retry caching logic
        """
        assert requestType in ("search", "officers"), "Request type needs to be in (search, officers)"
        #TO-DO: exceptions
        if companyNumber is None:
            companyNumber = ""
        toRetryList.append({
            "url": url,
            "requestType": requestType,
            "companyNumber": companyNumber
        })

    def computeSleepTime(self) -> int:
        """
        Computes how much time has passed since last checkpoint.
        Used to facilitate ratelimitting and handling 429
        """
        # Compute needed sleeptime
        currentTs = datetime.utcnow()
        timeAfterCheckpoit = (currentTs - self.checkpoint).seconds
        sleepTime = self.rate - timeAfterCheckpoit + self.sleepTimeBuffer
        return int(sleepTime)

    async def countRequest(self):
        """
        Facilitates ratelimitig by keeping track of how many tasks were completed and pausing if needed
        """
        # Check if we have tasks in the limit
        print()
        if self.limit >= int(self.baseLimit * 0.05):
            # Consume if yes
            self.limit -= 1
        else:
            # Compute needed sleeptime
            sleepTime = self.computeSleepTime()
            # Log sleeping step
            self.logger.warning(f"Hit RPS limit, sleeping for {sleepTime} seconds")
            await asyncio.sleep(sleepTime)
            # Assign new checkpoint to self
            self.checkpoint = datetime.utcnow()
            # Refresh our limit capacity
            self.limit = self.baseLimit

    async def handleOverLimit(self, url, requestType = None, companyNumber = None, toRetryList: list = None, first = True):
        """
        Method to handle cases where we go over request limit and receive 429 from the API
        """
        if first:
            sleepTime = self.computeSleepTime()
            self.logger.warning(f"Got 429 for {url}, sleeping for {sleepTime} to retry")
            await asyncio.sleep(sleepTime)
            # Assign new checkpoint to self
            self.checkpoint = datetime.utcnow()
        # If we are hitting 429 more than once, there probably is no point in sleeping more :(
        else:
            self.logger.warning(f"Saving {url} to retry cache")
            self.cacheForRetry(
                url = url,
                requestType = requestType,
                companyNumber = companyNumber,
                toRetryList = toRetryList
            )
    
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
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.get(url = url, auth = auth, params = params)
                print(self.limit)
                dataJson = await resp.json(content_type = None)
                rUrl = resp.url
                if (status := resp.status) != 200:
                    # If we still get 429
                    if status == 429:
                        await self.handleOverLimit(url = rUrl)
                        #Retry on the spot
                        retry = await session.get(
                            url = rUrl, auth = auth
                        )
                        # Here I am repeating myself, maybe create a fancier get function?
                        await self.countRequest()
                        if (retryStatus := retry.status) == 200:
                            self.logger.info(f"Successful retry for {rUrl}")
                            dataJson  = await retry.json(content_type = None)
                        else:
                            self.logger.warning(f"Got {retryStatus} for {rUrl} after retry")
                            if retryStatus == 429:
                                await self.handleOverLimit(
                                    url = rUrl,
                                    requestType = requestType,
                                    companyNumber = companyNumber,
                                    first = False,
                                    toRetryList = toRetry
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
            except Exception as e:
                self.logger.error(f"Got an error with {url}: {e}")
            finally:
                # Count our requests not to hit 429
                await self.countRequest()

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
