import pandas as pd
import dataset
import aiohttp
import asyncio
from typing import Union
from logging import Logger
from aiohttp import BasicAuth
from datetime import datetime
# Connector class for ratelimited requests to the API
class Connector:
    """
    Class for ratelimiting
    """
    def __init__(self, rate: int, limit: int, sleepTimeBuffer: int, logger: Logger, allowedRequestTypes: list):
        self.rate = rate
        self.limit = asyncio.Semaphore(limit)
        self.baseLimit = limit
        self.logger = logger
        self.allowedRequestTypes = allowedRequestTypes
        # Timing variable for rate limiting
        self.checkpoint = datetime.utcnow()
        # Buffer for rate limiting
        self.sleepTimeBuffer = sleepTimeBuffer

    def __validateRequestType(self, requestType: str):
        """
        Validates request type to be present within self
        """
        if requestType not in self.allowedRequestTypes:
            return ValueError(f"Request type needs to be in {self.allowedRequestTypes}")
        else:
            return None

    
    def __cacheForRetry(self, url, requestType: str, toRetryList: list, companyNumber: str = None) -> Union[None, Exception]:
        """
        Function for handling retry caching logic
        """
        # Validate that request type is one of the expected
        e  = self.__validateRequestType(requestType)

        if e is not None:
            return e
    
        if companyNumber is None:
            companyNumber = ""
        toRetryList.append({
            "url": url,
            "requestType": requestType,
            "companyNumber": companyNumber
        })



    def __computeSleepTime(self) -> int:
        """
        Computes how much time has passed since last checkpoint.
        Used to facilitate ratelimitting and handling 429
        """
        # Compute needed sleeptime
        currentTs = datetime.utcnow()
        timeAfterCheckpoit = (currentTs - self.checkpoint).seconds
        sleepTime = self.rate - timeAfterCheckpoit + self.sleepTimeBuffer
        return int(sleepTime)

    async def __countRequest(self):
        """
        Facilitates ratelimitig by keeping track of how many tasks were completed and pausing if needed
        """
        # Check if we have tasks in the limit
        if self.limit._value >= int(self.baseLimit * 0.05): # This is a bit hacky, but works
            # Consume if yes
            #self.limit -= 1
            await self.limit.acquire()
        else:
            # Compute needed sleeptime
            sleepTime = self.__computeSleepTime()
            # Log sleeping step
            self.logger.warning(f"Hit RPS limit, sleeping for {sleepTime} seconds")
            await asyncio.sleep(sleepTime)
            # Assign new checkpoint to self
            self.checkpoint = datetime.utcnow()
            # Refresh our limit capacity
            self.limit = asyncio.Semaphore(self.baseLimit)

    async def __handleOverLimit(self, url, requestType = None, companyNumber = None, toRetryList: list = None, first = True):
        """
        Method to handle cases where we go over request limit and receive 429 from the API
        """
        if first:
            sleepTime = self.__computeSleepTime()
            self.logger.warning(f"Got 429 for {url}, sleeping for {sleepTime} to retry")
            await asyncio.sleep(sleepTime)
            # Assign new checkpoint to self
            self.checkpoint = datetime.utcnow()
        # If we are hitting 429 more than once, there probably is no point in sleeping more :(
        else:
            self.logger.warning(f"Saving {url} to retry cache")
            e = self.__cacheForRetry(
                url = url,
                requestType = requestType,
                companyNumber = companyNumber,
                toRetryList = toRetryList
            )
            
            if e is not None:
                self.logger.warning(f"Retry caching error: {e}")
    
    def __gatherRequestStats(self, metaData: dict, requestType: str, respStatus: int) -> Union[None, KeyError]:
        """
        Stores data on responses in metaData dict. Mostly needed for runtime summary stats
        """
        try:
            if 200<= respStatus < 300:
                metaData[requestType]["success"].append(1)
            else:
                metaData[requestType]["rest"].append(1)
        except KeyError as e:
            self.logger.warning(f"Error when saving response to metadata dict: {e}")
    
    async def makeRequest(
        self,
        requestType: str,
        url: str,
        auth: aiohttp.BasicAuth,
        storage: list,
        toRetry: list,
        metaData: dict,
        params: dict = None,
        companyNumber: str = None) -> None:
        """
        Async function for making http requests to company house API
        """
        e = self.__validateRequestType(requestType)
        # We stop the execution if we face an unexpected request type
        if e is not None:
            # Because we raise exception, logging also happens on the spot
            self.logger.error(f"Cannot make request because of unexpected type {requestType}: {e}")
            raise e
        
        async with aiohttp.ClientSession() as session:
            try:
                await self.__countRequest()
                resp = await session.get(url = url, auth = auth, params = params)
                dataJson = await resp.json(content_type = None)
                rUrl = resp.url
                # Save resp status to metadata dict
                self.__gatherRequestStats(metaData, requestType, resp.status)
                if (status := resp.status) != 200:
                    # If we still get 429
                    if status == 429:
                        await self.__handleOverLimit(url = rUrl)
                        #Retry on the spot
                        retry = await session.get(url = rUrl, auth = auth)
                        # Here I am repeating myself, maybe create a fancier get function?
                        await self.__countRequest()
                        # Save resp status to metadata dict
                        self.__gatherRequestStats(metaData, requestType, retry.status)
                        if (retryStatus := retry.status) == 200:
                            self.logger.info(f"Successful retry for {rUrl}")
                            dataJson  = await retry.json(content_type = None)
                        else:
                            self.logger.warning(f"Got {retryStatus} for {rUrl} after retry")
                            if retryStatus == 429:
                                await self.__handleOverLimit(
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
                self.logger.warning(f"Got an error with {url}: {e}")


# Child object for having a 1 go-to entity for all lead operations
class LeadManager(Connector):
    """
    Object for persisting parameters of lead searching and storing responses
    """
    def __init__(
        self,
        cache: str,
        cacheTable: str,
        retryTable: str,
        rate: int,
        limit: int,
        sleepTimeBuffer: int,
        logger: Logger,
        allowedRequestTypes: list) -> None:

        super().__init__(rate, limit, sleepTimeBuffer, logger, allowedRequestTypes)
        self.logger = logger
        self.searchStorage = []
        self.companyStorage = []
        self.officerStorage = []
        self.toRetryList = []
        self.cacheConn = dataset.connect(f"sqlite:///{cache}")
        self.cacheTable = cacheTable
        self.rertyTable = retryTable

    @staticmethod
    def __parseAddress(addressDict: dict) -> str:
        """
        Parses company registered office address (dict) to string
        """
        return ", ".join(list(addressDict.values()))

    @staticmethod
    def __parseSic(sicCodeList: list) -> str:
        """
        Converts list of sic codes to string
        """
        return ", ".join(sicCodeList)
    
    def __parseSearcResults(self, colsToSave: list, runMetaData: dict) -> pd.DataFrame:
        """
        Iterates over search results (list of dicts) and resaves it as a dataframe
        """ 
        #Read all data to dataframe
        outframe = pd.DataFrame.from_records(self.searchStorage)
        outframe = outframe[colsToSave]
        #Apply transformations to complicated data structs in the dataframe
        outframe["address_string"] = outframe["registered_office_address"].map(self.__parseAddress)
        outframe["sic_codes_string"] = outframe["sic_codes"].map(self.__parseSic)
        #Store in self
        self.processedSearch = outframe
        self.processedSearch.drop(columns = ["registered_office_address", "sic_codes"], inplace = True)
        self.processedSearch["added_on_run_id"] = runMetaData["run_id"]
        self.processedSearch["added_run_ts"] = runMetaData["run_start_ts"]
        #TO-DO: add run id?
    
    def cacheSearch(self, colsToSave: list, runMetaData: dict) -> Union[None, Exception]:
        """
        Writes parsed search results to db
        """
        #TO-DO: exceptions handling
        self.__parseSearcResults(colsToSave, runMetaData = runMetaData)
        records = self.processedSearch.to_dict(orient = "records")
        self.cacheConn[self.cacheTable].insert_many(records)

    def cacheRetries(self, retryType: str) -> Union[None, Exception]:
        """
        Write 429 request urls to cache for retries
        """
        #log exception?
        #TO-DO: unite with cacheSearch?
        try:
            if (retryCnt := len(self.toRetryList)) == 0:
                self.logger.info("No retry URLs to cache :(")
                return

            self.cacheConn[self.rertyTable].insert_many(self.toRetryList)
            self.logger.info(f"Wrote {retryCnt} to cache")
            #Clean list so that it could be re-used for different type of retries
            self.toRetryList = []
            
        except Exception as e:   
            return e

    def getCachedToAppend(self, existingIds: list, runMetaData: dict):
        """
        Reads db for company entries that were searched on previous run and are not in @existingIds
        """
        existingCompanies = [f"\'{item}\'" for item in existingIds]
        existingCompanies = f"({','.join(existingCompanies)})"
        runId = runMetaData["run_id"]

        results = self.cacheConn.query(
            f"""
            SELECT * FROM {self.cacheTable}
            WHERE added_on_run_id <> :runId
            AND company_number NOT IN {existingCompanies}
            """, runId = runId
        )

        df = pd.DataFrame(results)
        try:
            df.drop(columns = ['id'], inplace = True)
        except KeyError:
            pass
        return df
        #TO-DO: Exceptions handling
    
    def __getCachedRetries(self, retryType: str):
        """
        Selects entries from retries cache filtered by a given type to pandas
        """
        try:
            results = self.cacheConn.query(
                f"""
                SELECT url FROM retries
                WHERE type = :retryType
                """, retryType = retryType
            )
            return pd.DataFrame(results), None
        except Exception as e:
            return None, e
    
    def __deleteRetryEntries(self, urls: list):
        """
        Removes rows from retry cache
        """
        toDelete = [f"\'{url}\'" for url in urls]
        toDelete = f"({','.join(toDelete)})"
   
        self.cacheConn.query(
            f"""
            DELETE FROM {self.rertyTable}
            WHERE url IN {toDelete}
            """
        )
    
    def tidySearchResults(self, cacheDf: pd.DataFrame, sheetCompanyNumbers: pd.Series) -> tuple:
        """
        Merges processedSearch with cacheDf and cleans it
        """
        try:
            # Merge with cache
            df =  pd.concat([self.processedSearch, cacheDf], ignore_index = True)
            # Clean Data
            df["added_run_ts"] = pd.to_datetime(df["added_run_ts"])
            df.sort_values(by = "added_run_ts", ascending= True, inplace = True)
            df.drop_duplicates(subset = "company_number", inplace = True, keep = "first")
            # Remove overlap with companies present in the sheet
            df = df[~df["company_number"].isin(sheetCompanyNumbers)]
            df.reset_index(inplace = True, drop = True)
            return df, None
        except Exception as e:
            return None, e

    def processRetryCache(
        self,
        retryType: str,
        taskList: list,
        auth: BasicAuth,
        metaData: dict,
        dbClean = True,
        companyNumber: str = None,
        taskUrlLog: list = None) -> Union[None, Exception]:
        """
        Queries urls for retrying from local DB and adds them to a list of tasks for Asyncio event loop
        """
        retryFrame, err = self.__getCachedRetries(retryType = retryType)
        if err is not None:
            return err
        # Handle case where we have no retries to process
        if (cntRetries := len(retryFrame)) == 0:
            self.logger.info("No retries to process")
            return None
        # Actual processing of data if we get any
        else:
            # This handles only two options, needs to be rewritten if we decide to do more
            storage = self.searchStorage if retryType == "search" else self.officerStorage
            retryLinks = retryFrame["url"].values
            for url in retryLinks:
                #Make sure that we don't add duplicate urls by using data from cache table
                if taskUrlLog is None:
                    pass
                elif url in taskUrlLog:
                    self.logger.info(f"Skipping {url} fromm retry cache because it was already present in task list")
                    continue
                taskList.append(
                    self.makeRequest(
                        url = url,
                        requestType = retryType,
                        logger = self.logger,
                        auth = auth,
                        storage = storage,
                        toRetry = self.toRetryList,
                        companyNumber = companyNumber,
                        metaData = metaData
                    )
                )
            self.logger.info(f"Added {cntRetries} search entries to retry from cache")
            # Clean local db retry table so that we don't spend time on it next time
            if dbClean:
                self.__deleteRetryEntries(retryLinks)
                self.logger.info("Cleaned retry entries from cache")
            if taskUrlLog is not None:
                del taskUrlLog
            return None

    @staticmethod
    def __parseOfficerData(officerJson) -> str:
        """
        Parses contents of officerList response
        """
        try:
            officers = [f"{officer['officer_role']}: {officer['name']}" for officer in officerJson]
            officerStr = "; ".join(officers)
            return officerStr, None
        except Exception as e:
            return None, e
    
    def tidyOfficerResults(self) -> pd.DataFrame:
        """
        Transforms list of officer dicts to a dataframe that can be convenietly joined with other company data
        """
        outframe = pd.DataFrame(columns = ["company_number", "company_officer_names"])
        for entry in self.officerStorage:
            companyNumber = entry["companyNumber"]
            officerData, err = self.__parseOfficerData(entry["data"])
            if err is not None:
                self.logger.warning(f"Failed to parse officer data for company {companyNumber}. Data might be incomplete")
            row = pd.DataFrame({
                "company_number": [companyNumber],  "company_officer_names": [officerData]
            })
            outframe = pd.concat([outframe, row], ignore_index = True)
        return outframe

    def cleanCacheTable(self, cleanRetries = False):
        """
        Removes table from cache db
        """
        try:
            self.cacheConn.load_table(self.cacheTable).delete()
            if cleanRetries:
                self.cacheConn.load_table(self.rertyTable).delete()
                self.logger.info(f"Cleaned cache!")
            return None
        except Exception as e:
            return e
    
