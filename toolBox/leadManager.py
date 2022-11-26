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


# Child object for having a 1 go-to entity for all lead operations
class LeadManager(Connector):
    """
    Object for persisting parameters of lead searching and storing responses
    """
    def __init__(self, cache: str, rate: int, limit: int, sleepTimeBuffer: int, logger: Logger):
        super().__init__(rate, limit, sleepTimeBuffer, logger)
        self.logger = logger
        self.searchStorage = []
        self.companyStorage = []
        self.officerStorage = []
        self.toRetryList = []
        self.cache = f"sqlite:///{cache}"
        self.cacheTable = "companies"
        self.rertyTable = "retries"

    @staticmethod
    def parseAddress(addressDict: dict) -> str:
        """
        Parses company registered office address (dict) to string
        """
        return ", ".join(list(addressDict.values()))

    @staticmethod
    def parseSic(sicCodeList: list) -> str:
        """
        Converts list of sic codes to string
        """
        return ", ".join(sicCodeList)
    
    def parseSearcResults(self, colsToSave: list, runMetaData: dict) -> pd.DataFrame:
        """
        Iterates over search results (list of dicts) and resaves it as a dataframe
        """ 
        #Read all data to dataframe
        outframe = pd.DataFrame.from_records(self.searchStorage)
        outframe = outframe[colsToSave]
        #Apply transformations to complicated data structs in the dataframe
        outframe["address_string"] = outframe["registered_office_address"].map(self.parseAddress)
        outframe["sic_codes_string"] = outframe["sic_codes"].map(self.parseSic)
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
        self.parseSearcResults(colsToSave, runMetaData = runMetaData)
        records = self.processedSearch.to_dict(orient = "records")
        with dataset.connect(self.cache) as conn:
            conn[self.cacheTable].insert_many(records)
    
    def processRetries(self, retryType: str, companyNumber: str = None) -> tuple:
        """
        Processes list of retry urls to a list of dicts
        Retry type is needed to correctly handle cash
        """
        if retryType not in ("search", "company", "officers"):
            raise Exception("""Wrong value for retryType input, should be in ("search", "company", "search")""")
        try:
            if companyNumber is None:
                companyNumber = ""
            return [{"url": item, "type": retryType, "company_number": companyNumber} for item in self.toRetryList], None
        except Exception as e:
            return None, e

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

            with dataset.connect(self.cache) as conn:
                conn[self.rertyTable].insert_many(self.toRetryList)
            self.logger.info(f"Wrote {retryCnt} to cache")
            #Clean list so that it could be re-used for different type of retries
            self.toRetryList = []
            
        except Exception as e:
            self.logger.error(f"Retries caching error: {e}")         
            return e

    def getCachedToAppend(self, existingIds: list, runMetaData: dict):
        """
        Reads db for company entries that were searched on previous run and are not in @existingIds
        """
        existingCompanies = [f"\'{item}\'" for item in existingIds]
        existingCompanies = f"({','.join(existingCompanies)})"
        runId = runMetaData["run_id"]
        with dataset.connect(self.cache) as conn:
            results = conn.query(
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
    
    def getCachedRetries(self, retryType: str):
        """
        Selects entries from retries cache filtered by a given type to pandas
        """
        try:
            with dataset.connect(self.cache) as conn:
                results = conn.query(
                    f"""
                    SELECT url FROM retries
                    WHERE type = :retryType
                    """, retryType = retryType
                )
            return pd.DataFrame(results), None
        except Exception as e:
            return None, e
    
    def deleteRetryEntries(self, urls: list):
        """
        Removes rows from retry cache
        """
        toDelete = [f"\'{url}\'" for url in urls]
        toDelete = f"({','.join(toDelete)})"
        with dataset.connect(self.cache) as conn:
            conn.query(
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
            self.logger.error(f"Search & Cache cleaning faced error: {e}")
            return None, e

    def processRetryCache(
        self,
        retryType: str,
        taskList: list,
        auth: BasicAuth,
        dbClean = True,
        companyNumber: str = None,
        taskUrlLog: list = None) -> Union[None, Exception]:
        """
        Queries urls for retrying from local DB and adds them to a list of tasks for Asyncio event loop
        """
        retryFrame, err = self.getCachedRetries(retryType = retryType)
        if err is not None:
            self.logger.error(f"Error when getting retry urls of {retryType} type: {err}")
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
                        companyNumber = companyNumber
                    )
                )
            self.logger.info(f"Added {cntRetries} search entries to retry from cache")
            # Clean local db retry table so that we don't spend time on it next time
            if dbClean:
                self.deleteRetryEntries(retryLinks)
                self.logger.info("Cleaned retry entries from cache")
            if taskUrlLog is not None:
                del taskUrlLog
            return None

    @staticmethod
    def parseOfficerData(officerJson) -> str:
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
            officerData, err = self.parseOfficerData(entry["data"])
            if err is not None:
                self.logger.error(f"Failed to parse officer data for company {companyNumber}. Data might be incomplete")
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
            with dataset.connect(self.cache) as conn:
                conn.load_table(self.cacheTable).delete()
                if cleanRetries:
                    conn.load_table(self.rertyTable).delete()
            self.logger.info(f"Cleaned cache!")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to clean cache: {e}")
            return e
