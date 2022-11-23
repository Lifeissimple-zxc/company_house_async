import pandas as pd
import dataset
from typing import Union
from logging import Logger
from connector import Connector
from aiohttp import BasicAuth

class LeadManager:
    """
    Object for persisting parameters of lead searching and storing responses
    """
    def __init__(self, cache: str, logger: Logger):
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
        connector: Connector,
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
                    connector.makeRequest(
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









        


