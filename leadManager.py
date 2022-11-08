import pandas as pd
import dataset
from typing import Union
from logging import Logger


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
    
    def processRetries(self, retryType: str) -> tuple:
        """
        Processes list of retry urls to a list of dicts
        Retry type is needed to correctly handle cash
        """
        if retryType not in ("search", "company", "search"):
            raise Exception("""Wrong value for retryType input, should be in ("search", "company", "search")""")
        try:
            return [{"url": item, "type": retryType} for item in self.toRetryList], None
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

            records, err = self.processRetries(retryType)
            if err is not None:
                self.logger.error(f"Retry cache preparation error: {err}")
                return err

            with dataset.connect(self.cache) as conn:
                conn[self.rertyTable].insert_many(records)
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
        #TO-DO: Handle retries
        with dataset.connect(self.cache) as conn:
            results = conn.query(
                f"""
                SELECT url FROM retries
                WHERE type = :retryType
                """, retryType = retryType
            )
        return pd.DataFrame(results)
    
    def deleteRetryEntries(self, urls: list):
        """
        Removes rows from retry cache
        """
        toDelete = [f"\'{url}\'" for url in urls]
        print(toDelete)
        toDelete = f"({','.join(toDelete)})"
        print(toDelete)
        with dataset.connect(self.cache) as conn:
            conn.query(
                f"""
                DELETE FROM {self.rertyTable}
                WHERE url IN {toDelete}
                """
            )
    
    def tidySearchResults(self, cacheDf: pd.DataFrame) -> tuple:
        """
        Merges processedSearch with cacheDf and cleans it
        """
        try:
            df =  pd.concat([self.processedSearch, cacheDf], ignore_index = True)
            df["added_run_ts"] = pd.to_datetime(df["added_run_ts"])
            df.sort_values(by = "added_run_ts", ascending= True, inplace = True)
            df.drop_duplicates(subset = "company_number", inplace = True, keep = "first")
            df.reset_index(inplace = True, drop = True)
            return df, None
        except Exception as e:
            self.logger.error(f"Search & Cache cleaning faced error: {e}")
            return None, e








        


