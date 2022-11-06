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
    
    def processRetries(self) -> tuple:
        """
        Processes list of retry urls to a list of dicts
        """
        try:
            return [{"url": item} for item in self.toRetryList], None
        except Exception as e:
            return None, e

    def cacheRetries(self) -> Union[None, Exception]:
        """
        Write 429 request urls to cache for retries
        """
        #log exception?
        #TO-DO: unite with cacheSearch?
        try:
            if (retryCnt := len(self.toRetryList)) == 0:
                self.logger.info("No retry URLs to cache :(")
                return

            records, err = self.processRetries()
            if err is not None:
                self.logger.error(f"Retry cache preparation error: {err}")
                return err

            with dataset.connect(self.cache) as conn:
                conn[self.rertyTable].insert_many(records)
            self.logger.info(f"Wrote {retryCnt} to cache")
            
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
        df.drop(columns = ['id'], inplace = True)
        return df
        #TO-DO: Exceptions handling






        


