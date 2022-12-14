import pandas as pd
import yaml
from datetime import timedelta, date, datetime
from os.path import exists
from os import makedirs
from os.path import join
from typing import Union
from logging import Logger
from uuid import uuid4


class utilMaster:
    """
    Helper class that contains multi-purpose functions.
    Main goal is to persists logger and handle exceptions.
    """
    def __init__(self):
        pass

    def assignLogger(self, logger: Logger) -> None:
        """
        Assigns logger to the class instance that will be persisted for further use and errors'logging
        """
        if not isinstance(logger, Logger):
            raise TypeError("Passed logger does not belong to Logger class!")
        self.logger = logger
            


    def getDaysDelta(self, lastLeadDate: date):
        try:
            delta = date.today() - lastLeadDate
            return delta.days + 1 #adding 1 to cover overlapping day as well!
        except Exception as e:
            self.logger.error(f"Days delta computation error: {e}")
            return None

    def createSearchDates(self, daysBack):
        try:
            return [date.today() - timedelta(days = x) for x in range(daysBack + 1)]
        except Exception as e:
            self.logger.error(f"Search daterange computation error: {e}")
            return None

    def createParams(self, headerBase: dict, day: date) -> Union[dict, Exception]:
        try:
            output = headerBase
            output["incorporated_from"] = str(day)
            output["incorporated_to"] = str(day)
            return output
        except Exception as e:
            self.logger.error(f"Request params generation error: {e}")
            return None

    def softDirCreate(self, path: str) -> Union[None, Exception]:
        """
        Checks if directory exists and creates if not
        """ 
        try:
            if not exists(path):
                makedirs(path)
            return None
        except Exception as e:
            self.logger.error(f"Directory creation error: {e}")
            return e

    def checkMaxDate(self, df: pd.DataFrame, dateSeriesName: str, searchParams: dict) -> Union[dict, None]:
        try:
            maxLeadCreatedStr = str(max(pd.to_datetime(df[dateSeriesName])))[:10]
            maxLeadCreatedDate = datetime.strptime(maxLeadCreatedStr, "%Y-%m-%d").date()
            #TO-DO: LOG step of adjusting days_back_parameter
            daysFromLastLead = self.getDaysDelta(lastLeadDate = maxLeadCreatedDate)
            searchParams["days_back"] = daysFromLastLead
            return searchParams
        except Exception as e:
            self.logger.error(f"Max date check error: {e}")
            return None
    
    def readYaml(self, path: str) -> Union[dict, None]: #TBD if needed
        """
        Read Yaml to a dict, log if there is an error
        """
        try:
            with open(path, 'r') as yamlFile:
                ymlParsed = yaml.safe_load(yamlFile)
                return ymlParsed
        except Exception as e:
            self.logger.error(f"Yaml read error: {e}")
            return None
    
    @staticmethod
    def generateRunMetaData(requestTypes: list) -> dict:
        """
        Function that generates run uuid and its start time
        """
        base = {"run_id": str(uuid4()), "run_start_ts": datetime.utcnow()}
        for rtype in requestTypes:
            container = dict(success = [], rest = [])
            base[rtype] = container
        return base

    @staticmethod
    def splitToChunks(array: list, chunkSize: int) -> list:
        """
        Splits a list into chunks of given size
        :param array[list] list of items to be split to chunks:
        :param chunkSize[int] size defining the items in one chunk:
        :return nested list:
        """
        return [array[i:i + chunkSize] for i in range(0, len(array), chunkSize)]

    @staticmethod
    def getRunTimeStats(metadata: dict) -> dict:
        """
        Uses metadata dict to compute summary stats of the run and returns as a dict
        """
        # Computations
        runEnd = datetime.utcnow()
        runtime = round((runEnd - metadata["run_start_ts"]).seconds)
        # Search endpoint stats
        searchRequestsSuccess = sum(metadata["search"]["success"])
        searchRequestsRest = sum(metadata["search"]["rest"])
        searchRequests = searchRequestsSuccess + searchRequestsRest
        # Officer endpoint stats
        officerRequestsSuccess = sum(metadata["officers"]["success"])
        officerRequestsRest = sum(metadata["officers"]["rest"])
        officerRequests = officerRequestsSuccess + officerRequestsRest
        # Save results to dict
        summary = dict(
            run_start_ts = metadata["run_start_ts"],
            run_end_ts = runEnd,
            runtime = runtime,

            total_search_requests = searchRequests,
            success_search_requests = searchRequestsSuccess,
            fail_search_requests = searchRequestsRest,

            total_officer_requests = officerRequests,
            success_officer_requests = officerRequestsSuccess,
            fail_officer_requests = officerRequestsRest,
        )
        return summary
             

