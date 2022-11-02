import pandas as pd
import dataset
from typing import Union


class LeadManager:
    """
    Object for persisting parameters of lead searching and storing responses
    """
    def __init__(self, cache: str):
        self.searchStorage: list = []
        self.companyStorage: list = []
        self.officerStorage: list = []
        self.cache = f"sqlite:///{cache}"
        self.cacheTable = "companies"

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
    
    def parseSearcResults(self, colsToSave: list) -> pd.DataFrame:
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
        #TO-DO: add run id?
    
    def writeCache(self) -> Union[None, Exception]:
        """
        Writes parsed search results to db
        """
        #TO-DO: exceptions handling
        records = self.processedSearch.to_dict(orient = "records")
        with dataset.connect(self.cache) as conn:
            conn[self.cacheTable].insert_many(records)




        


