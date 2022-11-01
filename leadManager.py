import pandas as pd
import dataset
class LeadManager:
    """
    Object for persisting parameters of lead searching and storing responses
    """
    def __init__(self, cache: str):
        self.searchStorage: list = []
        self.companyStorage: list = []
        self.officerStorage: list = []
        self.cache = dataset.connect(f"sqlite:///{cache}")
    
    def parseSearcResuts(self, colsToSave: list) -> pd.DataFrame:
        """
        Iterates over search list of dicts and resaves it as a dataframe
        """ 
        # outframe = pd.DataFrame(colums = colsToSave)
        # for entry in self.searchStorage:
        #     appendRow = pd.DataFrame(entry)[colsToSave]
        #     outframe = pd.concat(outframe, appendRow)
        outframe = pd.DataFrame.from_records(self.searchStorage)
        return outframe[colsToSave]
    


