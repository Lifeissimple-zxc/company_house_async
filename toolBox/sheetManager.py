import pandas as pd
import pygsheets
from logging import Logger
from janitor import clean_names #This is used within pandas, not alone
from typing import Union
from pygsheets import PyGsheetsException

class sheetManager:
    def __init__(
        self,
        logger: Logger,
        benchmarkSheets: list,
        controlPanelSheetName: str,
        leadsSheetName: str,
        sheetSecretVarName: str = "GSHEET_SECRET"
        ) -> None:
        """
        Helper class for interacting with Google Sheets API
        """
        self.logger = logger
        self.benchmarkSheets = benchmarkSheets
        self.controlPanelSheetName = controlPanelSheetName
        self.leadsSheetName = leadsSheetName
        self.scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        self.sheetSecretVarName = sheetSecretVarName
    
    def _connect(self) -> Union[None, Exception]:
        """
        Inits a connection to the API
        """
        try:
            client = pygsheets.authorize(service_account_env_var = self.sheetSecretVarName)
            self.client = client
        except Exception as e:
            return e

    
    def _openSheet(self, sheetId: str) -> Union[None, Exception]:
        """
        Open spreadsheet by ID
        """
        try:
            self.spreadsheet = self.client.open_by_key(sheetId)
        except Exception as e:
            return e

    
    def _validateSheet(self) -> Union[None, Exception]:
        """
        Validates that the provided spreadsheet has all the needed tabs
        """
        try:
            sheetTitles = [ws.title for ws in self.spreadsheet.worksheets()]
            for sheet in self.benchmarkSheets:
                if sheet not in sheetTitles:
                    return AttributeError(f"{sheet} is missing in {self.spreadsheet.id} document, reconfig needed!")
        except Exception as e:
            return e

    def _readToDf(self, workSheetName: str):
        """
        Reads sheet to a dataframe
        """
        try:
            # Open worksheet
            worksheet = self.spreadsheet.worksheet_by_title(workSheetName)
        except PyGsheetsException as e:
            return e
        # Read to pandas
        df = pd.DataFrame(worksheet.get_as_df().clean_names())
        # Convert types, can be editted if needed, for now all goes to string
        df.astype({col: "object" for col in  df.columns})
        # Optinal block: attribute assignment
        try:
            # Save worksheet attribute
            worksheetAttr = f"{workSheetName}Sheet"
            setattr(self, worksheetAttr, worksheet)
            # Save frame as an attribute
            dfAttr = f"{workSheetName}Frame"
            setattr(self, dfAttr, df)
        except AttributeError as e:
            return e

        return None
    
    def _parseSearchParams(self, controlPanelFrame: pd.DataFrame) -> Union[dict, None]:
        """
        Function to parse control panel search params to a dict
        """
        try:
            #Create base for 
            searchConfig = {"params": {}}
            searchFrame = controlPanelFrame.query("purpose == 'search' and actual_input != ''")
            for index, row in searchFrame.iterrows():
                key = row["parameter"]
                dataType = row["data_type"]
                input = row["actual_input"]
                
                if dataType == "list":
                    input = ",".join(input.split(";\n"))
                elif dataType == "int":
                    input = int(input)

                if key != "days_back":
                    searchConfig["params"][key] = input
                else:
                    searchConfig[key] = input

            return searchConfig, None
        except Exception as e:
            return None, e
    
    def getColsToKeep(self) -> list: #Do we actualy need this?
        """
        Reads columns to keep from searched data
        """
        try:
            searchFrame = getattr(self, f"{self.controlPanelSheetName}Frame")
            colsStr = searchFrame.query("parameter == 'cols_to_keep'")["actual_input"].values[0]
            cols = colsStr.split(";\n")
            return cols, None
        except Exception as e:
            return None, e
    
    def prepareSeachInputs(self, sheetId: str, workSheetsToRead: list, validation = True) -> tuple:
        """
        Master function that establishes connection to the api and reads prepares input data for search step
        """
        # If statements to avoid unnecessary reconnections
        if not hasattr(self, "client"):
            e = self._connect()
            if e is not None:
                return None, e

        if not hasattr(self, "spreadsheet"):
            e = self._openSheet(sheetId)
            if e is not None:
                return None, e
        
        # Validate if needed
        if validation:
            e = self._validateSheet()
            if e is not None:
                return None, e

        # Read sheets to DFs and assign to self
        for ws in workSheetsToRead:
            e = self._readToDf(ws)
            if e is not None:
                return None, e

        # Parse search params
        searchParams, e = self._parseSearchParams(
            getattr(self, f"{self.controlPanelSheetName}Frame")
        )
        if e is not None:
            return None, e

        return searchParams, None
    
    def appendToSheet(self, sheetLeads: pd.Series, df: pd.DataFrame) -> None:
        """"
        Appends dataframe to the sheet
        """
        df["added_run_ts"] = df["added_run_ts"].astype(str)
        df["date_of_creation"] = pd.to_datetime(df["date_of_creation"])
        # Sort by created for convenience
        df.sort_values(by = "date_of_creation", ascending = True, inplace = True)
        df["date_of_creation"] = df["date_of_creation"].astype(str)
        rowsUpdate = df.values.tolist()
        getattr(self, f"{self.leadsSheetName}Sheet").insert_rows(len(sheetLeads) + 1, number = len(df), values = rowsUpdate)
        self.logger.info(f"{len(df)} new leads have been appended to the sheet")

