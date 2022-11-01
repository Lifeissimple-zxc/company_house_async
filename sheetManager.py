import pandas as pd
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import json
from dotenv import load_dotenv
import os
from sys import exit
from janitor import clean_names
from attr import define
from logging import Logger
from customLogger import customLogger
from typing import Union

class sheetManager:
    def __init__(
        self,
        logger: Logger, #TO-DO: can we use a simple logger here?
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
    
    def connect(self) -> Union[None, Exception]:
        """
        Inits a connection to the API
        """
        try:
            client = pygsheets.authorize(service_account_env_var = self.sheetSecretVarName)
            self.client = client
        except Exception as e:
            self.logger.error(f"Api connection error: {e}")
            return e

    
    def openSheet(self, sheetId: str) -> Union[None, Exception]:
        """
        Open spreadsheet by ID
        """
        try:
            self.spreadsheet = self.client.open_by_key(sheetId)
        except Exception as e:
            self.logger.error(f"Failed to open sheet with id {sheetId}: {e}")
            #TO-DO: send notification
            return e

    
    def validateSheet(self) -> Union[None, Exception]:
        """
        Validates that the provided spreadsheet has all the needed tabs
        """
        try:
            sheetTitles = [ws.title for ws in self.spreadsheet.worksheets()]
            for sheet in self.benchmarkSheets:
                if sheet not in sheetTitles:
                    self.logger.warning(f"{sheet} is missing in {self.sheetId} document, reconfig needed!")
                    return AttributeError("Spreadsheet does not have the needed configuration")
        except Exception as e:
            self.logger.error(f"Sheet validation error: {e}")
            return e

    def readControlPanel(self):
        try:
            self.panelSheet = self.spreadsheet.worksheet_by_title(self.controlPanelSheetName)
            self.controlPanelFrame = pd.DataFrame(self.panelSheet.get_as_df()).clean_names()
        except Exception as e:
            self.logger.error(f"Control panel reading error: {e}")
    
   
    def readLeadsTable(self):
        try:
            self.leadSheet = self.spreadsheet.worksheet_by_title(self.leadsSheetName)
            self.leadFrame = pd.DataFrame(self.leadSheet.get_as_df()).clean_names()
            return None
        except Exception as e:
            self.logger.error(f"Reading leads table error: {e}")
            return e

    
    def sheetToDf(self, workSheetName: str) -> tuple:
        """
        Read spreadsheet to pandas, a generic function
        """
        try:
            worksheet = self.spreadsheet.worksheet_by_title(workSheetName)
            return pd.DataFrame(worksheet.get_as_df().clean_names()), None
        except Exception as e:
            self.logger.error(f"{workSheetName} reading error: {e}")
            return None, e

    def parseSearchParams(self) -> Union[dict, None]:
        """
        Function to parse control panel search params to a dict
        """
        try:
            #Create base for 
            searchConfig = {"params": {}}
            searchFrame = self.controlPanelFrame.query("purpose == 'search' and actual_input != ''")
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

            return searchConfig
        except Exception as e:
            self.logger.error(f"Search params parsing error: {e}")
            return None
    
    def prepareSeachInputs(self, sheetId: str, validation = True) -> tuple:
        """
        Master function that establishes connection to the api and reads prepares input data for search step
        """
        try:
            #If statements to avoid unnecessary reconnections
            if not hasattr(self, "client"):
                self.connect()
            if not hasattr(self, "spreadsheet"):
                if sheetId is not None:
                    self.openSheet(sheetId)
            #Validate if needed
            if validation:
                self.validateSheet()
            #Read sheets to DFs
            self.readControlPanel()
            self.readLeadsTable()
            #Parse search params
            return self.parseSearchParams(), None
        except Exception as e:
            return None, e

