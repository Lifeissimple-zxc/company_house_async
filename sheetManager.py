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

class sheetManager:
    def __init__(
        self,
        logger: customLogger, #TO-DO: can we use a simple logger here?
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
    
    def connect(self):
        """
        Inits a connection to the API
        """
        try:
            client = pygsheets.authorize(service_account_env_var = self.sheetSecretVarName)
            self.client = client
        except Exception as e:
            self.logger.logger.error(f"Api connection error: {e}")
            

    
    def openSheet(self, sheetId: str):
        """
        Open spreadsheet by ID
        """
        try:
            self.spreadsheet = self.client.open_by_key(sheetId)
        except Exception as e:
            self.logger.logger.error(f"Failed to open sheet with id {sheetId}: {e}")
            #TO-DO: send notification

    
    def validateSheet(self):
        """
        Validates that the provided spreadsheet has all the needed tabs
        """
        try:
            sheetTitles = [ws.title for ws in self.spreadsheet.worksheets()]
            for sheet in self.benchmarkSheets:
                if sheet not in sheetTitles:
                    self.logger.logger.warning(f"{sheet} is missing in {self.sheetId} document, reconfig needed!")
                    exit()
        except Exception as e:
            self.logger.logger.error(f"Sheet validation error: {e}")

    def readControlPanel(self):
        try:
            self.panelSheet = self.spreadsheet.worksheet_by_title(self.controlPanelSheetName)
            self.controlPanelFrame = pd.DataFrame(self.panelSheet.get_as_df()).clean_names()
        except Exception as e:
            self.logger.logger.error(f"Control panel reading error: {e}")
    
    def parseSearchParams(self):
        try:
            searchConfig = {"headers": {}}
            searchFrame = self.controlPanelFrame.query("purpose == 'search' and actual_input != ''")
            for row in searchFrame.iterrows():
                key = row[1]["parameter"]
                dataType = row[1]["data_type"]
                input = row[1]["actual_input"]
                
                if dataType == "list":
                    input = ",".join(input.split(";\n"))
                elif dataType == "int":
                    input = int(input)

                if key != "days_back":
                    searchConfig["headers"][key] = input
                else:
                    searchConfig[key] = input

            return searchConfig, None
        except Exception as e:
            self.logger.logger.error(f"Search params parsing error: {e}")
            return None, e

    def readLeadsTable(self):
        try:
            self.leadSheet = self.spreadsheet.worksheet_by_title(self.leadsSheetName)
            self.sheetLeadsFrame = pd.DataFrame(self.leadSheet.get_as_df()).clean_names()
            return len(self.sheetLeadsFrame), None
        except Exception as e:
            self.logger.logger.error(f"Reading leads table error: {e}")
            return None, e
        

