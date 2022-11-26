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
                    self.logger.warning(f"{sheet} is missing in {self.spreadsheet.id} document, reconfig needed!")
                    return AttributeError("Spreadsheet does not have the needed configuration")
        except Exception as e:
            self.logger.error(f"Sheet validation error: {e}")
            return e

    def readToDf(self, workSheetName: str, makeAttr = True):
        """
        Reads sheet to a dataframe
        """
        try:
            # Open worksheet
            worksheet = self.spreadsheet.worksheet_by_title(workSheetName)
        except PyGsheetsException as e:
            self.logger.error(f"{workSheetName} reading error: {e}")
            return e
        # Read to pandas
        df = pd.DataFrame(worksheet.get_as_df().clean_names())
        # Convert types, can be editted if needed, for now all goes to string
        df.astype({col: "object" for col in  df.columns})
        # Optinal block: attribute assignment
        if makeAttr:
            try:
                # Save worksheet attribute
                worksheetAttr = f"{workSheetName}Sheet"
                setattr(self, worksheetAttr, worksheet)
                # Save frame as an attribute
                dfAttr = f"{workSheetName}Frame"
                setattr(self, dfAttr, df)
            except AttributeError as e:
                self.logger.error(
                    f"Failed to assign attributes for sheet {workSheetName}, details: {e}"
                    )
                return e
        return None
    
    def parseSearchParams(self, controlPanelFrame: pd.DataFrame) -> Union[dict, None]:
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
            self.logger.error(f"Search params parsing error: {e}")
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
            self.logger.error(f"Cols to keep reading error: {e}")
            return None, e
    
    def prepareSeachInputs(self, sheetId: str, workSheetsToRead: list, validation = True) -> tuple:
        """
        Master function that establishes connection to the api and reads prepares input data for search step
        """
        # If statements to avoid unnecessary reconnections
        if not hasattr(self, "client"):
            self.connect()
        if not hasattr(self, "spreadsheet"):
            if sheetId is not None:
                self.openSheet(sheetId)
        # Validate if needed
        if validation:
            self.validateSheet()
        # Read sheets to DFs and assign to self
        for ws in workSheetsToRead:
            readErr = self.readToDf(ws)
            if readErr is not None:
                raise Exception("Failed to read needed sheets data, cannot proceed further :(")
        # Parse search params
        searchParams, prepErr = self.parseSearchParams(
            getattr(self, f"{self.controlPanelSheetName}Frame")
        )
        if prepErr is not None:
            self.logger.error(f"Preparing search inputs failed: {prepErr}")
            raise prepErr
        return searchParams
    
    def appendToSheet(self, sheetLeads: pd.Series, df: pd.DataFrame):
        """"
        Appends dataframe to the sheet
        """
        df["added_run_ts"] = df["added_run_ts"].astype(str)
        rowsUpdate = df.values.tolist()
        getattr(self, f"{self.leadsSheetName}Sheet").insert_rows(len(sheetLeads) + 1, number = len(df), values = rowsUpdate)
        self.logger.info(f"{len(df)} new leads have been appended to the sheet")

