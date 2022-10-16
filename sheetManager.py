import pandas as pd
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import json
from dotenv import load_dotenv
import os
from sys import exit
from janitor import clean_names
from attr import define

#Read from env file
load_dotenv()
GSHEET_SECRET = os.getenv("GSHEET_SECRET")
GSHEET_ID = os.environ.get("GSHEET_ID")
BENCHMARK_SHEETNAMES = os.getenv("BENCHMARK_SHEETNAMES")
GSHEET_CONTROL_PANEL_NAME = os.getenv("GSHEET_CONTROL_PANEL_NAME")
GSHEET_LEAD_TABLE_NAME = os.getenv("GSHEET_LEAD_TABLE_NAME")

sheetSecrets = json.loads(GSHEET_SECRET)
benchmarkSheets = BENCHMARK_SHEETNAMES.split(",")

#Rewrite the below with pygsheets (this lib is used at uber also!)

class sheetManager:
    def __init__(self, sheetId: str, sheetSecrets, benchmarkSheets, controlPanelSheetName, leadsSheetName):
        # self.sheetId = sheetId,
        self.sheetId = sheetId
        self.sheetSecrets = sheetSecrets
        self.benchmarkSheets = benchmarkSheets
        self.controlPanelSheetName = controlPanelSheetName
        self.leadsSheetName = leadsSheetName
        self.scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

    
    def openSheet(self):
        client = pygsheets.authorize(service_account_env_var = "GSHEET_SECRET")

        if client: print("Connected to Google API!")
        else:
            print("Connection to Google API failed :(")
            #send notification :( )
            exit()
        try:
            self.spreadsheet = client.open_by_key(self.sheetId)
        except:
            print("Failed to open file!")
            #Notification
    
    def validateSheet(self):
        sheetTitles = [ws.title for ws in self.spreadsheet.worksheets()]
        for sheet in self.benchmarkSheets:
            if sheet not in sheetTitles:
                print(f"{sheet} is missing in {self.sheetId} document, reconfig needed!")
                exit()
        print("Sheet has all the needed tabs!")

    def readControlPanel(self):
        self.panelSheet = self.spreadsheet.worksheet_by_title(self.controlPanelSheetName)
        self.controlPanelFrame = pd.DataFrame(self.panelSheet.get_as_df()).clean_names()
    
    def parseSearchParams(self):
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

        return searchConfig
    
    def readLeadsTable(self):
        self.leadSheet = self.spreadsheet.worksheet_by_title(self.leadsSheetName)
        self.sheetLeadsFrame = pd.DataFrame(self.leadSheet.get_as_df()).clean_names()
        return len(self.sheetLeadsFrame)
        

