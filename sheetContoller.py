from cgitb import handler
import pandas as pd
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
import json
from dotenv import load_dotenv
import os
from sys import exit
from janitor import clean_names

#Read from env file
load_dotenv()
GSHEET_SECRET = os.getenv("GSHEET_SECRET")
GSHEET_ID = os.environ.get("GSHEET_ID")
BENCHMARK_SHEETNAMES = os.getenv("BENCHMARK_SHEETNAMES")
GSHEET_CONTROL_PANEL_NAME = os.getenv("GSHEET_CONTROL_PANEL_NAME")

sheetSecrets = json.loads(GSHEET_SECRET)
benchmarkSheets = BENCHMARK_SHEETNAMES.split(",")

#Rewrite the below with pygsheets (this lib is used at uber also!)
class sheetController:
    def __init__(self, sheetId: str, sheetSecrets, benchmarkSheets, controlPanelSheetName):
        # self.sheetId = sheetId,
        self.sheetId = sheetId
        self.sheetSecrets = sheetSecrets
        self.benchmarkSheets = benchmarkSheets
        self.controlPanelSheetName = controlPanelSheetName
        self.scope = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    
    def openSheet(self):
        client = pygsheets.authorize(service_account_env_var = "GSHEET_SECRET")

        if client: print("Connected to Google API!")
        else:
            print("Connection to Google API failed :(")
            #send notification :( )
            exit()
        try:
            self.speadsheet = client.open_by_key(self.sheetId)
        except:
            print("Failed to open file!")
            #Notification
    
    def validateSheet(self):
        sheetTitles = [ws.title for ws in self.speadsheet.worksheets()]
        for sheet in self.benchmarkSheets:
            if sheet not in sheetTitles:
                print(f"{sheet} is missing in {self.sheetId} document, reconfig needed!")
                exit()
        print("Sheet has all the needed tabs!")

    def readControlPanel(self):
        self.panelSheet = self.speadsheet.worksheet_by_title(self.controlPanelSheetName)
        self.controlPanelFrame = pd.DataFrame(
                self.panelSheet.get_values(
                start = (1,1),
                end = "F1000"
                )
            )
        self.controlPanelFrame.columns = self.controlPanelFrame.iloc[0]
        self.controlPanelFrame = clean_names(self.controlPanelFrame[1:])
        self.controlPanelFrame.reset_index(drop = True)
    
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
        

handler = sheetController(
    sheetId = GSHEET_ID,
    sheetSecrets = sheetSecrets,
    benchmarkSheets = benchmarkSheets,
    controlPanelSheetName = GSHEET_CONTROL_PANEL_NAME
)
handler.openSheet()
handler.validateSheet()
handler.readControlPanel()
searchParams = handler.parseSearchParams()
print(searchParams)
# handler.validateSheet()
# df = handler.speadsheet.worksheet("controlPanel").get_all_values()
# print(df)