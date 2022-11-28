# Using simplequeue bc we do not need task tracking when doing logging
import asyncio
import aiohttp
import requests
import logging
import dataset
from datetime import datetime
from queue import SimpleQueue as Queue
from logging import (
    Handler,
    StreamHandler,
    LogRecord
)
from logging.handlers import (
    QueueHandler,
    QueueListener,
    RotatingFileHandler,
    TimedRotatingFileHandler
)
from toolBox import (
    LOG_DB,
    LOG_DB_TABLE_NAME
)

# handler for async: https://stackoverflow.com/questions/45842926/python-asynchronous-logging
# handler for db: https://stackoverflow.com/questions/67693767/how-do-i-create-an-sqlite-3-database-handler-for-my-python-logger
# Different handlders for different levels: https://medium.com/nerd-for-tech/logging-with-logging-in-python-d3d8eb9a155a
# Inspiration for discord handler: https://pypi.org/project/python-logging-discord-handler/
# Discord markdown doc: https://support.discord.com/hc/en-us/articles/210298617-Markdown-Text-101-Chat-Formatting-Bold-Italic-Underline-
#Set queue for async code
#Write handler for discord - this first!

class dbHandler(Handler):
    """
    Custom handler that will be used to write logs to an sqlite db
    """
    def __init__(self, db: str, table: str, runId: str) -> None:
        # Inherit from parent
        super().__init__()
        # Connect to the database
        self.db = dataset.connect(f"sqlite:///{db}")
        # Store table name & run id in self for further use
        self.table = table
        self.runId = runId
        

    def _prepareRecord(self, record: LogRecord):
        """
        Prepares log record to be inserted to a database
        """
        recordMessage = record.getMessage()
        recordFunc = record.funcName
        recordLevel = record.levelname
        # Transform created time to a more readable format
        recordCreated = datetime.utcfromtimestamp(record.created)

        # Prepare a dict to add to db
        row = dict(
            record_message = recordMessage,
            record_function = recordFunc,
            record_level = recordLevel,
            record_created = recordCreated,
            run_id = self.runId
        )
        return row
    
    def emit(self, record):
        """
        Reads record instance to a dict and writes this dict to db
        """
        # We firstly prepare our record
        row = self._prepareRecord(record)
        # Insert data to db
        self.db.get_table(self.table).insert(row)

class discordHandler(Handler):
    """
    Custom handler to send logs to Discord
    """
    def __init__(self, webhook: str, poc1: str, poc2: str, runId: str, maxChars: int = 2000) -> None:
        # Inherit from parent class
        super().__init__()
        # Create session for requests
        self.sesh = requests.session()
        # Persisnt webhook within self
        self.webhook = webhook
        # Persist for enforcing character limit: https://discord.com/developers/docs/resources/webhook#execute-webhook
        self.maxChars = maxChars
        # Store useful attr within self
        self.poc1 = poc1
        self.poc2 = poc2
        self.runId = runId
        # Set level: we only want warning+ to go to discord
        self.setLevel(logging.WARNING)
    
    @staticmethod
    def _formatLevel(record: LogRecord, wrapperString = ":warning:") -> str:
        """
        Formats level of a message, based on https://docs.python.org/3/library/logging.html#logging-levels
        """
        recordLevel = record.levelname
        # Add more warning signs depending on the level of the issue we are getting
        if record.levelno == 30:
            recordLevel = f"{wrapperString}recordLevel{wrapperString}"
        elif record.levelno == 40:
            recordLevel = f"{wrapperString * 2}recordLevel{wrapperString * 2}"
        elif record.levelno == 50:
            recordLevel = f"{wrapperString * 3}recordLevel{wrapperString * 3}"
        else:
            pass
        
        return recordLevel

    def _truncateMessage(self, message: str) -> str:
        """
        Truncates message to fit in within the Discord character limit
        """
        message = message[:self.maxChars + 1]
        return message

    def _prepareMessage(self, record: LogRecord) -> str:
        """
        Method for preparing a discord message from the record object
        """
        # Get message attributes from the record
        recordMessage = record.getMessage()
        recordFunc = record.funcName
        recordLevelFormatted = self._formatLevel(record)
        # Compose final message
        fullMessage = f"<@{self.poc1}>\n<@{self.poc2}>\nRun: {self.runId}.\n{recordLevelFormatted} IN {recordFunc}(). Details: {recordMessage}"
        # Truncate message
        fullMessage = self._truncateMessage(fullMessage)
        
        return fullMessage

    def _prepareContents(self, record: LogRecord) -> dict:
        """
        Prepare content that can be sent to Discord Webhook
        """
        fullMessage = self._prepareMessage(record)

        payload = {
            "content": fullMessage
        }

        return payload

    def sendToDiscord(self, record: LogRecord, retries: int = 3):
        """
        Uses other other methods as building blocks to send a log record to Discord as string
        """
        data = self._prepareContents(record)
        print(data)

        resp = self.sesh.post(url = self.webhook, data = data)
        print("Hi from discord message", resp.text)
        # Check if we want to retry
        if 200 <= resp.status_code < 300:
            return
        for i in range(retries):
            resp = self.sesh.post(url = self.webhook, data = data)
            if 200 <= resp.status_code < 300:
                break

    def emit(self, record: LogRecord):
        """
        Calls async sendToDiscord to produce a discord message
        """
        self.sendToDiscord(record)

#The above works in a sync fashion, try to make it async using top level api, then try lower levels
            

    

        


# Once discord handler is done --> work on configuring a queue for logger
    

