# Using simplequeue bc we do not need task tracking when doing logging
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
    # Continue from here
    # Make sure that only warning and above gets  emitted
    pass

# Once discord handler is done --> work on configuring a queue for logger
    

