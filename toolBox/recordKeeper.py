# Using simplequeue bc we do not need task tracking when doing logging
from queue import SimpleQueue as Queue
import logging
from logging.handlers import (
    QueueHandler,
    QueueListener,
    RotatingFileHandler,
    TimedRotatingFileHandler
)

# handler for async: https://stackoverflow.com/questions/45842926/python-asynchronous-logging
# handler for db: 
# Different handlders for different levels: https://medium.com/nerd-for-tech/logging-with-logging-in-python-d3d8eb9a155a
# Inspiration for discord handler: https://pypi.org/project/python-logging-discord-handler/
# Discord markdown doc: https://support.discord.com/hc/en-us/articles/210298617-Markdown-Text-101-Chat-Formatting-Bold-Italic-Underline-
#Write custom handler for db
#Set queue for async code

