from logging import Logger
from re import U
from typing import Union

#Puttinng logger generator and other loggig utils here

class customLogger:
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
    
    def logIfError(self, errorMsg, exception, successMsg) -> Union[None, Exception]:
        """
        Logs exception if it is not none. Otherwise logs success
        """
        try:
            if exception is not None:
                self.logger.error(f"{errorMsg}: {exception}")
            else:
                self.logger.info(successMsg)
            return None
        except Exception as e:
            return e