import asyncio
from datetime import timedelta, date
from os.path import exists
from os import makedirs
from os.path import join
from typing import Union

def getDaysDelta(lastLeadDate: date):
    delta = date.today() - lastLeadDate
    return delta.days + 1 #adding 1 to cover overlapping day as well!

def createSearchDates(daysBack):
    return [date.today() - timedelta(days = x) for x in range(daysBack)]

def createParams(headerBase: dict, day: date):
    output = headerBase
    output["incorporated_from"] = str(day)
    output["incorporated_to"] = str(day)
    return output

def softDirCreate(path: str) -> Union[None, Exception]:
    """
    Checks if directory exists and creates if not
    """ 
    try:
        if not exists(path):
            makedirs(path)
        return None
    except Exception as e:
        return e

