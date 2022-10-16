from datetime import timedelta, date

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