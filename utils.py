from datetime import timedelta, date

def createSearchDates(daysBack):
        return [date.today() - timedelta(days = x) for x in range(daysBack)]