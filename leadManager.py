from attr import define

@define
class LeadManager:
    """
    Object for persisting parameters of lead searching and storing responses
    """
    searchStorage: list = []
    companyStorage: list = []
    officerStorage: list = []