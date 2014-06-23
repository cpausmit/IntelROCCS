#====================================================================================================
#  C L A S S E S  for the database interface
#====================================================================================================

#---------------------------------------------------------------------------------------------------
"""
Class:  DbInterface
Each site will be fully described for our application in this class.
"""
#---------------------------------------------------------------------------------------------------
class Dbinterface:
    "Database interface for IntelROCCS."

    def __init__(self, siteName):
        self.name = siteName
