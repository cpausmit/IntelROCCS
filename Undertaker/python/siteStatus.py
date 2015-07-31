#====================================================================================================
#  C L A S S E S  concerning the site status
#====================================================================================================

#---------------------------------------------------------------------------------------------------
"""
Class:  SiteStatus(siteName='')
Stores flags for each site.
"""
#---------------------------------------------------------------------------------------------------
class SiteStatus:
    "A SiteStatus defines the ststua of the site."

    def __init__(self, siteName):
        self.name = siteName
        # do we want to use this site and is retrieved from Quotas table (SiteStorage)?
        self.status = 0
        # will be set to 0 if information for site is corrupted, do not issue delete requests
        self.valid = 1
        self.size = 0
        self.siteId = 0

    def setStatus(self,status):
        self.status = status
    def setValid(self,valid):
        self.valid = valid
    def setSize(self,size):
        self.size = size
    def setId(self,id):
        self.siteId = id

    def getStatus(self):
        return self.status
    def getValid(self):
        return self.valid
    def getSize(self):
        return self.size
    def getId(self):
        return self.siteId
