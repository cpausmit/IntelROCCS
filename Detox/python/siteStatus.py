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
        self.valid = {}
        self.size = {}
        self.siteId = 0

    def setStatus(self,status):
        self.status = status
    def setValid(self,valid,group):
        self.valid[group] = valid
    def setValidAll(self,valid):
        for group in self.valid:
            self.valid[group] = valid
    def setSize(self,size,group):
        self.size[group] = size
        if size > 0:
            self.valid[group] = 1
        else:
            self.valid[group] = 0
    def setId(self,id):
        self.siteId = id

    def getStatus(self):
        return self.status
    def getValid(self,groups):
        for grp in groups:
            if grp in self.valid:
                if self.valid[grp] == 0:
                    return 0
            else:
                return 0
        return 1
    def isValidAny(self):
        for group in self.valid:
            if self.valid[group] == 1:
                return 1
        return 0
    def hasDefinedGroup(self,group):
         phedGroups = group.split('+')
         if group in phedGroups:
             return True
         return False
    def getSize(self,group):
        phedGroups = group.split('+')
        size = 0.0
        for grp in phedGroups:
            size = size + self.size[grp]
        return size
    def getId(self):
        return self.siteId
