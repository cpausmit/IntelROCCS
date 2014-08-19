#----------------------------------------------------------------------------------------------------
#
# This class provides support for storing datasets information extracted from Popularity service
#
#----------------------------------------------------------------------------------------------------
import time, datetime

class UsedDataset:

    def __init__(self, dataset):
        self.dataset = dataset
        self.siteNames = {}
        self.lastUsedAtSite = {}
        self.timesUsedAtSite = {}

    def updateForSite(self,site,utime,nacc):
        if site not in self.siteNames:
            self.siteNames[site] = 1

        if site not in self.lastUsedAtSite:
            self.lastUsedAtSite[site] = utime
        else:
            prepoch = int(time.mktime(time.strptime(self.lastUsedAtSite[site], "%Y-%m-%d")))
            now = int(time.mktime(time.strptime(utime, "%Y-%m-%d")))
            if now > prepoch:
                self.lastUsedAtSite[site] = utime

        if site not in self.timesUsedAtSite:
            self.timesUsedAtSite[site] = nacc
        else:
            self.timesUsedAtSite[site] =  self.timesUsedAtSite[site] + nacc

    def locatedOnSites(self):
        return siteNames.keys()

    def isOnSite(self,site):
        if site in self.siteNames:
            return True
        return False

    def lastUsed(self,site):
        return self.lastUsedAtSite[site]

    def timesUsed(self,site):
        return self.timesUsedAtSite[site]
