#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import sites, popDbDb
import IntelROCCS.Api.db.dbApi as dbApi

class siteRanker():
    def __init__(self):
        self.sites = sites.sites()
        self.phedexDb = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
        self.popDb = popDbDb.popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)
        self.dbApi = dbApi.dbApi()
#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getMaxCpu(self, site):
        cpu = []
        for i in range(22):
            date = datetime.date.today() - datetime.timedelta(days=i+1)
            usedCpu = getSiteCpu(site, date.strftime('%Y-%m-%d'))
            cpu.append((site, usedCpu))
        cpu = sorted(cpu, reverse=True, key=itemgetter(1))
        maxCpu = 0
        for i in range(3):
            maxCpu += cpu[i][2]
        maxCpu = maxCpu/3
        return maxCpu
    
    def getMaxStorage(self, site):
        cpu = []
        query = "SELECT SizeTb FROM Quotas WHERE GroupName=%s AND SiteName=%s"
        values = ['AnalysisOps', site]
        data = self.dbApi.dbQuery(query, values=values)
        maxStorage = data[0]
        return maxStorage

    def getAvailableCpu(self, site):
        date = datetime.date.today() - datetime.timedelta(days=1)
        maxCpu = getMaxCpu(site)
        usedCpu = getSiteCpu(site, date.strftime('%Y-%m-%d'))
        availableCpu = maxCpu - usedCpu
        return availableCpu

    def getAvailableStorage(self, site):
        maxStorage = getMaxStorage(site)
        usedStorage = self.phedexDb.getSiteStorage(site)
        availableCpu = maxCpu - usedCpu
        return availableCpu

    def getSiteRankings(self):
        # rank = availableSiteStorageGb * availableSiteCpu
        siteRankings = dict()
        availableSites = sites.getAvailableSites()
        for siteName in availableSites:
            availableCpu = getAvailableCpu(siteName)
            availableStorageGb = getAvailableStorage(siteName)
            rank = (availableCpu*availableStorageGb)
            siteRankings[SiteName] = rank
        return siteRankings

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./siteRanking.py"
        sys.exit(2)
    siteRanker = siteRanker()
    data = siteRanker.getSiteRankings()
    print data
    sys.exit(0)
