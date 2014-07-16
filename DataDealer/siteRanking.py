#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import getSites

class siteRanking():
    def __init__(self):
        self.dbApi = dbApi.dbApi()
        # get sites
        getSites = getSites.getSites()
        availableSites = getSites.getAvailableSites()
        # get max values
        maxSiteCpu = getMaxSiteCpu() # Only do one site at a time?
        maxSiteStorageGb = getMaxSiteStorageGb()
        # get one day ago values
        # build site info
        self.siteInfo = self.buildSiteInfo(availableSites, oneDayAgoCpu, oneDayAgoStorageGb maxSiteCpu, maxSiteStorageGb)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def buildSiteInfo(self, availableSites, oneDayAgoCpu, oneDayAgoStorageGb, maxSiteCpu, maxSiteStorageGb):
        siteInfo = dict()
        for site in availableSites:
            availableSiteStorageGb = maxSiteStorageGb - oneDayAgoStorageGb
            availableSiteCpu = getMaxSiteCpu - oneDayAgoCpu
            info = {'availableSiteStorageGb':availableSiteStorageGb , 'availableSiteCpu':availableSiteCpu}
            siteInfo[site] = info
        return siteInfo

    def getMaxSiteStorageGb(self):
        query = "SELECT SiteName, SizeTb FROM Quotas WHERE GroupName=%s" # Name of site field column?
        values = ['AnalysisOps']
        data = self.dbApi.dbQuery(query, values=values)
        maxSiteStorage = dict()
        for site in data:
            maxSiteStorage[site[0]] = site[1]
        return maxSiteStorageGb

    def oneDayAgoStorageGb(self, phedexJsonData):
        # use phedex cache
        oneDayAgoStorageGb = int(used_space/10**9)
        space = quota - used_space
        return space

    def getCPU(self, site):
        # Get cpu as below for last 5 days
        # cpu = top3:max(cpu_usage)/3 - cpu_usage
        # Example: {'2014-06-18':cpu, '2014-06-17':cpu, '2014-06-16':cpu, '2014-06-15':cpu, '2014-06-14':cpu}
        cpus = dict()
        for i in range(1,6):
            tstart = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            tstop = tstart
            try:
                json_data = self.popdb.DSStatInTimeWindow(sitename=site, tstart=tstop, tstop=tstop)
            except Exception, e:
                return None
            cpu = 0
            data = json_data.get('DATA')
            if not data:
                cpu += 0
            for dataset in data:
                cpu += dataset.get('TOTCPU')
            cpus[tstart] = cpu
        return cpus

    def getSiteRankings(self):
        # rank = availableSiteStorageGb * availableSiteCpu
        siteRankings = dict()
        for siteName, info in self.siteInfo.iteritems():
            availableSiteCpu = info['availableSiteCpu']
            availableSiteStorageGb = info['availableSiteStorageGb']
            rank = (availableSiteCpu*availableSiteStorageGb)
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
    siteRanking = siteRanking()
    data = siteRanking.getSiteRankings()
    print data
    sys.exit(0)
