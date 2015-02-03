#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import sys, os, math, datetime, sqlite3, operator, random, ConfigParser
import phedexData, popDbData, dbApi

class weeklyRockerBoard():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('intelroccs.cfg')
        self.rankingsCachePath = config.get('DataDealer', 'cache')
        self.limit = config.getfloat('DataDealer', 'weekly_limit')
        self.phedexData = phedexData.phedexData()
        self.popDbData = popDbData.popDbData()
        self.dbApi = dbApi.dbApi()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def weightedChoice(self, choices):
        total = sum(w for c, w in choices.items())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.items():
            if upto + w > r:
                return c
            upto += w

    def getPopularity(self, datasetName):
        popularity = 0
        today = datetime.date.today()
        cpusOld = []
        for i in range(8, 15):
            date = today - datetime.timedelta(days=i)
            cpusOld.append(self.popDbData.getDatasetCpus(datasetName, date.strftime('%Y-%m-%d')))
        for i in range(1, 8):
            date = today - datetime.timedelta(days=i)
            cpuNew = self.popDbData.getDatasetCpus(datasetName, date.strftime('%Y-%m-%d'))
            for cpuOld in cpusOld:
                popularity += cpuNew - cpuOld
        return popularity

    def rankingsCache(self, datasetRankings, siteRankings):
        if not os.path.exists(self.rankingsCachePath):
            os.makedirs(self.rankingsCachePath)
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        if os.path.isfile(cacheFile):
            os.remove(cacheFile)
        rankingsCache = sqlite3.connect(cacheFile)
        with rankingsCache:
            cur = rankingsCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Datasets (DatasetName TEXT UNIQUE, Rank REAL)')
            cur.execute('CREATE TABLE IF NOT EXISTS Sites (SiteName TEXT UNIQUE, Rank REAL)')
            for datasetName, rank in datasetRankings.items():
                cur.execute('INSERT INTO Datasets(DatasetName, Rank) VALUES(?, ?)', (datasetName, rank))
            for siteName, rank in siteRankings.items():
                cur.execute('INSERT INTO Sites(SiteName, Rank) VALUES(?, ?)', (siteName, rank))

    def getDatasetRankings(self, datasets):
        alphaValues = dict()
        for datasetName in datasets:
            nReplicas = self.phedexData.getNumberReplicas(datasetName)
            sizeGb = self.phedexData.getDatasetSize(datasetName)
            popularity = self.getPopularity(datasetName)
            alpha = popularity/float(nReplicas*sizeGb)
            alphaValues[datasetName] = alpha
        mean = (1/len(alphaValues))*sum(v for v in alphaValues.values())
        datasetRankings = dict()
        for k, v in alphaValues.items():
            dev = v - mean
            datasetRankings[k] = dev
        return datasetRankings

    def getSiteRankings(self, sites, datasetRankings):
        siteRankings = dict()
        for siteName in sites:
            datasets = self.phedexData.getAnalysisOpsDatasetsAtSite(siteName)
            rank = sum(datasetRankings[d] for d in datasets)
            siteRankings[siteName] = rank
        return siteRankings

    def getSiteQuotas(self, sites):
        siteQuotas = dict()
        for siteName in sites:
            query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
            values = [siteName, "AnalysisOps"]
            data = self.dbApi.dbQuery(query, values=values)
            quota = data[0][0]*10**3
            used = self.phedexData.getSiteStorage(siteName)
            left = quota*self.limit - used
            if left <= 0:
                continue
            siteQuotas[siteName] = left
        return siteQuotas

    def getNewReplicas(self, datasetRankings, siteRankings, siteQuotas):
        subscriptions = dict()
        while (datasetRankings):
            if not siteRankings:
                break
            dataset = max(datasetRankings.iteritems(), key=operator.itemgetter(1))
            datasetName = dataset[0]
            datasetRank = dataset[1]
            if datasetRank <= 0:
                break
            siteRanks = siteRankings
            invalidSites = self.phedexData.getSitesWithDataset(datasetName)
            for siteName in invalidSites:
                if siteName in siteRanks:
                    del siteRanks[siteName]
            if not siteRanks:
                continue
            site = min(siteRanks.iteritems(), key=operator.itemgetter(1))
            siteName =site[0]
            siteRank = site[1]
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
            siteRankings[siteName] += datasetRank
            datasetSizeGb = self.phedexData.getDatasetSize(datasetName)
            siteQuotas[siteName] -= datasetSizeGb
            if siteQuotas[siteName] <= 0:
                del siteRankings[siteName]
            del datasetRankings[datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def weeklyRba(self, datasets, sites):
        subscriptions = []
        datasetRankings = self.getDatasetRankings(datasets)
        siteQuotas = self.getSiteQuotas(sites)
        sites = siteQuotas.keys()
        siteRankings = self.getSiteRankings(sites, datasetRankings)
        self.rankingsCache(datasetRankings, siteRankings)
        subscriptions = self.getNewReplicas(datasetRankings, siteRankings, siteQuotas)
        return subscriptions
