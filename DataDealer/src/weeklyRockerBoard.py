#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import sys, os, math, datetime, sqlite3, operator, random, ConfigParser
import phedexData, popDbData, dbApi

class weeklyRockerBoard():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.cfg')
        self.rankingsCachePath = config.get('DataDealer', 'cache')
        self.budget = config.getint('DataDealer', 'budget')
        self.lowerBudget = config.getint('DataDealer', 'lower_budget')
        self.lowerThreshold = config.getfloat('DataDealer', 'lower_threshold')
        self.upperThreshold = config.getfloat('DataDealer', 'upper_threshold')
        self.limit = config.getfloat('DataDealer', 'limit')
        self.upperLimit = config.getfloat('DataDealer', 'upper_limit')
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
        date = today - datetime.timedelta(days=1)
        cpuh1 = self.popDbData.getDatasetCpus(datasetName, date.strftime('%Y-%m-%d'))
        for i in range(2, 15):
            date = today - datetime.timedelta(days=i)
            cpuh2 = self.popDbData.getDatasetCpus(datasetName, date.strftime('%Y-%m-%d'))
            popularity += cpuh1 - cpuh2
            cpuh1 = cpuh2
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

    def getNewReplicas(self, datasetRankings, siteRankings, totalQuota, totalUsed):
        subscriptions = dict()
        sizeSubscribedGb = 0
        maxRank = max(siteRankings.iteritems(), key=operator.itemgetter(1))[1]
        for siteName, rank in siteRankings.items():
            siteRankings[siteName] = maxRank - rank
        dataset = max(datasetRankings.iteritems(), key=operator.itemgetter(1))
        while (datasetRankings):
            datasetName = dataset[0]
            datasetSizeGb = self.phedexData.getDatasetSize(datasetName)
            if sizeSubscribedGb + datasetSizeGb > self.budget:
                break
            del datasetRankings[datasetName]
            siteRank = siteRankings
            invalidSites = self.phedexData.getSitesWithDataset(datasetName)
            for siteName in invalidSites:
                if siteName in siteRank:
                    del siteRank[siteName]
            siteName = self.weightedChoice(siteRank)
            sizeSubscribedGb += datasetSizeGb
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
            dataset = max(datasetRankings.iteritems(), key=operator.itemgetter(1))
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def weeklyRba(self, datasets, sites):
        subscriptions = []
        datasetRankings = self.getDatasetRankings(datasets)
        for i in range(10):
            dataset = max(datasetRankings.iteritems(), key=operator.itemgetter(1))
            print str(dataset[1]) + " " + str(dataset[0])
            del datasetRankings[dataset[0]]
        return subscriptions
        siteRankings = self.getSiteRankings(sites, datasetRankings)
        self.rankingsCache(datasetRankings, siteRankings)
        totalQuota = 0
        totalUsed = 0
        for siteName in sites:
            query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
            values = [siteName, "AnalysisOps"]
            data = self.dbApi.dbQuery(query, values=values)
            totalQuota += data[0][0]*10**3
            used = self.phedexData.getSiteStorage(siteName)
            totalUsed += used
        subscriptions = self.getNewReplicas(datasetRankings, siteRankings, totalQuota, totalUsed)
        return subscriptions
