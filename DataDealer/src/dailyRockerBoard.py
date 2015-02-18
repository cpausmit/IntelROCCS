#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import os, random, sqlite3, ConfigParser, time
import phedexData, crabApi

class dailyRockerBoard():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.cfg')
        self.rankingsCachePath = config.get('DataDealer', 'cache')
        self.phedexData = phedexData.phedexData()
        self.crabApi = crabApi.crabApi()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def updateCache(self, datasets, sites):
        if not os.path.exists(self.rankingsCachePath):
            os.makedirs(self.rankingsCachePath)
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        rankingsCache = sqlite3.connect(cacheFile)
        with rankingsCache:
            cur = rankingsCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Datasets (DatasetName TEXT UNIQUE, Rank REAL)')
            cur.execute('CREATE TABLE IF NOT EXISTS Sites (SiteName TEXT UNIQUE, Rank REAL)')
            for datasetName in datasets:
                cur.execute('INSERT OR REPLACE INTO Datasets(DatasetName, Rank) VALUES(?, ?)', (datasetName, 10))
            for siteName in sites:
                cur.execute('INSERT OR REPLACE INTO Sites(SiteName, Rank) VALUES(?, ?)', (siteName, 0))

    def getDatasets(self, datasets):
        timestamp = int(time.time()) - 86400
        newDatasets = []
        oldDatasets = set(datasets)
        query = 'TaskType =?= "ROOT" && JobStatus =?= 2 && QDate < %d' % (timestamp)
        attributes = ["CRAB_InputData"]
        data = self.crabApi.crabCall(query, attributes)
        for classAd in data:
            newDatasets.append(classAd.get("CRAB_InputData"))
        dSets = set(newDatasets)
        newDatasets = [dataset for dataset in dSets if dataset in oldDatasets]
        return newDatasets

    def getNewReplicas(self, datasets, sites):
        subscriptions = dict()
        invalidSites = []
        for datasetName in datasets:
            invalidSites.append(self.phedexData.getSitesWithDataset(datasetName))
        newSites = [site for site in sites if site not in invalidSites]
        for datasetName in datasets:
            siteName = random.choice(newSites)
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def dailyRba(self, datasets, sites):
        newDatasets = self.getDatasets(datasets)
        self.updateCache(newDatasets, sites)
        subscriptions = self.getNewReplicas(newDatasets, sites)
        return subscriptions
