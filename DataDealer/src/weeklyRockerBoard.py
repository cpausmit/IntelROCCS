#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import os, datetime, sqlite3, operator, random, ConfigParser
import phedexData, popDbData, dbApi

class weeklyRockerBoard():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
        self.rankingsCachePath = config.get('data_dealer', 'rankings_cache')
        self.threshold = config.getfloat('data_dealer', 'weekly_threshold')
        self.limit = config.getint('data_dealer', 'weekly_limit_gb')
        self.phedexData = phedexData.phedexData()
        self.popDbData = popDbData.popDbData()
        self.dbApi = dbApi.dbApi()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def rankingsCache(self, datasetRankings, siteRankings):
        if not os.path.exists(self.rankingsCachePath):
            os.makedirs(self.rankingsCachePath)
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        rankingsCache = sqlite3.connect(cacheFile)
        with rankingsCache:
            cur = rankingsCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Datasets (DatasetName TEXT UNIQUE, Rank REAL)')
            cur.execute('CREATE TABLE IF NOT EXISTS Sites (SiteName TEXT UNIQUE, Rank REAL)')
            for datasetName, rank in datasetRankings.items():
                cur.execute('INSERT OR REPLACE INTO Datasets(DatasetName, Rank) VALUES(?, ?)', (datasetName, rank))
            for siteName, rank in siteRankings.items():
                cur.execute('INSERT OR REPLACE INTO Sites(SiteName, Rank) VALUES(?, ?)', (siteName, rank))

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
        utcNow = datetime.datetime.utcnow()
        today = datetime.date(utcNow.year, utcNow.month, utcNow.day)
        accsOld = []
        for i in range(8, 15):
            date = today - datetime.timedelta(days=i)
            accsOld.append(self.popDbData.getDatasetAccesses(date.strftime('%Y-%m-%d'), datasetName))
        for i in range(1, 8):
            date = today - datetime.timedelta(days=i)
            accNew = self.popDbData.getDatasetAccesses(date.strftime('%Y-%m-%d'), datasetName)
            for accOld in accsOld:
                popularity += accNew - accOld
        return popularity

    def getDatasetRankings(self, datasets):
        alphaValues = dict()
        for datasetName in datasets:
            nReplicas = self.phedexData.getNumberReplicas(datasetName)
            sizeGb = self.phedexData.getDatasetSize(datasetName)
            popularity = self.getPopularity(datasetName)
            alpha = float(popularity)/float(nReplicas*sizeGb)
            alphaValues[datasetName] = alpha
        mean = (1./len(alphaValues))*sum(v for v in alphaValues.values())
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
            left = quota*self.threshold - used
            if left <= 0:
                continue
            siteQuotas[siteName] = left
        return siteQuotas

    def getNewReplicas(self, datasetRankings, siteRankings, siteQuotas):
        subscriptions = dict()
        validSites = siteQuotas.keys()
        allSites = siteRankings.keys()
        invalidSites = [site for site in allSites if site not in validSites]
        for site in invalidSites:
            del siteRankings[site]
        subscribedGb = 0
        while (datasetRankings):
            if not siteRankings:
                print " ALERT -- No more sites available"
                break
            dataset = max(datasetRankings.iteritems(), key=operator.itemgetter(1))
            datasetName = dataset[0]
            datasetRank = dataset[1]
            if datasetRank <= 0:
                print " ALERT -- Dataset %s didn't have a positive ranking" % (datasetName)
                break
            siteRanks = siteRankings
            invalidSites = self.phedexData.getSitesWithDataset(datasetName)
            for siteName in invalidSites:
                if siteName in siteRanks:
                    del siteRanks[siteName]
            if not siteRanks:
                print " ALERT -- Dataset %s have no available sites" % (datasetName)
                continue
            site = min(siteRanks.iteritems(), key=operator.itemgetter(1))
            siteName = site[0]
            datasetSizeGb = self.phedexData.getDatasetSize(datasetName)
            if siteQuotas[siteName] - datasetSizeGb <= 0:
                print " ALERT -- Dataset %s (%d GB) was too big to subscribe to site %s" % (datasetName, datasetSizeGb, siteName)
                del siteRankings[siteName]
                continue
            if subscribedGb + datasetSizeGb > self.limit:
                print " ALERT -- Couldn't subscribe dataset %s (%d GB) due to reached limit" % (datasetName, datasetSizeGb)
                break
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
            siteRankings[siteName] += datasetRank
            siteQuotas[siteName] -= datasetSizeGb
            subscribedGb += datasetSizeGb
            del datasetRankings[datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def weeklyRba(self, datasets, sites):
        self.popDbData.buildDSStatInTimeWindowCache(sites, datasets)
        subscriptions = []
        datasetRankings = self.getDatasetRankings(datasets)
        siteQuotas = self.getSiteQuotas(sites)
        siteRankings = self.getSiteRankings(sites, datasetRankings)
        self.rankingsCache(datasetRankings, siteRankings)
        subscriptions = self.getNewReplicas(datasetRankings, siteRankings, siteQuotas)
        return subscriptions
