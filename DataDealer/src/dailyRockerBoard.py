#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import os, re, sqlite3, ConfigParser, time, operator, datetime
from operator import itemgetter
import phedexData, crabApi, dbApi, popDbData

class dailyRockerBoard():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
        self.crabCachePath = config.get('data_dealer', 'crab_cache')
        self.threshold = config.getfloat('data_dealer', 'daily_threshold')
        self.limit = config.getint('data_dealer', 'daily_limit_gb')
        self.crab_time_limit = config.getint('data_dealer', 'crab_time_limit_s')
        self.crab_ratio = config.getfloat('data_dealer', 'crab_ratio')
        self.dbApi = dbApi.dbApi()
        self.phedexData = phedexData.phedexData()
        self.crabApi = crabApi.crabApi()
        self.popDbData = popDbData.popDbData()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def updateJobsCache(self, jobs):
        if not os.path.exists(self.crabCachePath):
            os.makedirs(self.crabCachePath)
        cacheFile = "%s/%s.db" % (self.crabCachePath, "crabCache")
        if os.path.isfile(cacheFile):
            os.remove(cacheFile)
        crabCache = sqlite3.connect(cacheFile)
        datasetRankings = dict()
        newDatasets = []
        with crabCache:
            cur = crabCache.cursor()
            cur.execute('CREATE TABLE Jobs (DatasetName TEXT, Timestamp INTEGER, User TEXT, NumJobs REAL, JobsLeft REAL)')
            cur.execute('CREATE TABLE Sites (SiteName TEXT UNIQUE)')
            for job in jobs:
                cur.execute('INSERT INTO Jobs(DatasetName, Timestamp, User, NumJobs, JobsLeft) VALUES(?, ?, ?, ?, ?)', (job[0], job[1], job[2], job[3], job[4]))
                datasetRankings[job[0]] = job[4]/job[3]
            cur.execute('SELECT DISTINCT DatasetName FROM Jobs WHERE (JobsLeft/NumJobs)>?', (self.crab_ratio,))
            for row in cur:
                newDatasets.append(row[0])
        return newDatasets

    def updateSitesCache(self, datasets):
        cacheFile = "%s/%s.db" % (self.crabCachePath, "crabCache")
        crabCache = sqlite3.connect(cacheFile)
        invalidSites = []
        for dataset in datasets:
            sites = self.phedexData.getSitesWithDataset(dataset)
            for site in sites:
                invalidSites.append((site,))
        with crabCache:
            cur = crabCache.cursor()
            cur.executemany('INSERT OR IGNORE INTO Sites(SiteName) VALUES(?)', invalidSites)
            invalidSites = []
            cur.execute('SELECT * FROM Sites')
            for row in cur:
                invalidSites.append(row[0])
        return invalidSites

    def updateRankingsCache(self, datasets):
        if not os.path.exists(self.rankingsCachePath):
            os.makedirs(self.rankingsCachePath)
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        rankingsCache = sqlite3.connect(cacheFile)
        datasetRankings = dict()
        for dataset in datasets:
            rank = self.getPopularity(dataset)
            datasetRankings[dataset] = rank
        with rankingsCache:
            cur = rankingsCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS Datasets (DatasetName TEXT UNIQUE, Rank REAL)')
            for datasetName, rank in datasetRankings.items():
                cur.execute('INSERT OR REPLACE INTO Datasets(DatasetName, Rank) VALUES(?, ?)', (datasetName, rank))

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
        recentSubscriptions = []
        requestTimestamp = datetime.datetime.fromtimestamp(int(time.time()) - 60*60*24*14)
        query = "SELECT DISTINCT Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Requests.Date>=%s AND Requests.RequestType=%s"
        values = [requestTimestamp, 0]
        data = self.dbApi.dbQuery(query, values=values)
        for row in data:
            recentSubscriptions.append(row[0])
        user_re = re.compile('^sciaba$')
        dataset_re = re.compile('^/GenericTTbar/HC-.*/GEN-SIM-RECO$')
        timestamp = int(time.time()) - self.crab_time_limit
        crabQueue = []
        query = 'TaskType =?= "ROOT" && JobStatus =?= 2 && QDate < %d' % (timestamp)
        attributes = ["CRAB_InputData", "QDate", "CRAB_UserHN", "CRAB_JobCount", "DAG_NodesQueued"]
        data = self.crabApi.crabCall(query, attributes)
        for classAd in data:
            if dataset_re.match(classAd.get("CRAB_InputData")) or user_re.match(classAd.get("CRAB_UserHN")):
                continue
            crabQueue.append((classAd.get("CRAB_InputData"), classAd.get("QDate"), classAd.get("CRAB_UserHN"), classAd.get("CRAB_JobCount"), classAd.get("DAG_NodesQueued")))
        jobs = [job for job in crabQueue if (job[0] in datasets) and (job[0] not in recentSubscriptions)]
        jobs.sort(key=itemgetter(1))
        return jobs

    def getSiteRankings(self, sites):
        newSites = dict()
        for siteName in sites:
            query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
            values = [siteName, "AnalysisOps"]
            data = self.dbApi.dbQuery(query, values=values)
            quota = data[0][0]*10**3
            used = self.phedexData.getSiteStorage(siteName)
            left = quota*self.threshold - used
            newSites[siteName] = left
        return newSites

    def getNewReplicas(self, datasets, sites):
        subscriptions = dict()
        subscribedGb = 0
        for datasetName in datasets:
            site = max(sites.iteritems(), key=operator.itemgetter(1))
            siteName = site[0]
            siteRank = site[1]
            datasetSizeGb = self.phedexData.getDatasetSize(datasetName)
            if (datasetSizeGb > siteRank) or (datasetSizeGb + subscribedGb > self.limit):
                print " ALERT -- Dataset %s (%d GB) was too big to subscribe" % (datasetName, datasetSizeGb)
                continue
            subscribedGb += datasetSizeGb
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def dailyRba(self, datasets, sites):
        self.popDbData.buildDSStatInTimeWindowCache(sites)
        jobs = self.getDatasetRankings(datasets)
        newDatasets = self.updateJobsCache(jobs)
        self.updateRankingsCache(newDatasets)
        invalidSites = self.updateSitesCache(newDatasets)
        sites = [site for site in sites if site not in invalidSites]
        newSites = self.getSiteRankings(sites)
        subscriptions = self.getNewReplicas(newDatasets, newSites)
        return subscriptions
