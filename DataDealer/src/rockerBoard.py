#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import sys, os, math, datetime, sqlite3, operator, random
import phedexData, popDbData, dbApi

class rockerBoard():
	def __init__(self):
		phedexCache = os.environ['DATA_DEALER_PHEDEX_CACHE']
		popDbCache = os.environ['DATA_DEALER_POP_DB_CACHE']
		cacheDeadline = int(os.environ['DATA_DEALER_CACHE_DEADLINE'])
		self.rankingCachePath = os.environ['DATA_DEALER_RANKING_CACHE']
		self.threshold = int(os.environ['DATA_DEALER_THRESHOLD'])
		self.budget = int(os.environ['DATA_DEALER_BUDGET'])
		self.phedexData = phedexData.phedexData(phedexCache, cacheDeadline)
		self.popDbData = popDbData.popDbData(popDbCache, cacheDeadline)
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
		for i in range(1, 8):
			date = today - datetime.timedelta(days=i)
			cpuh = self.popDbData.getDatasetCpus(datasetName, date.strftime('%Y-%m-%d'))
			if not (cpuh == 0):
				if i == 1:
					popularity += cpuh
				elif i == 2:
					popularity += cpuh*math.log(i)
				else:
					popularity += cpuh**(1/math.log(i))
			else:
				if i == 1:
					return 0
		return popularity

	def rankingCache(self, datasetRankings, siteRankings):
		if not os.path.exists(self.rankingCachePath):
			os.makedirs(self.rankingCachePath)
		cacheFile = "%s/%s.db" % (self.rankingCachePath, "rankingCache")
		if os.path.isfile(cacheFile):
			os.remove(cacheFile)
		rankingCache = sqlite3.connect(cacheFile)
		with rankingCache:
			cur = rankingCache.cursor()
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
			datasets = self.phedexData.getDatasetsAtSite(siteName)
			rank = sum(datasetRankings[d] for d in datasets)
			siteRankings[siteName] = rank
		return siteRankings

	def getNewReplicas(self, datasetRankings, siteRankings, systemLeft):
		subscriptions = dict()
		sizeSubscribedGb = 0
		maxRank = max(siteRankings.iteritems(), key=operator.itemgetter(1))[1]
		for siteName, rank in siteRankings.items():
			siteRankings[siteName] = maxRank - rank
		while (sizeSubscribedGb < self.budget and datasetRankings and sizeSubscribedGb < systemLeft):
			datasetName = max(datasetRankings.iteritems(), key=operator.itemgetter(1))[0]
			del datasetRankings[datasetName]
			siteName = self.weightedChoice(siteRankings)
			sizeSubscribedGb += self.phedexData.getDatasetSize(datasetName)
			if siteName in subscriptions:
				subscriptions[siteName].append(datasetName)
			else:
				subscriptions[siteName] = [datasetName]
		return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
	def rba(self, datasets, sites):
		datasetRankings = self.getDatasetRankings(datasets)
		siteRankings = self.getSiteRankings(sites, datasetRankings)
		self.rankingCache(datasetRankings, siteRankings)
		totalQuota = 0
		totalUsed = 0
		for siteName in sites:
			query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
			values = [siteName, "AnalysisOps"]
			data = self.dbApi.dbQuery(query, values=values)
			totalQuota += data[0][0]*10**3
			used = self.phedexData.getSiteStorage(siteName)
			totalUsed += used
		systemLeft = totalQuota*0.8 - totalUsed
		subscriptions = self.getNewReplicas(datasetRankings, siteRankings, systemLeft)
		return subscriptions
