#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Subscribes selected datasets
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
import dbApi, phedexApi, phedexData

class subscribe():
	def __init__(self):
		phedexCache = os.environ['PHEDEX_CACHE']
		cacheDeadline = int(os.environ['CACHE_DEADLINE'])
		self.phedexData = phedexData.phedexData(phedexCache, cacheDeadline)
		self.phedexApi = phedexApi.phedexApi()
		self.dbApi = dbApi.dbApi()

	def createSubscriptions(self, subscriptions):
		for siteName in iter(subscriptions):
			datasets, subscriptionData = self.phedexApi.createXml(datasets=subscriptions[siteName], instance='prod')
			if not datasets:
				continue
			jsonData = []
			jsonData = self.phedexApi.subscribe(node=siteName, data=subscriptionData, level='dataset', move='n', custodial='n', group='AnalysisOps', request_only='n', no_mail='n', comments='IntelROCCS DataDealer', instance='prod')
			if not jsonData:
				continue
			requestType = 0
			requestId = 0
			request = jsonData.get('phedex')
			try:
				requestId = request.get('request_created')[0].get('id')
			except IndexError, e:
				print(" ERROR -- Failed to create subscription for datasets %s on site %s" % (str(subscriptions[siteName]), siteName))
				continue
			requestTimestamp = int(request.get('request_timestamp'))
			for datasetName in datasets:
				datasetRank = datasetRankingsCopy[datasetName]
				replicas = phedexDb.getNumberReplicas(datasetName)
				accesses = popDbDb.getDatasetAccesses(datasetName, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
				sizeGb = self.phedexData.getDatasetSize(datasetName)
				query = "INSERT INTO Requests(RequestId, RequestType, DatasetId, SiteId, SizeGb, Replicas, Accesses, Rank, Timestamp) SELECT %s, %s, Datasets.DatasetId, Sites.SiteId, %s, %s, %s, %s, %s FROM Datasets, Sites WHERE Datasets.DatasetName=%s AND Sites.SiteName=%s"
				values = [requestId, requestType, sizeGb, replicas, accesses, datasetRank, requestTimestamp, datasetName, siteName]
				self.dbApi.dbQuery(query, values=values)