#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Subscribes selected datasets
#---------------------------------------------------------------------------------------------------
import sys, os, datetime, sqlite3
import dbApi, phedexApi

class subscribe():
	def __init__(self):
		self.rankingsCachePath = os.environ['DATA_DEALER_RANKINGS_CACHE']
		self.phedexApi = phedexApi.phedexApi()
		self.dbApi = dbApi.dbApi()

	def createSubscriptions(self, subscriptions):
		cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
		for siteName in iter(subscriptions):
			datasets, subscriptionData = self.phedexApi.createXml(datasets=subscriptions[siteName], instance='prod')
			if not datasets:
				continue
			jsonData = []
			jsonData = self.phedexApi.subscribe(node=siteName, data=subscriptionData, level='dataset', move='n', custodial='n', group='AnalysisOps', request_only='n', no_mail='n', comments='IntelROCCS DataDealer', instance='prod')
			if not jsonData:
				continue
			requestId = 0
			requestType = 0
			request = jsonData.get('phedex')
			try:
				requestId = request.get('request_created')[0].get('id')
			except IndexError, e:
				print(" ERROR -- Failed to create subscription for datasets %s on site %s" % (str(subscriptions[siteName]), siteName))
				continue
			requestTimestamp = datetime.datetime.fromtimestamp(float(request.get('request_timestamp')))
			for datasetName in datasets:
				rankingsCache = sqlite3.connect(cacheFile)
				with rankingsCache:
					cur = rankingsCache.cursor()
					cur.execute('SELECT Rank FROM Datasets WHERE DatasetName=?', (datasetName,))
					row = cur.fetchone()
					datasetRank = row[0]
				groupName = "AnalysisOps"
				query = "INSERT INTO Requests(RequestId, RequestType, DatasetId, SiteId, GroupId, Rank, Date) SELECT %s, %s, Datasets.DatasetId, Sites.SiteId, Groups.GroupId, %s, %s FROM Datasets, Sites, Groups WHERE Datasets.DatasetName=%s AND Sites.SiteName=%s AND Groups.GroupName=%s"
				values = [requestId, requestType, datasetRank, requestTimestamp, datasetName, siteName, groupName]
				self.dbApi.dbQuery(query, values=values)
