#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Subscribes selected datasets
#---------------------------------------------------------------------------------------------------
import sys, os, sqlite3, datetime
import dbApi, phedexApi

class subscribe():
	def __init__(self):
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
			requestId = 0
			requestType = 0
			request = jsonData.get('phedex')
			try:
				requestId = request.get('request_created')[0].get('id')
			except IndexError, e:
				print(" ERROR -- Failed to create subscription for datasets %s on site %s" % (str(subscriptions[siteName]), siteName))
				continue
			requestTimestamp = int(request.get('request_timestamp'))
			for datasetName in datasets:
				datasetRank = datasetRankingsCopy[datasetName]
				groupName = "AnalysisOps"
				#query = "INSERT INTO Requests(RequestId, RequestType, DatasetId, SiteId, GroupId, Rank, Timestamp) SELECT %s, %s, Datasets.DatasetId, Sites.SiteId, Groups.GroupId, %s, %s FROM Datasets, Sites, Groups WHERE Datasets.DatasetName=%s AND Sites.SiteName=%s AND Groups.GroupName=%s"
				#values = [requestId, requestType, datasetRank, requestTimestamp, datasetName, siteName, groupName]
				#self.dbApi.dbQuery(query, values=values)
				requestsDb = sqlite3.connect("%s/requests.db" % (os.environ['HOME']))
				with requestsDb:
					cur = requestsDb.cursor()
					cur.execute('INSERT INTO Requests(RequestId, DatasetName, SiteName, Rank, Timestamp) VALUES(?, ?, ?, ?, ?)', (requestId, datasetName, siteName, datasetRank, requestTimestamp))