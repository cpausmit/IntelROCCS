#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Subscribes selected datasets
#---------------------------------------------------------------------------------------------------
import os, datetime, sqlite3, ConfigParser
import dbApi, phedexApi

class subscribe():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'intelroccs.cfg'))
        self.rankingsCachePath = config.get('DataDealer', 'cache')
        self.phedexApi = phedexApi.phedexApi()
        self.dbApi = dbApi.dbApi()

    def createSubscriptions(self, subscriptions):
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        for siteName in iter(subscriptions):
            datasets, subscriptionData = self.phedexApi.createXml(datasets=subscriptions[siteName], instance='prod')
            if not datasets:
                continue
            comment = "IntelROCCS DataDealer - For more information about this subscription see http://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/data_dealer-latest.report"
            jsonData = []
            for attempt in range(3):
                jsonData = self.phedexApi.subscribe(node=siteName, data=subscriptionData, level='dataset', move='n', custodial='n', group='AnalysisOps', request_only='n', no_mail='n', comments=comment, instance='prod')
                if not jsonData:
                    continue
                requestId = 0
                requestType = 0
                request = jsonData.get('phedex')
                try:
                    requestId = request.get('request_created')[0].get('id')
                except IndexError:
                    continue
                else:
                    break
            else:
                print("ERROR -- Failed to create subscription for datasets %s on site %s" % (str(subscriptions[siteName]), siteName))
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
