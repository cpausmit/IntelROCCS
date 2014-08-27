#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, copy, sqlite3, datetime
import init
import datasetRanker, siteRanker, selection, subscribe, subscriptionReport
import phedexApi, popDbApi, dbApi

# Setup parameters
# We would like to make these easier to change in the future
phedexCache = os.environ['PHEDEX_CACHE']
popDbCache = os.environ['POP_DB_CACHE']
cacheDeadline = os.environ['CACHE_DEADLINE']

dbApi_ = dbApi.dbApi()
requests = []
requestsDb = sqlite3.connect("%s/requests.db" % (os.environ['HOME']))
with requestsDb:
	cur = requestsDb.cursor()
	cur.execute('SELECT RequestId, RequestType, DatasetName, SiteName, GroupName, Rank, Timestamp FROM Requests')
	for row in cur:
		requests.append(row)

for request in requests:
	requestId = request[0]
	requestType = request[1]
	datasetName = request[2]
	siteName = request[3]
	groupName = request[4]
	datasetRank = request[5]
	date = datetime.datetime.fromtimestamp(float(request[6]))
	query = "UPDATE Requests, Datasets SET Requests.Date=%s WHERE Requests.RequestId=%s AND Requests.DatasetId=Datasets.DatasetId AND Datasets.DatasetName=%s AND Requests.RequestType=%s"
	values = [date, requestId, datasetName, requestType]
	dbApi_.dbQuery(query, values=values)

sys.exit(0)

phedexApi_ = phedexApi.phedexApi()
phedexApi_.renewProxy()

popDbApi_ = popDbApi.popDbApi()
popDbApi_.renewSsoCookie()

#===================================================================================================
#  M A I N
#===================================================================================================
# Get dataset rankings
print "Dataset Ranking --- Start"
datasetRanker_ = datasetRanker.datasetRanker()
datasetRankings = datasetRanker_.getDatasetRankings()
datasetRankingsCopy = copy.deepcopy(datasetRankings)
print "Dataset Ranking --- Stop"

# Get site rankings
print "Site Ranking --- Start"
siteRanker_ = siteRanker.siteRanker()
siteRankings = siteRanker_.getSiteRankings()
print "Site Ranking --- Stop"

# Select datasets and sites for subscriptions
print "Select Subscriptions --- Start"
selection_ = selection.selection()
subscriptions = selection_.selectSubscriptions(datasetRankings, siteRankings)
print "Select Subscriptions --- Stop"

# subscribe selected datasets
print "Subscribe --- Start"
subscribe_ = subscribe.subscribe()
subscribe_.createSubscriptions(subscriptions)
print "Subscribe --- Stop"

# Send summary report
print "Daily email --- Start"
subscriptionReport_ = subscriptionReport.subscriptionReport()
subscriptionReport_.createReport()
print "Daily email --- Stop"

# DONE
sys.exit(0)
