#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, copy, sqlite3, subprocess, datetime, operator
import init
#import datasetRanker, siteRanker, select, phedexDb, popDbDb, subscriptionReport
#import datasetRanker
import phedexApi, popDbApi, phedexData, popDbData

# Setup parameters
# We would like to make these easier to change in the future
budgetGb = os.environ['DATA_DEALER_BUDGET']
phedexCache = os.environ['PHEDEX_CACHE']
popDbCache = os.environ['POP_DB_CACHE']
cacheDeadline = os.environ['CACHE_DEADLINE']

phedexData_ = phedexData.phedexData(phedexCache, cacheDeadline)
phedexApi_ = phedexApi.phedexApi()
#phedexApi_.renewProxy()

popDbData_ = popDbData.popDbData(popDbCache, cacheDeadline)
popDbApi_ = popDbApi.popDbApi(popDbCache, cacheDeadline)
#popDbApi_.renewSsoCookie()

sys.exit(0)
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
print "Subscriptions --- Start"
phedexCache = os.environ['PHEDEC_CACHE']
blockReplicasCache = "%s/blockReplicas.db" % (phedexCache)
blockReplicasDb = sqlite3.connect(blockReplicasCache)
select_ = select.select()
subscriptions = dict()
selectedGb = 0
while (selectedGb < budgetGb) and (datasetRankings):
	datasetName = select_.weightedChoice(datasetRankings)
	siteName = select_.weightedChoice(siteRankings)
	if siteName in subscriptions:
		subscriptions[siteName].append(datasetName)
	else:
		subscriptions[siteName] = [datasetName]
	del datasetRankings[datasetName]
	with blockReplicasDb:
		cur = blockReplicasDb.cursor()
		cur.execute('SELECT SizeGb FROM Datasets WHERE DatasetName=?', (datasetName,))
		sizeGb = cur.fetchone()[0]
	selectedGb += sizeGb
print "Subscriptions --- Stop"

print "Update DB --- Start"
# change to mit db
requestsDbPath = "%s/Cache" % (os.environ['INTELROCCS_BASE'])
requestsDbFile = "%s/requests.db" % (requestsDbPath)
requestsDbCon = sqlite3.connect(requestsDbFile)
phedexDb = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
popDbDb = popDbDb.popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)
datetime.date.today() - datetime.timedelta(days=1)
with requestsDbCon:
	cur = requestsDbCon.cursor()
	cur.execute('CREATE TABLE IF NOT EXISTS Requests (RequestId INTEGER, RequestType INTEGER, DatasetName TEXT, SiteName TEXT, SizeGb REAL, Replicas INTEGER, Accesses INTEGER, Rank REAL, GroupName TEXT, Timestamp TEXT)')

# create subscriptions
for siteName in iter(subscriptions):
	datasets, subscriptionData = phedexApi.createXml(datasets=subscriptions[siteName], instance='prod')
	if not datasets:
		with open(logFilePath, 'a') as logFile:
			logFile.write("%s DataDealer ERROR: Creating PhEDEx XML data failed for datasets %s on site %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(subscriptions[siteName]), siteName))
		continue
	jsonData = phedexApi.subscribe(node=siteName, data=subscriptionData, level='dataset', move='n', custodial='n', group='AnalysisOps', request_only='y', no_mail='n', comments='IntelROCCS DataDealer', instance='prod')
	if not jsonData:
		with open(logFilePath, 'a') as logFile:
			logFile.write("%s DataDealer ERROR: Failed to create subscription for datasets %s on site %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(subscriptions[siteName]), siteName))
		continue
	requestType = 0
	requestId = 0
	groupName = 'AnalysisOps'
	request = jsonData.get('phedex')
	try:
		requestId = request.get('request_created')[0].get('id')
	except IndexError, e:
		with open(logFilePath, 'a') as logFile:
			logFile.write("%s DataDealer ERROR: Failed to create subscription for datasets %s on site %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(subscriptions[siteName]), siteName))
		continue
	requestTimestamp = int(request.get('request_timestamp'))
	for datasetName in datasets:
		datasetRank = datasetRankingsCopy[datasetName]
		replicas = phedexDb.getNumberReplicas(datasetName)
		accesses = popDbDb.getDatasetAccesses(datasetName, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
		sizeGb = 0
		with phedexDbCon:
			cur = phedexDbCon.cursor()
			cur.execute('SELECT SizeGb FROM Datasets WHERE DatasetName=?', (datasetName,))
			sizeGb = cur.fetchone()[0]
			with requestsDbCon:
				cur = requestsDbCon.cursor()
				cur.execute('INSERT INTO Requests(RequestId, RequestType, DatasetName, SiteName, SizeGb, Replicas, Accesses, Rank, GroupName, Timestamp) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (requestId, requestType, datasetName, siteName, sizeGb, replicas, accesses, datasetRank, groupName, requestTimestamp))
print "Update DB --- Stop"

# Send summary report
print "Daily email --- Start"
subscriptionReport.subscriptionReport()
print "Daily email --- Stop"

# DONE
sys.exit(0)
