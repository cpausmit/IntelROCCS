#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. It will run all other scripts and functions. It will
# generate a list of datasets to replicate and on which site to replicate onto and perform the
# replication.
# 
# At the end a summary is emailed out with what was done during the run.
#---------------------------------------------------------------------------------------------------
import sys, os, copy, subprocess, datetime, operator
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import datasetRanker, siteRanker, select
import IntelROCCS.Api.popDb.popDbApi as popDbApi
import IntelROCCS.Api.phedex.phedexApi as phedexApi

# Setup parameters
# We would like to make these easier to change in the future
threshold = 1 # TODO : Find threshold
budgetGb = 10000 # TODO : Decide on a budget
popDbApi = popDbApi.popDbApi()
phedexApi = phedexApi.phedexApi()
popDbApi.renewSSOCookie()

#===================================================================================================
#  M A I N
#===================================================================================================
# Get dataset rankings
datasetRanker = datasetRanker.datasetRanker(threshold)
datasetRankings = datasetRanker.getDatasetRankings()
datasetRankingsCopy = copy.deepcopy(datasetRankings)

# Get site rankings
siteRanker = siteRanker.siteRanker()
siteRankings = siteRanker.getSiteRankings()

# Select datasets and sites for subscriptions
select = select.select()
subscriptions = dict()
selectedGb = 0
while (selectedGb < budgetGb) and (datasetRankings):
	datasetName = select.weightedChoice(datasetRankings)
	siteName = select.weightedChoice(siteRankings)
	if siteName in subscriptions:
		subscriptions[siteName].append(datasetName)
	else:
		subscriptions[siteName] = [datasetName]
	del datasetRankings[datasetName]

# create subscriptions
for siteName in iter(subscriptions):
	print siteName
	print subscriptions[siteName]
 	subscriptionData = phedexApi.createXml(subscriptions[siteName], instance='prod')
	jsonData = phedexApi.subscribe(node=siteName, data=subscriptionData, level='dataset', move='n', custodial='n', group='AnalysisOps', request_only='y', no_mail='n', comments='IntelROCCS DataDealer', instance='prod')
	requestId = jsonData.get('phedex').get('request_created')[0].get('id')
	print "Request Id : " + str(requestId)
	print jsonData
	print "Site : " + str(siteName)
	#requestTime = jsonData.get('phedex').get('request_created')[0].get('id')
	for datasetName in subscriptions[siteName]:
		# Insert into database
		# Info: requestId, requestType(0), siteName, datasetName, rank, groupName('AnalysisOps'), requestTime
		print "Dataset : " + str(datasetName)
		print "Rank : " + str(datasetRankingsCopy[datasetName])
	#self.updatedb(json_data)

# Send summary report
# TODO : Send daliy report

# DONE
sys.exit(0)
