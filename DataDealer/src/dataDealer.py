#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, copy, sqlite3, subprocess, datetime, operator
import init
#import subscribe, subscriptionReport
import datasetRanker, siteRanker, select, subscribe, subscriptionReport
import phedexApi, popDbApi

# Setup parameters
# We would like to make these easier to change in the future
phedexCache = os.environ['PHEDEX_CACHE']
popDbCache = os.environ['POP_DB_CACHE']
cacheDeadline = os.environ['CACHE_DEADLINE']

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
print "Subscriptions --- Start"
select_ = select.select()
subscriptions = select_.selectSubscriptions(datasetRankings, siteRankings)
print "Subscriptions --- Stop"

print subscriptions
sys.exit(0)

print "Update DB --- Start"
subscribe_ = subscribe()
subscribe_.createSubscriptions(subscriptions)
print "Update DB --- Stop"

# Send summary report
print "Daily email --- Start"
subscriptionReport.subscriptionReport()
print "Daily email --- Stop"

# DONE
sys.exit(0)
