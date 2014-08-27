#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, copy
import init
import datasetRanker, siteRanker, selection, subscribe, subscriptionReport
import phedexApi, popDbApi

# Setup parameters
# We would like to make these easier to change in the future
phedexCache = os.environ['PHEDEX_CACHE']
popDbCache = os.environ['POP_DB_CACHE']
cacheDeadline = os.environ['CACHE_DEADLINE']

# phedexApi_ = phedexApi.phedexApi()
# phedexApi_.renewProxy()

# popDbApi_ = popDbApi.popDbApi()
# popDbApi_.renewSsoCookie()

#===================================================================================================
#  M A I N
#===================================================================================================
# Get dataset rankings
print "Dataset Ranking --- Start"
# datasetRanker_ = datasetRanker.datasetRanker()
# datasetRankings = datasetRanker_.getDatasetRankings()
# datasetRankingsCopy = copy.deepcopy(datasetRankings)
print "Dataset Ranking --- Stop"

# Get site rankings
print "Site Ranking --- Start"
# siteRanker_ = siteRanker.siteRanker()
# siteRankings = siteRanker_.getSiteRankings()
print "Site Ranking --- Stop"

# Select datasets and sites for subscriptions
print "Select Subscriptions --- Start"
# selection_ = selection.selection()
# subscriptions = selection_.selectSubscriptions(datasetRankings, siteRankings)
print "Select Subscriptions --- Stop"

# subscribe selected datasets
print "Subscribe --- Start"
#subscribe_ = subscribe.subscribe()
#subscribe_.createSubscriptions(subscriptions)
print "Subscribe --- Stop"

# Send summary report
print "Daily email --- Start"
subscriptionReport_ = subscriptionReport.subscriptionReport()
subscriptionReport_.createReport()
print "Daily email --- Stop"

# DONE
sys.exit(0)
