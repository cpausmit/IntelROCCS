#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, copy
import init
import datasetRanker, siteRanker, selection, subscribe, subscriptionReport
import phedexApi, popDbApi

# initialize
phedexApi_ = phedexApi.phedexApi()
phedexApi_.renewProxy()

popDbApi_ = popDbApi.popDbApi()
popDbApi_.renewSsoCookie()

#===================================================================================================
#  M A I N
#===================================================================================================
# get dataset rankings
print "Dataset Ranking --- Start"
datasetRanker_ = datasetRanker.datasetRanker()
datasetRankings = datasetRanker_.getDatasetRankings()
datasetRankingsCopy = copy.deepcopy(datasetRankings)
print "Dataset Ranking --- Stop"

# get site rankings
print "Site Ranking --- Start"
siteRanker_ = siteRanker.siteRanker()
siteRankings = siteRanker_.getSiteRankings()
print "Site Ranking --- Stop"

# select datasets and sites for subscriptions
print "Select Subscriptions --- Start"
selection_ = selection.selection()
subscriptions = selection_.selectSubscriptions(datasetRankings, siteRankings)
print "Select Subscriptions --- Stop"

# subscribe selected datasets
print "Subscribe --- Start"
subscribe_ = subscribe.subscribe()
subscribe_.createSubscriptions(subscriptions, datasetRankingsCopy)
print "Subscribe --- Stop"

# Send summary report
print "Daily email --- Start"
subscriptionReport_ = subscriptionReport.subscriptionReport()
subscriptionReport_.createReport()
print "Daily email --- Stop"

# DONE
sys.exit(0)
