#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. It will run all other scripts and functions. It will
# generate a list of datasets to replicate and on which site to replicate onto and perform the
# replication.
# 
# At the end a summary is emailed out with what was done during the run.
#---------------------------------------------------------------------------------------------------
import sys, os, subprocess, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import datasetRanker, siteRanker

# Setup parameters
# We would like to make these easier to change in the future
threshold = 100 # TODO : Find threshold
budgetGb = 10000 # TODO : Decide on a budget
#===================================================================================================
#  M A I N
#===================================================================================================
# Get dataset rankings
datasetRanker = datasetRanker.datasetRanker()
datasetRankings = datasetRanker.getDatasetRankings(threshold)

# Get site rankings
#siteRanker = siteRanker.siteRanker()
#siteRankings = siteRanker.getSiteRankings()

# Select datasets and sites for subscriptions
# subscriptions = dict()
# while (selectedGB < budgetGB) and (sortedDatasetRankings):
# 	dataset, rank = select.weightedChoice(sortedDatasetRankings)
# 	site = select.weightedChoice(sortedSiteRankings)
# 	if site in subscriptions:
# 		subscriptions[site].append(dataset)
# 	else:
# 		subscriptions[site] = [dataset]
# 	sortedDatasetRankings.remove((dataset, rank))
# print subscriptions

# create subscriptions
# for site in iter(subscriptions):
# 	data = self.phdx.xmlData(subscriptions[site])
	# TODO : Improve comments
	# TODO : Check for errors
	#json_data = self.phdx.subscribe(node=site, data=data, level='file', group='AnalysisOps', request_only='y', comments='IntelROCCS DataDealer')
	# TODO : Insert subscription into db
	#self.updatedb(json_data)

# Send summary report
# TODO : Send daliy report

# DONE
sys.exit(0)
