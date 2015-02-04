#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import sys, os, random
import phedexData, crabApi

class dailyRockerBoard():
    def __init__(self):
        self.phedexData = phedexData.phedexData()
        self.crabApi = crabApi.crabApi()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def getDatasets(self, datasets):
        newDatasets = []
        query = ""
        attributes = []
        data = self.crabApi.crabCall(query, attributes)
        newDatasets = []
        newDatasets = [dataset for dataset in newDatasets if dataset in datasets]
        return newDatasets

    def getNewReplicas(self, datasets, sites):
        subscriptions = dict()
        invalidSites = []
        for datasetName in datasets:
            invalidSites.append(self.phedexData.getSitesWithDataset(datasetName))
        sites = [site for site in sites if site not in invalidSites]
        for datasetName in datasets:
            siteName = random.choice(sites)
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def dailyRba(self, sites, datasets):
        newDatasets = self.getDatasets(datasets)
        subscriptions = self.getNewReplicas(datasetRankings)
        return subscriptions
