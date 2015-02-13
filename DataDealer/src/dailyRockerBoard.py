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
        oldDatasets = set(datasets)
        query = 'TaskType =?= "ROOT" && JobStatus =?= 1'
        attributes = ["CRAB_InputData"]
        data = self.crabApi.crabCall(query, attributes)
        for classAd in data:
            newDatasets.append(classAd.get("CRAB_InputData"))
        dSets = set(newDatasets)
        print dSets
        newDatasets = [dataset for dataset in dSets if dataset in oldDatasets]
        return newDatasets

    def getNewReplicas(self, datasets, sites):
        subscriptions = dict()
        invalidSites = []
        for datasetName in datasets:
            invalidSites.append(self.phedexData.getSitesWithDataset(datasetName))
        iSets = set(invalidSites)
        newSites = [site for site in sites if site not in invalidSites]
        for datasetName in datasets:
            siteName = random.choice(newSites)
            if siteName in subscriptions:
                subscriptions[siteName].append(datasetName)
            else:
                subscriptions[siteName] = [datasetName]
        return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
    def dailyRba(self, datasets, sites):
        newDatasets = self.getDatasets(datasets)
        print newDatasets
        subscriptions = self.getNewReplicas(newDatasets, sites)
        return subscriptions
