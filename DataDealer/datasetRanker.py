#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the Analysisops space.
#
# TODO: Currently don't handle errors in popDB or Phedex, or if no data is returned an IndexError
# is thrown which is currently not caught.
#---------------------------------------------------------------------------------------------------
import sys, os, math, json, datetime, itemgetter
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import phedexDb
import IntelROCCS.Api.popDb.getPopDbData as popDbData

class datasetRanker():
    def __init__(self):
        phedex = phedexDb.phedexDb(12)
        date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        oneDayAgoPopDbJsonData = popDb.getPopDbData("DSStatInTimeWindow", date)
        oneDayAgoAccesses = popDbJsonDataParser(oneDayAgoPopDbJsonData)
        date = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        twoDaysAgoPopDbJsonData = popDb.getPopDbData("DSStatInTimeWindow", date)
        twoDaysAgoAccesses = popDbJsonDataParser(twoDaysAgoPopDbJsonData)
        self.datasetInfo = self.buildDatasetInfo(phedexJsonData, oneDayAgoAccesses, twoDaysAgoAccesses)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def popDbJsonDataParser(self, popDbJsonData):
        datasetAccesses = dict()
        data = json_data.get('DATA')
        for dataset in data:
            datasetName = dataset.get('COLLNAME')
            nAccesses = dataset.get('NACC')
            datasetAccesses[datasetName] = nAccesses
        return datasetAccesses

    def buildDatasetInfo(self, phedexJsonData, oneDayAgoAccesses, twoDaysAgoAccesses):
        datasetInfo = dict()
        datasets = phedexJsonData.get('phedex').get('dataset')
        for dataset in datasets:
            datasetName = dataset.get('name')
            sizeGb = int(dataset.get('bytes')/10**9)
            replicas = 0
            for replica in dataset.get('block')[0].get('replica'):
                replicas += 1
            nAccesses = 1
            if datasetName in oneDayAgoAccesses:
                nAccesses = oneDayAgoAccesses[datasetName]
            dAccesses = 1
            if datasetName in twoDaysAgoAccesses:
                dAccesses = nAccesses - twoDaysAgoAccesses[DatasetName]
            info = {'replicas':replicas, 'sizeGb':sizeGb, 'nAccesses':nAccesses, 'dAccesses':dAccesses}
            datasetInfo[datasetName] = info
        return datasetInfo

#===================================================================================================
#  M A I N
#===================================================================================================
    def getDatasetRankings(self, threshold):
        # rank = (log(n_accesses)*d_accesses)/(size*relpicas^2)
        datasetRankings = dict()
        for datasetName, info in self.datasetInfo.iteritems():
            replicas = info['replicas']
            sizeGb = info['sizeGb']
            nAccesses = info['nAccesses']
            dAccesses = max(info['dAccesses'], 1)
            rank = (math.log10(nAccesses)*dAccesses)/(sizeGb*replicas**2)
            if rank >= threshold:
                datasetRankings[datasetName] = rank
        return datasetRankings
        
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./datasetRanking.py"
        sys.exit(2)
    datasetRanker = datasetRanker()
    datasetRankings = datasetRanker.getDatasetRankings()
    print datasetRankings
    sys.exit(0)
