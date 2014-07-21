#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the Analysisops space.
#
# TODO: Currently don't handle errors in popDB or Phedex, or if no data is returned an IndexError
# is thrown which is currently not caught.
#---------------------------------------------------------------------------------------------------
import sys, os, math, json, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import phedexDb, popDbDb

class datasetRanker():
    def __init__(self, threshold):
        self.threshold = threshold
        self.phedexDb = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
        self.popDb = popDbDb.popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)

#===================================================================================================
#  H E L P E R S
#===================================================================================================


#===================================================================================================
#  M A I N
#===================================================================================================
    def getDatasetRankings(self):
        # rank = (log(n_accesses)*d_accesses)/(size*relpicas^2)
        datasetRankings = dict()
        datasetCandidates = dict()
        datasets = self.phedexDb.getAllDatasets()
        date = datetime.date.today() - datetime.timedelta(days=1)
        for datasetName in datasets:
            replicas = max(self.phedexDb.getNumberReplicas(datasetName), 1)
            sizeGb = max(self.phedexDb.getDatasetSize(datasetName), 1)
            nAccesses = max(self.popDb.getDatasetAccesses(datasetName, date.strftime('%Y-%m-%d')), 1)
            dAccesses = max(nAccesses - self.popDb.getDatasetAccesses(datasetName, (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')), 1)
            rank = (math.log10(nAccesses)*dAccesses)/(sizeGb*replicas**2)
            datasetRankings[datasetName] = rank
            if rank >= self.threshold:
                datasetCandidates[datasetName] = rank
        return datasetCandidates
        
# Use this for testing purposes or as a script. 
# Usage: python ./datasetRanker.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./datasetRanker.py"
        sys.exit(2)
    datasetRanker = datasetRanker(20)
    datasetRankings = datasetRanker.getDatasetRankings()
    print datasetRankings
    sys.exit(0)
