#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import sys, os, math, json, datetime
import phedexData, popDbData

class datasetRanker():
	def __init__(self, threshold):
		phedexCache = os.environ['PHEDEX_CACHE']
		popDbCache = os.environ['POP_DB_CACHE']
		cacheDeadline = os.environ['CACHE_DEADLINE']
		self.threshold = os.environ['DATA_DEALER_THRESHOLD']
		self.phedexData = phedexData.phedexDb(phedexCache, cacheDeadline)
		self.popDbData = popDbData.popDbData(popDbCache, cacheDeadline)
		self.datasetRankings = dict()
#===================================================================================================
#  H E L P E R S
#===================================================================================================

#===================================================================================================
#  M A I N
#===================================================================================================
	def getDatasetRankings(self):
		# rank = (log(n_accesses)*d_accesses)/(size*relpicas^2)
		datasetCandidates = dict()
		datasets = self.phedexData.getAllDatasets()
		date = datetime.date.today() - datetime.timedelta(days=1)
		for datasetName in datasets:
			replicas = max(self.phedexData.getNumberReplicas(datasetName), 1)
			sizeGb = self.phedexData.getDatasetSize(datasetName)
			nAccesses = max(self.popDbDb.getDatasetAccesses(datasetName, date.strftime('%Y-%m-%d')), 1)
			dAccesses = max(self.popDbDb.getDatasetAccesses(datasetName, (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')), 1)
			rank = (math.log10(nAccesses)*dAccesses)/(sizeGb*replicas**2)
			self.datasetRankings[datasetName] = rank
			if rank >= self.threshold:
				datasetCandidates[datasetName] = rank
		return datasetCandidates

# Use this for testing purposes or as a script.
# Usage: python ./datasetRanker.py
if __name__ == '__main__':
	if not (len(sys.argv) == 1):
		print "Usage: python ./datasetRanker.py"
		sys.exit(2)
	datasetRanker_ = datasetRanker(1)
	datasetRankings = datasetRanker_.getDatasetRankings()
	print datasetRankings
	sys.exit(0)
