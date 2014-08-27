#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Select datasets to subscribe and select sites on which to subscribe these datasets
#---------------------------------------------------------------------------------------------------
import sys, os, random
import phedexData

class selection():
	def __init__(self):
		self.budgetGb = int(os.environ['DATA_DEALER_BUDGET'])
		phedexCache = os.environ['PHEDEX_CACHE']
		cacheDeadline = int(os.environ['CACHE_DEADLINE'])
		self.phedexData = phedexData.phedexData(phedexCache, cacheDeadline)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def weightedChoice(self, choices):
		total = sum(w for c, w in choices.iteritems())
		r = random.uniform(0, total)
		upto = 0
		for c, w in choices.iteritems():
			if upto + w > r:
				return c
			upto += w

	def selectSubscriptions(self, datasetRankings, siteRankings):
		selectedGb = 0
		subscriptions = dict()
		print self.budgetGb
		while (selectedGb < self.budgetGb) and (datasetRankings):
			datasetName = self.weightedChoice(datasetRankings)
			siteName = self.weightedChoice(siteRankings)
			if siteName in subscriptions:
				subscriptions[siteName].append(datasetName)
			else:
				subscriptions[siteName] = [datasetName]
			del datasetRankings[datasetName]
			sizeGb = self.phedexData.getDatasetSize(datasetName)
			selectedGb += int(sizeGb)
			print selectedGb
		return subscriptions

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./select.py
if __name__ == '__main__':
	if not (len(sys.argv) == 1):
		print "Usage: python ./select.py"
		sys.exit(2)
	select_ = select()
	# Testcase
	choices = {'dataset1': 1, 'dataset2': 2, 'dataset3': 3}
	datasetName, ranking = select_.weightedChoice(choices)
	print datasetName + " : " + str(ranking)
	sys.exit(0)
