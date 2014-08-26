#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
from operator import itemgetter
import sites, popDbData, phedexData, dbApi

class siteRanker():
	def __init__(self):
		phedexCache = os.environ['PHEDEX_CACHE']
		popDbCache = os.environ['POP_DB_CACHE']
		cacheDeadline = int(os.environ['CACHE_DEADLINE'])
		self.phedexData = phedexData.phedexData(phedexCache, cacheDeadline)
		self.popDbData = popDbData.popDbData(popDbCache, cacheDeadline)
		self.dbApi = dbApi.dbApi()
		self.sites = sites.sites()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def getMaxCpu(self, site):
		cpu = []
		for i in range(22):
			date = datetime.date.today() - datetime.timedelta(days=i+1)
			usedCpu = self.popDbData.getSiteCpu(site, date.strftime('%Y-%m-%d'))
			cpu.append((site, usedCpu))
		cpu = sorted(cpu, reverse=True, key=itemgetter(1))
		maxCpu = 0
		for i in range(3):
			maxCpu += cpu[i][1]
		maxCpu = maxCpu/3
		return maxCpu

	def getMaxStorage(self, site):
		query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
		values = [site, 'AnalysisOps']
		data = self.dbApi.dbQuery(query, values=values)
		maxStorage = data[0][0]
		return maxStorage

	def getAvailableCpu(self, site):
		date = datetime.date.today() - datetime.timedelta(days=1)
		maxCpu = self.getMaxCpu(site)
		usedCpu = self.popDbData.getSiteCpu(site, date.strftime('%Y-%m-%d'))
		availableCpu = maxCpu - usedCpu
		return max(availableCpu, 1)

	def getAvailableStorage(self, site):
		maxStorage = self.getMaxStorage(site)
		usedStorage = self.phedexData.getSiteStorage(site)
		availableStorage = maxStorage*10**3 - usedStorage
		return max(availableStorage, 1)

	def getSiteRankings(self):
		# rank = availableSiteStorageGb * availableSiteCpu
		siteRankings = dict()
		availableSites = self.sites.getAvailableSites()
		for siteName in availableSites:
			availableCpu = self.getAvailableCpu(siteName)
			availableStorageGb = self.getAvailableStorage(siteName)
			rank = (availableCpu + availableStorageGb)/10**3
			siteRankings[siteName] = rank
		return siteRankings

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./datasetRanking.py
if __name__ == '__main__':
	if not (len(sys.argv) == 1):
		print "Usage: python ./siteRanking.py"
		sys.exit(2)
	siteRanker_ = siteRanker()
	data = siteRanker_.getSiteRankings()
	print data
	sys.exit(0)
