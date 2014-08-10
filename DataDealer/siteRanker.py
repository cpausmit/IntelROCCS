#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
from operator import itemgetter
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import sites, popDbDb, phedexDb
import IntelROCCS.Api.db.dbApi as dbApi

class siteRanker():
	def __init__(self):
		self.logFile = os.environ['INTELROCCS_LOG']
		self.sites = sites.sites()
		self.phedexDb = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
		self.popDbDb = popDbDb.popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)
		self.dbApi = dbApi.dbApi()
#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def getMaxCpu(self, site):
		cpu = []
		for i in range(22):
			date = datetime.date.today() - datetime.timedelta(days=i+1)
			usedCpu = self.popDbDb.getSiteCpu(site, date.strftime('%Y-%m-%d'))
			cpu.append((site, usedCpu))
		cpu = sorted(cpu, reverse=True, key=itemgetter(1))
		maxCpu = 0
		for i in range(3):
			maxCpu += cpu[i][1]
		if not maxCpu:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s Site Rank ERROR: Max CPU for site %s returned nothing\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), site))
		maxCpu = maxCpu/3
		return maxCpu

	def getMaxStorage(self, site):
		query = "SELECT SizeTb FROM Quotas WHERE GroupName=%s AND SiteName=%s"
		values = ['AnalysisOps', site]
		data = self.dbApi.dbQuery(query, values=values)
		if not data:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s Site Rank ERROR: Max storage for site %s returned nothing\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), site))
			return 10
		maxStorage = data[0][0]
		return maxStorage

	def getAvailableCpu(self, site):
		date = datetime.date.today() - datetime.timedelta(days=1)
		maxCpu = self.getMaxCpu(site)
		usedCpu = self.popDbDb.getSiteCpu(site, date.strftime('%Y-%m-%d'))
		availableCpu = maxCpu - usedCpu
		return availableCpu

	def getAvailableStorage(self, site):
		maxStorage = self.getMaxStorage(site)
		usedStorage = self.phedexDb.getSiteStorage(site)
		availableStorage = maxStorage*10**3 - usedStorage
		return availableStorage

	def getSiteRankings(self):
		# rank = availableSiteStorageGb * availableSiteCpu
		siteRankings = dict()
		availableSites = self.sites.getAvailableSites()
		for siteName in availableSites:
			availableCpu = max(self.getAvailableCpu(siteName), 1)
			availableStorageGb = max(self.getAvailableStorage(siteName), 1)
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
	siteRanker = siteRanker()
	data = siteRanker.getSiteRankings()
	print data
	sys.exit(0)
