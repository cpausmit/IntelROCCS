#====================================================================================================
#  C L A S S E S  concerning the description of datasets
#====================================================================================================

class DatasetProperties:
	def __init__(self, name):
		self.name = name
		self.dbaseId = -1
		self.globalRank = None
		#self.deprecated = False
		self.trueSize = 0
		self.trueNfiles = 0
		self.fullOnTape = False
		self.kickFromPool = False
		self.daysUsedAgo = 0
		self.siteList = []
		self.delFromSites = []
		self.weightAtSites = {}
		self.rankAtSites = {}

	def append(self,sites):
		for s in sites:
			if s not in self.siteList:
				self.siteList.append(s)

	def setRankAtSites(self,site, rank):
		if site not in self.siteList:
			raise Exception('Non-exising site name ' + site)
		self.rankAtSites[site] = rank

	def setWeightAtSites(self,site, weight):
		if site not in self.siteList:
			raise Exception('Non-exising site name ' + site)
		self.weightAtSites[site] = weight

	def setGlobalRank(self,rank):
		self.globalRank = rank

	def setId(self,	dsetId):
		self.dbaseId = dsetId

	def setFullOnTape(self,fullOnTape):
		self.fullOnTape = fullOnTape

	def isFullOnTape(self):
		return self.fullOnTape

	def setDaysSinceUsed(self,days):
		self.daysUsedAgo = days

	def daysSinceUsed(self):
		return self.daysUsedAgo

	#def setDeprecated(self,deprecated):
	#	self.deprecated = deprecated

	def addDelTarget(self,site):
		self.delFromSites.append(site)

	def removeDelTarget(self,site):
		if site in self.delFromSites:
			self.delFromSites.remove(site)

	def setTrueSize(self,size):
		self.trueSize = size

	def setTrueNfiles(self,nfiles):
		self.trueNfiles = nfiles

	def getTrueSize(self):
		return self.trueSize

	def dsetName(self):
                return self.name

	def mySites(self):
                return self.siteList

	def nSites(self):
                return len(self.siteList)

	def delTargets(self):
                return self.delFromSites

	def nBeDeleted(self):
                return len(self.delFromSites)

	def getGlobalRank(self):
		return self.globalRank
	
	def getId(self):
		return self.dbaseId

	#def isDeprecated(self):
	#	return self.deprecated

	def myRankAtSites(self,site):
		if site not in self.rankAtSites:
			raise Exception('Non-exising site name ' + site)
		return self.rankAtSites[site]

	def myWeightAtSites(self,site):
		if site not in self.weightAtSites:
			raise Exception('Non-exising site name ' + site)
		return self.weightAtSites[site]
