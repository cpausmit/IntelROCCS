#====================================================================================================
#  C L A S S E S  concerning the description of datasets
#====================================================================================================
		
class DatasetProperties:
	def __init__(self, name, size):
		self.name = name
		self.size = size
		self.globalRank = None
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
	
	def addDelTarget(self,site):
		self.delFromSites.append(site)	

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

	def mySize(self):
                return self.size

	def getGlobalRank(self):
		return self.globalRank

	def myRankAtSites(self,site):
		if site not in self.rankAtSites.keys():
			raise Exception('Non-exising site name ' + site)
		return self.rankAtSites[site]
	
	def myWeightAtSites(self,site):
		if site not in self.weightAtSites.keys():
			raise Exception('Non-exising site name ' + site)
		return self.weightAtSites[site]
