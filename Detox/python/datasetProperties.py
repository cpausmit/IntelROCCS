#====================================================================================================
#  C L A S S E S  concerning the description of datasets
#====================================================================================================
		
class DatasetProperties:
	def __init__(self, name,size):
		self.name = name
		self.size = size
		self.siteList = []
		self.delFromSites = []

	def append(self,sites):
		for s in sites:
			if s not in self.siteList: 
				self.siteList.append(s)

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
