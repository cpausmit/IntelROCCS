#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Middleware to access phedex data. To reduce access time we cache the data, this class checks if
# the cache exists and is up to date. If it does then fetch it and return. If cache does not exist
# then access phedex directly and get the data. First update the cache and then return the
# data.
#
# Make sure there is a valid proxy before calling.
#
# In case of an error a '0' will be returned, caller must check to make sure data is returned.
#---------------------------------------------------------------------------------------------------
import sys, re, os, json, sqlite3, datetime
import phedexApi

class phedexData:
	def __init__(self, phedexCache, cacheDeadline):
		self.phedexApi = phedexApi.phedexApi()
		self.phedexCache = phedexCache
		self.cacheDeadline = cacheDeadline

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def shouldAccessPhedex(self, apiCall):
		cacheFile = "%s/%s.db" % (self.phedexCache, apiCall)
		timeNow = datetime.datetime.now()
		deltaNhours = datetime.timedelta(seconds = 60*60*(self.cacheDeadline))
		modTime = datetime.datetime.fromtimestamp(0)
		if os.path.isfile(cacheFile):
			modTime = datetime.datetime.fromtimestamp(os.path.getmtime(cacheFile))
			if os.path.getsize(cacheFile) == 0:
				# cache is empty
				return True
			if (timeNow-deltaNhours) > modTime:
				# cache is not up to date
				return True
			# fetch data from cache
			return False
		# there is no cache file
		return True

	def updateCache(self, apiCall):
		if not os.path.exists(self.phedexCache):
			os.makedirs(self.phedexCache)
		cacheFile = "%s/%s.db" % (self.phedexCache, apiCall)
		if os.path.isfile(cacheFile):
			os.remove(cacheFile)
		jsonData = ""
		# can easily extend this to support more api calls
		if apiCall == "blockReplicas":
			jsonData = self.phedexApi.blockReplicas(node='T2*', subscribed='y', show_dataset='y')
			if not jsonData:
				print(" ERROR -- Could not update cache due to phedex error")
				return 1
			self.buildBlockReplicasCache(jsonData)
		return 0

	def buildBlockReplicasCache(self, jsonData):
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('CREATE TABLE IF NOT EXISTS Datasets (DatasetId INTEGER PRIMARY KEY AUTOINCREMENT, DatasetName TEXT UNIQUE, SizeGb INTEGER)')
			cur.execute('CREATE TABLE IF NOT EXISTS Replicas (SiteName TEXT, DatasetId INTEGER, GroupName TEXT, FOREIGN KEY(DatasetId) REFERENCES Datasets(DatasetId))')
		datasets = jsonData.get('phedex').get('dataset')
		for dataset in datasets:
			datasetName = dataset.get('name')
			#if re.match('.+/USER', datasetName):
			#	continue
			sizeBytes = 0
			for block in dataset.get('block'):
				sizeBytes += int(block.get('bytes'))
			sizeGb = float(sizeBytes)/10**9
			with blockReplicasCache:
				cur = blockReplicasCache.cursor()
				cur.execute('INSERT INTO Datasets(DatasetName, SizeGb) VALUES(?, ?)', (datasetName, sizeGb))
				datasetId = cur.lastrowid
				for replica in dataset.get('block')[0].get('replica'):
					siteName = replica.get('node')
					groupName = replica.get('group')
					cur.execute('INSERT INTO Replicas(SiteName, DatasetId, GroupName) VALUES(?, ?, ?)', (siteName, datasetId, groupName))

	def getAllDatasets(self):
		datasets = ""
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return datasets
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT DISTINCT DatasetName FROM Datasets NATURAL JOIN Replicas WHERE GroupName=?', ('AnalysisOps',))
			datasets = []
			for row in cur:
				if re.match('.+/USER', row[0]):
					continue
				datasets.append(row[0])
		return datasets

	def getDatasetsAtSite(self, siteName):
		datasets = []
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return datasets
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT DISTINCT DatasetName FROM Datasets NATURAL JOIN Replicas WHERE GroupName=? and SiteName=?', ('AnalysisOps', siteName))
			datasets = []
			for row in cur:
				if re.match('.+/USER', row[0]):
					continue
				datasets.append(row[0])
		return datasets

	def getDatasetSize(self, datasetName):
		sizeGb = 1000000
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return sizeGb
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT SizeGb FROM Datasets WHERE DatasetName=?', (datasetName,))
			row = cur.fetchone()
			if row:
				sizeGb = row[0]
		return sizeGb

	def getNumberReplicas(self, datasetName):
		replicas = 100
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return replicas
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT count(*) FROM Replicas NATURAL JOIN Datasets WHERE Datasets.DatasetName=?', (datasetName,))
			row = cur.fetchone()
			if row:
				replicas = row[0]
		return replicas

	def getSiteReplicas(self, datasetName):
		sites = []
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return sites
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT SiteName FROM Replicas NATURAL JOIN Datasets WHERE Datasets.DatasetName=? AND Replicas.GroupName=?', (datasetName, 'AnalysisOps'))
			for row in cur:
				sites.append(row[0])
		return sites

	def getSiteStorage(self, siteName):
		storageGb = 0
		if self.shouldAccessPhedex('blockReplicas'):
			# update
			error = self.updateCache('blockReplicas')
			if error:
				return sites
		# access cache
		blockReplicasCache = sqlite3.connect("%s/blockReplicas.db" % (self.phedexCache))
		with blockReplicasCache:
			cur = blockReplicasCache.cursor()
			cur.execute('SELECT SizeGb FROM Datasets NATURAL JOIN Replicas WHERE Replicas.SiteName=? AND Replicas.GroupName=?', (siteName, 'AnalysisOps'))
			for row in cur:
				storageGb += row[0]
		return storageGb

#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == '__main__':
	phedexCache = "%s/IntelROCCS/Cache/Phedex" % (os.environ['HOME'])
	phedexData_ = phedexData(phedexCache, 12)
	siteStorage = phedexData_.getSiteStorage('T2_AT_Vienna')
	if not siteStorage:
		print " ERROR -- Could not fetch phedex data"
		sys.exit(1)
	print " SUCCESS -- Phedex data successfully fetched"
	print siteStorage
	sys.exit(0)
