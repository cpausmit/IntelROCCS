#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime
import popDbApi

class popDbData:
	def __init__(self, popDbCache, cacheDeadline):
		self.popDbApi = popDbApi.popDbApi()
		self.popDbCache = popDbCache
		self.cacheDeadline = cacheDeadline

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def shouldAccessPopDb(self, apiCall, dataset='', site='', date):
		cacheFile = "%s/%s.db" % (self.popDbCache, apiCall)
		if (not os.path.isfile(cacheFile)) or (os.path.getsize(cacheFile) == 0):
			# there is no cache database
			return True
		# see if the data for that date is available
		cacheCon = sqlite3.connect(cacheFile)
		with cacheCon:
			if site:
				cur.execute('SELECT * FROM SiteData WHERE Day=? AND SiteName=?', (date, site))
				row = cur.fetchone()
				if not row:
					return True
			elif dataset:
				cur.execute('SELECT * FROM DatasetData WHERE Day=?', (date,))
				row = cur.fetchone()
				if not row:
					return True
		return False


	def updateDatasetCache(self, apiCall, site='', date):
		if not os.path.exists(self.popDbCache):
			os.makedirs(self.popDbCache)
		tstart = date
		tstop = tstart
		jsonData = ""
		# can easily extend this to support more api calls
		if apiCall == "DSStatInTimeWindow":
			if site:
				jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop, sitename=site)
				if not jsonData:
					print(" ERROR -- Could not update cache due to pop db error")
					return 1
				self.buildSiteDSStatInTimeWindowCache(jsonData, date)
			else:
				jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop)
				if not jsonData:
					print(" ERROR -- Could not update cache due to pop db error")
					return 1
				self.buildDatasetDSStatInTimeWindowCache(jsonData, date)
		return 0

	def buildSiteDSStatInTimeWindowCache(self, jsonData, date):
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('CREATE TABLE DatasetData (Day TEXT, DatasetName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
			cur.execute('CREATE TABLE SiteData (Day TEXT, SiteName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
		siteName = jsonData.get('SITENAME')
		datasets = jsonData.get('DATA')
		numberAccesses = 0
		numberCpus = 0
		for dataset in datasets:
			numberAccesses += dataset.get('NACC')
			numberCpus += dataset.get('TOTCPU')
		with self.dbCon:
			cur = self.dbCon.cursor()
			cur.execute('INSERT INTO SiteData(Day, SiteName, NumberAccesses, NumberCpus) VALUES(?, ?, ?, ?)', (date, siteName, numberAccesses, numberCpus))

	def buildDatasetDSStatInTimeWindowCache(self, jsonData, date):
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('CREATE TABLE DatasetData (Day TEXT, DatasetName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
			cur.execute('CREATE TABLE SiteData (Day TEXT, SiteName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
		datasets = jsonData.get('DATA')
		for dataset in datasets:
			datasetName = dataset.get('COLLNAME')
			numberAccesses = dataset.get('NACC')
			numberCpus = dataset.get('TOTCPU')
			with DSStatInTimeWindowCache:
				cur = DSStatInTimeWindowCache.cursor()
				cur.execute('INSERT INTO DatasetData(Day, DatasetName, NumberAccesses, NumberCpus) VALUES(?, ?, ?, ?)', (date, datasetName, numberAccesses, numberCpus))

	def getDatasetAccesses(self, datasetName, date):
		numberAccesses = 0
		if shouldAccessPopDb(apiCall='DSStatInTimeWindow', dataset=datasetName, date=date):
			error = self.updateCache(apiCall='DSStatInTimeWindow', date=date)
			if error:
				return numberAccesses
		# access cache
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('SELECT NumberAccesses FROM DatasetData WHERE DatasetName=? AND Day=?', (datasetName, date))
			row = cur.fetchone()
			if row:
				numberAccesses = row[0]
		return numberAccesses

	def getDatasetCpus(self, datasetName, date):
		numberCpus = 0
		if shouldAccessPopDb(apiCall='DSStatInTimeWindow', dataset=datasetName, date=date):
			error = self.updateCache(apiCall='DSStatInTimeWindow', date=date)
			if error:
				return numberCpus
		# access cache
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('SELECT NumberCpus FROM DatasetData WHERE DatasetName=? AND Day=?', (datasetName, date))
			row = cur.fetchone()
			if row:
				numberCpus = row[0]
		return numberCpus

		def getSiteAccesses(self, siteName, date):
		numberAccesses = 1
		if shouldAccessPopDb(apiCall='DSStatInTimeWindow', site=siteName, date=date):
			error = self.updateCache(apiCall='DSStatInTimeWindow', site=siteName, date=date)
			if error:
				return numberAccesses
		# access cache
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('SELECT NumberAccesses FROM SiteData WHERE SiteName=? AND Day=?', (siteName, date))
			row = cur.fetchone()
			if row:
				numberAccesses = row[0]
		return numberAccesses

	def getSiteCpu(self, siteName, date):
		numberCpus = 1
		if shouldAccessPopDb(apiCall='DSStatInTimeWindow', site=siteName, date=date):
			error = self.updateCache(apiCall='DSStatInTimeWindow', site=siteName, date=date)
			if error:
				return numberCpus
		# access cache
		DSStatInTimeWindowCache = sqlite3.connect("%s/DSStatInTimeWindow.db" % (self.popDbCache))
		with DSStatInTimeWindowCache:
			cur = DSStatInTimeWindowCache.cursor()
			cur.execute('SELECT NumberCpus FROM SiteData WHERE SiteName=? AND Day=?', (siteName, date))
			row = cur.fetchone()
			if row:
				numberCpus = row[0]
		return numberCpus

#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == '__main__':
	popDbCache = "%s/IntelROCCS/Cache/PopDb" % (os.environ.get['HOME'])
	popDbData_ = popDbData(popDbCache, 12)
	# TODO -- Create dataset and date
	numberAccesses = popDbData_.getDatasetAccesses(datasetName, date)
	if not numberAccesses:
		print " ERROR -- Could not fetch pop db data"
		sys.exit(1)
	print " SUCCESS -- Pop DB data successfully fetched"
	sys.exit(0)
