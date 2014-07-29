#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime
import popDbApi

class popDbData:
	def __init__(self, cachePath, oldestAllowedHours):
		self.logFile = os.environ['INTELROCCS_LOG']
		self.popDbApi = popDbApi.popDbApi()
		self.cachePath = cachePath
		self.oldestAllowedHours = oldestAllowedHours

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def shouldAccessPopDb(self, apiCall, date):
		cacheFile = "%s/%s/%s" % (self.cachePath, apiCall, date)
		if os.path.isfile(cacheFile):
			if os.path.getsize(cacheFile) == 0:
				# cache is empty
				return True
			# fetch data from cache
			return False
		# there is no cache file
		return True

	def updateCache(self, apiCall, date):
		tstart = date
		tstop = tstart
		jsonData = ""
		# can easily extend this to support more api calls
		if apiCall == "DSStatInTimeWindow":
			jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop)
			if not jsonData:
				with open(self.logFile, 'a') as logFile:
					logFile.write("%s PopDB Cache ERROR: Cannot update cache due to PopDB error\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
				return 0
		if not os.path.exists("%s/%s" % (self.cachePath, apiCall)):
			os.makedirs("%s/%s" % (self.cachePath, apiCall))
		with open("%s/%s/%s" % (self.cachePath, apiCall, date), 'w') as cacheFile:
			json.dump(jsonData, cacheFile)
		return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
	def getPopDbData(self, apiCall, date):
		jsonData = ""
		if self.shouldAccessPopDb(apiCall, date):
			jsonData = self.updateCache(apiCall, date)
		# access cache
		else:
			with open("%s/%s/%s" % (self.cachePath, apiCall, date), 'r') as cacheFile:
				try:
					jsonData = json.loads(cacheFile.read())
				except ValueError, e:
					with open(self.logFile, 'a') as logFile:
						logFile.write("%s PopDB Cache ERROR: Cache file corrupt. Will try to fetch from PopDB.\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
					jsonData = self.updateCache(apiCall, date)
		return jsonData

if __name__ == '__main__':
	cachePath = "%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE'])
	popDbData = popDbData(cachePath, 12)
	date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	jsonData = popDbData.getPopDbData("DSStatInTimeWindow", date)
	if not jsonData:
		print "ERROR: Could not fetch PopDB data, see log (%s) for more details" % (popDbApi.logFile)
		sys.exit(1)
	print "SUCCESS: PopDB data successfully fetched"
	sys.exit(0)
