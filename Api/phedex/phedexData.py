#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Middleware to access phedex data. To reduce access time we cache the data, this class checks if 
# the cache exists and is up to date. If it does then fetch it and return. If cache does not exist
# then access phedex directly and get the data. First update the cache and then return the
# data.
# 
# In case of an error a '0' will be returned, caller must check to make sure data is returned. 
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime, subprocess
import phedexApi

class phedexData:
    def __init__(self, cachePath, oldestAllowedHours):
        self.logFile = os.environ['INTELROCCS_LOG']
        self.phedexApi = phedexApi.phedexApi()
        self.cachePath = cachePath
        self.oldestAllowedHours = oldestAllowedHours

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def shouldAccessPhedex(self, apiCall):
        cacheFile = "%s/%s" % (self.cachePath, apiCall)
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(self.oldestAllowedHours))
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
        jsonData = ""
        # can easily extend this to support more api calls
        if apiCall == "blockReplicas":
            # TODO : deal with any exceptions from phedex
            jsonData = self.phedexApi.blockReplicas(node='T2*', subscribed='y', show_dataset='y', create_since='0')
            if not jsonData:
                with open(self.logFile, 'a') as logFile:
                    logFile.write("%s PhEDEx Cache ERROR: Cannot update cache due to PhEDEx error\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                return 0
        if not os.path.exists(self.cachePath):
            os.makedirs(self.cachePath)
        with open("%s/%s" % (self.cachePath, apiCall), 'w') as cacheFile:
            json.dump(jsonData, cacheFile)
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPhedexData(self, apiCall):
        jsonData = ""
        if self.shouldAccessPhedex(apiCall):
            # update
            error = self.phedexApi.renewProxy()
            if error:
                return 0
            jsonData =  self.updateCache(apiCall)
        # access cache
        else:
            with open("%s/%s" % (self.cachePath, apiCall), 'r') as cacheFile:
                try:
                    jsonData = json.loads(cacheFile.read())
                except ValueError, e:
                    with open(self.logFile, 'a') as logFile:
                        logFile.write("%s PhEDEx Cache ERROR: Cache file corrupt. Will try to fetch from PhEDEx.\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                jsonData = self.updateCache(apiCall)
        return jsonData

if __name__ == '__main__':
    phedexData = phedexData("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
    jsonData = phedexData.getPhedexData("blockReplicas")
    if not jsonData:
        print "ERROR: Could not fetch PhEDEx data"
        sys.exit(1)
    print "SUCCESS: PhEDEx data successfully fetched"
    sys.exit(0)
