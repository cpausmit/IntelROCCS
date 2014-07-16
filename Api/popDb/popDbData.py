#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import sys, os, json, datetime
import popDbApi

class popDbData:
    def __init__(self, cachePath, oldestAllowedHours):
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
            # TODO : deal with any exceptions from popDb
            jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop)
        if not os.path.exists("%s/%s" % (self.cachePath, apiCall)):
            os.makedirs("%s/%s" % (self.cachePath, apiCall))
        with open("%s/%s/%s" % (self.cachePath, apiCall, date), 'w') as cacheFile:
            json.dump(jsonData, cacheFile)
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
    def getPopDbData(self, apiCall, date):
        if self.shouldAccessPopDb(apiCall, date):
            # TODO : deal with if this fails
            self.popDbApi.renewSSOCookie()
            return self.updateCache(apiCall, date)
        # access cache
        with open("%s/%s/%s" % (self.cachePath, apiCall, date), 'r') as cacheFile:
            return json.loads(cacheFile.read())

if __name__ == '__main__':
    cachePath = "%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE'])
    popDbData = getPopDbData(cachePath, 12)
    for i in range(22):
        date = (datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        jsonData = popDbData.getPopDbData("DSStatInTimeWindow", date)
    sys.exit(0)
