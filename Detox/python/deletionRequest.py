#====================================================================================================
#  C L A S S E S  concerning the deletion requests
#====================================================================================================
import time, datetime

#---------------------------------------------------------------------------------------------------
"""
Class:  DeletionRequest(siteName='')
Stores information abotu deletion requests
"""
#---------------------------------------------------------------------------------------------------
class SiteDeletions:
    def __init__(self,siteName):
        self.siteName = siteName
        self.maxReqId = 0
        self.reqIds = {}
    def update(self, reqId, timeStamp):
        if self.maxReqId < reqId:
            self.maxReqId = reqId
        self.reqIds[reqId] = timeStamp

    def getLastReqId(self, nitems):
        lastIds = []
        i = 0
        for reqid in sorted(self.reqIds, reverse=True):
            lastIds.append(reqid)
            if i >= nitems:
                break
            i = i + 1
        return lastIds

    def getReqIds(self):
        return sorted(self.reqIds.keys(),reverse=True)

class DeletionRequest:
    #"A DeletionRequest aggregates information about submitted deletion requests."
    def __init__(self, reqId, siteName, timeStamp, orig=None):
        self.size   = 0
        self.ndsets = 0
        self.dsetsizes = {}
        self.dsetranks = {}
        self.reqId  = reqId
        self.site   = siteName
        self.tstamp = timeStamp
        if orig != None :
            self.copyConstructor(orig)

    def copyConstructor(self,orig):
        for dsetId in orig.dsetsizes:
            self.update(dsetId,orig.dsetranks[dsetId],orig.dsetsizes[dsetId])

    def update(self,dsetId,rank,size):
        if dsetId > 0:
            if dsetId in self.dsetsizes:
		print self.reqId
                print dsetId
                raise Exception("Duplicate dataset in DeletionRequest!!!")

        self.dsetsizes[dsetId] = size
        self.dsetranks[dsetId] = rank
        self.size = self.size + size
        self.ndsets = self.ndsets + 1

    def looksIdentical(self,otherReq):
        if otherReq == None:
            return False
        matched = 0
        for dset in self.dsetsizes.keys():
            if otherReq.hasDataset(dset):
                matched = matched + 1
	matchFrac = float(self.ndsets-matched)/float(self.ndsets)
        if abs(matchFrac) < 0.05:
            return True
        return False

    def hasDataset(self,dset):
        if dset in self.dsetsizes:
            return True
        return False

    def getDsets(self):
        return sorted(self.dsetranks, key=self.dsetranks.get,
                      reverse=True)

    def deltaTime(self,other):
         epoch1 = time.mktime(self.getTimeStamp().timetuple())
         epoch2 = time.mktime(other.getTimeStamp().timetuple())
         return (epoch1-epoch2)

    def getDsetRank(self,dset):
        return self.dsetranks[dset]
    def getDsetSize(self,dset):
        return self.dsetsizes[dset]
    def getTimeStamp(self):
        return self.tstamp
    def getNdsets(self):
        return self.ndsets
    def getSize(self):
        return self.size
    def siteName(self):
        return self.site
