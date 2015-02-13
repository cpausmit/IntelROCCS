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
        self.reqIds[reqId] = timeStamp
        if self.maxReqId < reqId:
            self.maxReqId = reqId
    def getLastReqId(self):
        return self.maxReqId
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
        self.reqId  = orig.reqId
        self.site   = orig.site
        self.tstamp = orig.tstamp
        for dset in orig.dsetsizes:
            self.update(dset,orig.dsetranks[dset],orig.dsetsizes[dset])

    def update(self,dset,rank,size):
        if dset in self.dsetsizes:
            print dset
            raise Exception("Duplicate dataset in DeletionRequest!!!")

        self.dsetsizes[dset] = size
        self.dsetranks[dset] = rank
        self.size = self.size + size
        self.ndsets = self.ndsets + 1

    def looksIdentical(self,otherReq):
        if otherReq == None:
            return False
        matched = 0
        for dset in self.dsetsizes.keys():
            if otherReq.hasDataset(dset):
                matched = matched + 1
        if (self.ndsets-matched)/self.ndsets < 0.05:
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
