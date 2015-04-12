#!/usr/bin/python

from time import strftime,gmtime
import time

'''Dataset object definition
    and common functions to 
    manipulate Datasets'''

# genesis=int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
genesis=1378008000

class Dataset(object):
    siteList = []
    """Object containing relevant dataset properties"""
    def __init__(self, name):
        self.name = name
        self.nFiles = -1
        self.sizeGB = -1
        self.nAccesses = {}
        self.movement = {}
        self.cTime = genesis
        self.nSites = -1
        self.currentSites = set([])
        self.transfersOntoCSites = {}
        self.isDeleted = None
    def addCurrentSite(self,siteName,timeStart=-1,timeComplete=-1):
        self.currentSites.add(siteName)
        if timeStart>0:
            self.transfersOntoCSites[siteName] = (timeStart,timeComplete)
    def setSiteMovement(self, siteName,movement):
        self.movement[siteName] = movement
    def addTransfer(self,siteName,t):
        if siteName not in self.movement:
            self.movement[siteName] = ([],[])
        self.movement[siteName][0].append(t)
    def addDeletion(self,siteName,t):
        if siteName not in self.movement:
            self.movement[siteName] = ([],[])
        self.movement[siteName][1].append(t)
    def addAccesses(self,siteName,n,utime=0):
        if n==0:
            return
        if siteName not in self.nAccesses:
            self.nAccesses[siteName]={}
        siteAccess = self.nAccesses[siteName]
        if utime not in siteAccess:
            siteAccess[utime] = 0
        siteAccess[utime]+=n
    def __str__(self):
        s = "================================================\n"
        s += self.name
        s += "\n\t nFiles = %i"%(self.nFiles)
        s += "\t sizeGB = %.2f"%(self.sizeGB)
        s += "\t cTime = %s\n"%(strftime("%Y-%m-%d",gmtime(self.cTime)))
        s += "\t Current sites ="
        for siteName in self.currentSites:
            s += "  %s"%(siteName)
        s += "\n"
        s += "\t Site History =\n"
        for siteName,m in self.movement.iteritems():
            s+="\t                %s %s\n"%(siteName,str(m))
        s += "================================================\n"
        return s
    def getTotalAccesses(self,start=-1,end=-1):
        if start==-1 and end==-1:
            r=0
            for siteName,accessesByTime in self.nAccesses.iteritems():
                for utime in accessesByTime:
                    r += accessesByTime[utime]
            return r
        else:
            r=0
            for siteName in self.nAccesses:
                for utime in accessesByTime:
                    if utime < end and utime > start:
                        r += accessesByTime[utime]
            return r
    def fixXrootd(self):
        # fix access history to account for xrootd accesses
        nXrootdAccesses = []
        realSites = []
        for siteName in self.siteList:
            if siteName not in self.movement:
                if siteName in self.nAccesses:
                    # dataset was never put on this site (already accounted for datasets created at sites)
                    for utime,accesses in self.nAccesses[siteName].iteritems():
                        nXrootdAccesses.append((utime,accesses))
                    del self.nAccesses[siteName]
            else:
                realSites.append(siteName)
        nRealSites = len(realSites)
        for siteName in realSites:
            # evenly distribute xrootd sites among sites on which the dataset exists
            for utime,n in nXrootdAccesses:
                self.addAccesses(siteName,float(n)/nRealSites,utime)


def updateByKey(d1,d2):
    # update d1 with k:v from d2 if k not in d1
    for k,v in d2.iteritems():
        if k not in d1:
            d1[k] = v

def removeByKey(d1,s):
    for ss in s:
        try:
            del d1[ss]
        except KeyError:
            pass

