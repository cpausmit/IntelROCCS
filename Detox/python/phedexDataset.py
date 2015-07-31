#---------------------------------------------------------------------------------------------------
#
# This class provides support for storing datasets information extracted from PhEDEx
#
#---------------------------------------------------------------------------------------------------
import time, sys

class PhedexDataset:

    def __init__(self, dataset):
        self.dataset = dataset
        self.trueSize = 0
        self.trueNfiles = 0
        self.siteNames = {}
        self.groupAtSite = {}
        self.sizeAtSite = {}
        self.validAtSite = {}
        self.custodialAtSite = {}
        self.reqTimeAtSite = {}
        self.updTimeAtSite = {}
        self.isdoneAtSite = {}
        self.rankAtSite = {}
        self.usedAtSite = {}
        self.filesAtSite = {}
        self.partialAtSite = {}
        self.globalRank = 9999.0
        self.epochTime = int(time.time())

    def updateForSite(self,site,size,group,files,custodial,reqtime,updtime,isdone):
        if site in self.groupAtSite:
            if self.groupAtSite[site] != group:
                print " WARNING -- dataset duplicated, subscribed by different groups"
                print self.dataset + ' ' + site + ' ' + self.groupAtSite[site] + ' ' + group
                return

        if site not in self.siteNames:
            self.siteNames[site] = 1
            self.sizeAtSite[site] = size
            self.filesAtSite[site] = files
            self.updTimeAtSite[site] = updtime
            self.reqTimeAtSite[site] = reqtime
            self.isdoneAtSite[site] =  isdone
        else:
            self.sizeAtSite[site] = self.sizeAtSite[site] + size
            self.filesAtSite[site] = self.filesAtSite[site] + files
            if updtime > self.updTimeAtSite[site]:
                self.updTimeAtSite[site] = updtime
            #check if this is changining, it should not
            if reqtime > self.reqTimeAtSite[site]:
                self.reqTimeAtSite[site] = reqtime
            if isdone == 0:
                self.isdoneAtSite[site] =  isdone

        self.setCustodial(site,custodial)
        self.groupAtSite[site] = group

    def setFinalValues(self):
        for site in self.siteNames:
            valid = 1
            reqtime = self.reqTimeAtSite[site]
            isdone = self.isdoneAtSite[site]
            if (self.epochTime - reqtime) < 60*60*24*14 and isdone == 0:
                valid = 0
            self.setValid(site,valid)

    def locatedOnSites(self,groups):
        validSites = []
        for site in self.siteNames:
            grp =  self.groupAtSite[site]
            if grp not in groups:
                continue
            #if self.validAtSite[site]:
            validSites.append(site)
        return validSites

    def isOnSite(self,site):
        if site in self.siteNames:
            return True
        return False

    def isOnT1Site(self):
        for site in self.siteNames:
            if site.startswith("T1_"):
                return True
        return False

    def matchesSite(self,pattern):
        for site in self.siteNames:
            if pattern in site:
                return True
        return False

    def setValid(self,site,valid):
        if site in self.validAtSite:
            if self.validAtSite[site] == 1:
                self.validAtSite[site] = valid
        else:
            self.validAtSite[site] = valid

    def setCustodial(self,site,custodial):
        if site in self.custodialAtSite:
            if self.custodialAtSite[site] == 0:
                self.custodialAtSite[site] = custodial
        else:
            self.custodialAtSite[site] = custodial

    def setLocalRank(self,site,rank):
        self.rankAtSite[site] = rank

    def setGlobalRank(self,rank):
        self.globalRank = rank

    def setIfUsed(self,site,used):
        self.usedAtSite[site] = used

    def setTrueSize(self,size):
        self.trueSize = size

    def setTrueNfiles(self,nfiles):
        self.trueNfiles = nfiles

    def getIfUsed(self,site):
        return self.usedAtSite[site]

    def getTrueSize(self):
        return self.trueSize

    def getMaxSize(self):
        maxSize = 0
        nsites = 0
        for site in self.sizeAtSite:
            if maxSize < self.sizeAtSite[site]:
                maxSize = self.sizeAtSite[site]
                nsites = 0
            if self.sizeAtSite[site] == maxSize:
		nsites = nsites + 1
        return (nsites,maxSize)

    def getTrueNfiles(self):
        return self.trueNfiles

    def getLocalRank(self,site):
        return self.rankAtSite[site]

    def getGlobalRank(self):
        return self.globalRank

    def size(self,site):
        if site in self.sizeAtSite:
            return self.sizeAtSite[site]
        return 0

    def group(self,site):
        if site in self.groupAtSite:
            return self.groupAtSite[site]
        return 'undef'

    def creationTime(self,site):
        if site in self.updTimeAtSite:
            return self.updTimeAtSite[site]
        return -999

    def isValid(self,site):
        if site in self.validAtSite:
            return self.validAtSite[site]
        return False

    def isCustodial(self,site):
        if site in self.custodialAtSite:
            return self.custodialAtSite[site]
        return False

    def isPartial(self,site):
        if site in self.partialAtSite:
            return self.partialAtSite[site]
        return False

    def getNfiles(self,site):
        return self.filesAtSite[site]

    def reqTime(self,site):
        return self.reqTimeAtSite[site]

    def updTime(self,site):
        return self.updTimeAtSite[site]

    def isDone(self,site):
        return self.isdoneAtSite[site]

    def findIncomplete(self):
        for site,files in self.filesAtSite.items():
            if files != self.trueNfiles:
                #only valid dataset can be taged as partial
                if self.isValid(site):
                    if self.trueSize < self.sizeAtSite[site]:
                        if self.matchesSite("T2_"):
                            pass
                            #print "  -- WARNING -- need correct size for " + self.dataset 
                    else:
                        self.partialAtSite[site] = True

    def printIntoLine(self):
        if(len(self.siteNames.keys()) < 1):
            return ""

        line = ""
        for site in self.siteNames:
            size =  self.sizeAtSite[site]
            group = self.groupAtSite[site]
            custd = self.custodialAtSite[site]
            files = self.filesAtSite[site]
            reqtime = self.reqTimeAtSite[site]
            updtime = self.updTimeAtSite[site]
            isdone =  self.isdoneAtSite[site]

            line = line + self.dataset + " " + group + " " + str(size)
            line = line + " " + str(files) + " " + str(custd) + " " + site
            line = line + " " + str(reqtime) + " " + str(updtime) + " " + str(isdone) + "\n"

        return line

    def fillFromLine(self,line):
        items = line.split()
        datasetName = items[0]
        items.remove(datasetName)
        if datasetName != self.dataset:
            raise Exception(" -- Logical mistake: mismatching dataset names")

        group = items[0]
        size = float(items[1])
        files = int(items[2])
        custd = int(items[3])
        t2Site = items[4]
        reqtime = int(items[5])
        updtime = int(items[6])
        isdone =  int(items[7])
        self.updateForSite(t2Site,size,group,files,custd,reqtime,updtime,isdone)
