#---------------------------------------------------------------------------------------------------
#
# This class provides support for storing datasets information extracted from PhEDEx
#
#---------------------------------------------------------------------------------------------------
class PhedexDataset:

    def __init__(self, dataset):
        self.dataset = dataset
        self.siteNames = {}
        self.groupAtSite = {}
        self.sizeAtSite = {}
        self.validAtSite = {}
        self.custodialAtSite = {}
        self.makeTimeAtSite = {}
        self.rankAtSite = {}
        self.filesAtSite = {}
        self.partialAtSite = {}
        self.globalRank = 9999.0

    def updateForSite(self,site,size,group,mtime,files,custodial,valid):
        if site in self.groupAtSite:
            if self.groupAtSite[site] != group:
                print " WARNING -- dataset duplicated, subscribed by different groups"
                print self.dataset + ' ' + site + ' ' + self.groupAtSite[site] + ' ' + group
                return

        if site not in self.siteNames:
            self.siteNames[site] = 1
            self.sizeAtSite[site] = size
            self.filesAtSite[site] = files
            self.makeTimeAtSite[site] = mtime
        else:
            self.sizeAtSite[site] = self.sizeAtSite[site] + size
            self.filesAtSite[site] = self.filesAtSite[site] + files
            if mtime < self.makeTimeAtSite[site]:
                self.makeTimeAtSite[site] = mtime

        self.groupAtSite[site] = group   
        self.setValid(site,valid)
        self.setCustodial(site,custodial)

    def locatedOnSites(self,groups=['AnalysisOps']):
        validSites = []
        for site in self.siteNames:
            grp =  self.groupAtSite[site]
            if grp not in groups:
                continue
            if self.validAtSite[site]:
                validSites.append(site)
        return validSites

    def isOnSite(self,site):
        if site in self.siteNames:
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

    def setIncomplete(self):
        for site in self.siteNames:
            if self.validAtSite[site]:
                self.partialAtSite[site] = True

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
        if site in self.makeTimeAtSite:
            return self.makeTimeAtSite[site]
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

    def findIncomplete(self):
        if len(self.siteNames.keys()) < 1:
            return

        fullSize = (sorted(self.sizeAtSite.values(),reverse=True))[0]
        fullFiles = (sorted(self.filesAtSite.values(),reverse=True))[0]
        for site,size in self.sizeAtSite.items():
            if size != fullSize:
                #only valid dataset can be taged as partial
                if self.isValid(site):
                    self.partialAtSite[site] = True

    def printIntoLine(self):
        if(len(self.siteNames.keys()) < 1):
            return ""

        line = ""
        for site in self.siteNames:
            size =  self.sizeAtSite[site]
            creationTime = self.makeTimeAtSite[site]
            group = self.groupAtSite[site]
            valid = self.validAtSite[site]
            custd = self.custodialAtSite[site]
            files = self.filesAtSite[site]
            
            line = line + self.dataset + " " + group + " " + str(creationTime)
            line = line + " "  + str(size) + " " + str(files) + " " + str(valid) + " " + str(custd)
            line = line + " " + site + "\n"
        
        return line

    def fillFromLine(self,line):
        items = line.split()
        datasetName = items[0]
        items.remove(datasetName)
        if datasetName != self.dataset:
            raise Exception(" -- Logical mistake: mismatching dataset names")
        
        group = items[0]
        mtime = int(items[1])
        size = float(items[2])
        files = int(items[3])
        valid = int(items[4])
        custd = int(items[5])
        t2Site = items[6]
        self.updateForSite(t2Site,size,group,mtime,files,custd,valid)
