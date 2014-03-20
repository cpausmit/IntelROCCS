#----------------------------------------------------------------------------------------------------
#
# This class provides support for storing datasets information extracted from PhEDEx
#
#----------------------------------------------------------------------------------------------------
class PhedexDataset:

    def __init__(self, dataset):
        self.dataset = dataset
        self.siteNames = []
        self.groupAtSite = {}
        self.sizeAtSite = {}
        self.validAtSite = {}
        self.custodialAtSite = {}
        self.makeTimeAtSite = {}

    def updateForSite(self,site,size,group,mtime,custodial,valid):
        if site not in self.siteNames:
            self.siteNames.append(site)
        if site in self.groupAtSite.keys():
            if self.groupAtSite[site] != group:
                print "!!! Group mismatch !!!"
                print self.dataset + ' ' + site + ' ' + self.groupAtSite[site] + \
                                      ' ' + group
        self.groupAtSite[site] = group

        if site in self.sizeAtSite.keys():
            self.sizeAtSite[site] = self.sizeAtSite[site] + size
        else:
            self.sizeAtSite[site] = size
    
        if site in self.makeTimeAtSite.keys(): 
            if mtime < self.makeTimeAtSite[site]:
                self.makeTimeAtSite[site] = mtime
        else:
            self.makeTimeAtSite[site] = mtime    

        self.setValid(site,valid)
        self.setCustodial(site,valid)

    def locatedOnSites(self):
        validSites = []
        for site in self.siteNames:
            if self.validAtSite[site]:
                validSites.append(site)
        return validSites

    def setValid(self,site,valid):
        if site in self.validAtSite.keys():
            if self.validAtSite[site] == True:
                self.validAtSite[site] = valid
        else:
            self.validAtSite[site] = valid

    def setCustodial(self,site,custodial):
        if site in self.custodialAtSite.keys():
            if self.custodialAtSite[site] == False:
                self.custodialAtSite[site] = custodial
        else:
            self.custodialAtSite[site] = custodial
        
    def size(self,site):
        if site in self.sizeAtSite.keys():
            return self.sizeAtSite[site]
        return 0

    def group(self,site):
        if site in self.groupAtSite.keys():
            return self.groupAtSite[site]
        return 'undef'

    def creationTime(self,site):
        if site in self.makeTimeAtSite.keys():
            return self.makeTimeAtSite[site]
        return -999

    def isValid(self,site):
        if site in self.validAtSite.keys():
            return self.validAtSite[site]
        return False

    def isCustodial(self,site):
        if site in self.custodialAtSite.keys():
            return self.custodialAtSite[site]
        return True

    def printIfDifferent(self):
        if len(self.siteNames) < 1:
            return
        size = self.sizeAtSite[self.siteNames[0]]
        crdate = self.makeTimeAtSite[self.siteNames[0]]
        different = False
        for site in self.siteNames:
            if self.sizeAtSite[site] != size:
                different = True
                break
            if self.makeTimeAtSite[site] != crdate:
                break    
        if different:
            print self.dataset
            print self.sizeAtSite
