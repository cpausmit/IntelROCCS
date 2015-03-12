#====================================================================================================
#  C L A S S E S  concerning the site description
#====================================================================================================

#---------------------------------------------------------------------------------------------------
"""
Class:  SiteProperties(siteName='')
Each site will be fully described for our application in this class.
"""
#---------------------------------------------------------------------------------------------------
import time, math

class SiteProperties:
    "A SiteProperties defines all needed site properties."

    def __init__(self, siteName):
        self.name = siteName
        self.datasetRanks = {}
        self.datasetSizes = {}
        self.dsetIsValid = {}
        self.dsetIsCustodial = {}
        self.dsetIsPartial = {}
        self.deprecated = {}
        self.dsetReqTime = {}
        self.dsetUpdTime = {}
        self.dsetIsDone = {}
        self.wishList = []
        self.datasetsToDelete = []
        self.protectedList = []
        self.siteSizeGbV = 0
        self.spaceTakenV = 0
        self.spaceNotUsed = 0
        self.spaceLCp = 0
        self.space2free = 0
        self.deleted = 0
        self.protected = 0
        self.epochTime = int(time.time())

    def addDataset(self,dset,rank,size,valid,partial,custodial,depr,reqtime,updtime,wasused,isdone):
        self.dsetIsValid[dset] = valid
        self.dsetIsPartial[dset] = partial
        self.dsetIsCustodial[dset] = custodial
        self.datasetRanks[dset] = rank
        self.datasetSizes[dset] = size
        if depr:
            self.deprecated[dset] = depr
        self.spaceTakenV = self.spaceTakenV + size
        self.dsetIsDone[dset] = isdone
        self.dsetReqTime[dset] = reqtime
        self.dsetUpdTime[dset] = updtime

        if wasused == 0:
            self.spaceNotUsed = self.spaceNotUsed + size

    def makeWishList(self):
        space = 0
        self.wishList = []
        for datasetName in sorted(self.datasetRanks.keys(), cmp=self.compare):
            if space > (self.space2free-self.deleted):
                break
            if datasetName in self.datasetsToDelete:
                continue
            if datasetName in self.protectedList:
                continue
            #custodial set can't be on deletion wish list
            if self.dsetIsCustodial[datasetName] :
                continue
            #non-valid dataset can't be on deletion list
            if not self.dsetIsValid[datasetName]:
                continue

            space = space + self.datasetSizes[datasetName]
            self.wishList.append(datasetName)

    def hasMoreToDelete(self):
        nsets = 0
        for datasetName in sorted(self.datasetRanks.keys(), cmp=self.compare):
            if datasetName in self.datasetsToDelete:
                continue
            if datasetName in self.protectedList:
                continue
            #custodial set can't be on deletion wish list
            if self.dsetIsCustodial[datasetName] :
                continue
            #non-valid dataset can't be on deletion list
            if not self.dsetIsValid[datasetName]:
                continue
            if datasetName in self.wishList:
                continue
            nsets = nsets + 1
        if nsets > 0:
            return True
        return False

    def onWishList(self,dset):
        if dset in self.wishList:
            return True
        return False

    def onProtectedList(self,dset):
        if dset in self.protectedList:
            return True
        return False

    def wantToDelete(self):
        if self.deleted < self.space2free:
            return True
        else:
            return False

    def grantWish(self,dset):
        if dset in self.protectedList:
            return
        if dset in self.datasetsToDelete:
            return
        self.datasetsToDelete.append(dset)
        self.deleted = self.deleted + self.datasetSizes[dset]

    def revokeWish(self,dset):
        if dset in self.datasetsToDelete:
            self.datasetsToDelete.remove(dset)
            self.deleted = self.deleted - self.datasetSizes[dset]

    def pinDataset(self,dset):
        if dset in self.datasetsToDelete:
            return False

        #can't pin partial dataset
        if self.dsetIsPartial[dset] :
            return False
        #can't pin non-valid dataset
        if not self.dsetIsValid[dset]:
            return False

        self.protectedList.append(dset)
        self.protected = self.protected + self.datasetSizes[dset]
        if dset in self.wishList:
            self.wishList.remove(dset)
        return True

    def lastCopySpace(self,datasets,nCopyMin):
        space = 0
        for dset in self.datasetSizes.keys():
            if dset in self.datasetsToDelete:
                continue
            dataset = datasets[dset]
            remaining = dataset.nSites() - dataset.nBeDeleted()
            if remaining <= nCopyMin:
                space = space + self.datasetSizes[dset]
	self.spaceLCp = space

    def setSiteSize(self,size):
        self.siteSizeGbV = size

    def siteSizeGb(self):
        return self.siteSizeGbV

    def dsetRank(self,set):
	return self.datasetRanks[set]

    def dsetSize(self,set):
        return self.datasetSizes[set]

    def isPartial(self,set):
        return self.dsetIsPartial[set]

    def siteName(self):
        return self.name

    def spaceTaken(self):
        return self.spaceTakenV

    def spaceDeleted(self):
        return self.deleted

    def spaceProtected(self):
        return self.protected

    def spaceFree(self):
        return self.siteSizeGbV - (self.spaceTakenV - self.deleted)

    def spaceLastCp(self):
        return self.spaceLCp

    def isDeprecated(self,dset):
        if dset in self.deprecated:
            return True
        return False

    def spaceDeprecated(self):
        size = 0
        for dset in self.deprecated:
            size = size + self.datasetSizes[dset]
        return size

    def spaceIncomplete(self):
        size = 0;
        for dset in self.dsetIsPartial:
            if self.dsetIsPartial[dset]:
                size = size + self.datasetSizes[dset]
        return size
    
    def nsetsDeprecated(self):
        nsets = 0
        for dset in self.deprecated:
            nsets = nsets + 1
        return nsets
        
    def hasDataset(self,dset):
        if dset in self.datasetRanks:
            return True
        else:
            return False

    def willDelete(self,dset):
        if dset in self.datasetsToDelete:
            return True
        else:
            return False

    def allSets(self):
        return sorted(self.datasetRanks.keys(), cmp=self.compare)

    def delTargets(self):
        return sorted(self.datasetsToDelete, cmp=self.compare)

    def protectedSets(self):
        return sorted(self.protectedList, cmp=self.compare)

    def setSpaceToFree(self,size):
        self.space2free = size

    def reqTime(self,dset):
        return self.dsetReqTime[dset]
    
    def dsetLoadTime(self,dset):
        return (self.dsetUpdTime[dset] - self.dsetReqTime[dset])

    def spaceUnused(self):
        return self.spaceNotUsed

    def dsetIsStuck(self,dset):
        if self.dsetIsDone[dset] == 0:
            reqtime = self.dsetReqTime[dset]
            if (self.epochTime - reqtime) > 60*60*24*14:
                return 1
        return 0

    def considerForStats(self,dset):
        if self.dsetLoadTime(dset) > 60*60*24*14:
            return False
        if self.dsetLoadTime(dset) <= 0:
            return False
        if (self.epochTime - self.dsetReqTime[dset]) > 60*60*24*90:
            return False
        return True

    def getDownloadStats(self):
        loadSize = 0
        loadTime = 0
        stuck = 0
        for dset in self.datasetSizes:
            if self.dsetIsStuck(dset) == 1:
                stuck = stuck + 1
                continue
            if not self.considerForStats(dset):
                continue

            if self.datasetSizes[dset] > 10:
                loadSize = loadSize + self.datasetSizes[dset]
                loadTime = loadTime + self.dsetLoadTime(dset)

        speed = 0
        if loadTime > 0:
            speed = loadSize/loadTime*(60*60*24)
        return (speed, loadSize, stuck)
      
    def getAverage(self,array):
        if len(array) < 3: return 0
        sortA = sorted(array)

        diff = 100
        prevMean = sortA[len(sortA)/2]
        prevRms = sortA[len(sortA)-1] - sortA[0]
        print sortA
        while diff > 0.01:
            ave = 0
            aveSq = 0
            nit = 0
            for i in range(1, len(sortA)):
                if abs(sortA[i] - prevMean) > 1.6*prevRms:
                    continue
                ave = ave + sortA[i]
                aveSq = aveSq + sortA[i]*sortA[i]
                nit = nit + 1
            ave = ave/nit
            rms = math.sqrt(aveSq/nit - ave*ave)
            diff = abs(ave - prevMean)/prevMean
            prevMean = ave
            prevRms = rms
                
        return prevMean
            

    def compare(self,item1, item2):
        r1 = self.datasetRanks[item1]
        r2 = self.datasetRanks[item2]
        if r1 < r2:
            return 1
        elif r1 > r2:
            return -1
        else:
            return 0
