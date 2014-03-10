#====================================================================================================
#  C L A S S E S  concerning the site description
#====================================================================================================

#---------------------------------------------------------------------------------------------------
"""
Class:  SiteProperties(siteName='')
Each site will be fully described for our application in this class.
"""
#---------------------------------------------------------------------------------------------------
class SiteProperties:
    "A SiteProperties defines all needed site properties."

    def __init__(self, siteName):
        self.name = siteName
        self.datasetRanks = {}
        self.datasetSizes = {}
        self.wishList = []
        self.datasetsToDelete = []
        self.protectedList = []
        self.siteSizeGbV = 0
        self.spaceTakenV = 0
        self.spaceLCp = 0
        self.space2free = 0
        self.deleted = 0
        self.protected = 0
        
    def addDataset(self,set,rank,size):
        self.datasetRanks[set] = rank
        self.datasetSizes[set] = size
        self.spaceTakenV = self.spaceTakenV + size

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
            space = space + self.datasetSizes[datasetName]
            self.wishList.append(datasetName)

    def onWishList(self,set):
        if set in self.wishList:
            return True
        return False

    def onProtectedList(self,set):
        if set in self.protectedList:
            return True
        return False
	
    def wantToDelete(self):
        if self.deleted < self.space2free:
            return True
        else:
            return False
            
    def grantWish(self,set):
        if set in self.protectedList:
            return
        if set in self.datasetsToDelete:
            return
        self.datasetsToDelete.append(set)
        self.deleted = self.deleted + self.datasetSizes[set]

    def pinDataset(self,set):
        if set in self.datasetsToDelete:
            return False
        self.protectedList.append(set)
        self.protected = self.protected + self.datasetSizes[set]
        if set in self.wishList:
            self.wishList.remove(set)
        return True
	
    def lastCopySpace(self,datasets,nCopyMin):
        space = 0
        for set in self.datasetSizes.keys():
            if set in self.datasetsToDelete:
                continue
            dataset = datasets[set]
            remaining = dataset.nSites() - dataset.nBeDeleted()
            if remaining <= nCopyMin:
                space = space + self.datasetSizes[set]
	self.spaceLCp = space
	
    def setSiteSize(self,size):
        self.siteSizeGbV = size

    def setSizeToDelete(self,size):
        self.space2free = size
	
    def siteSizeGb(self):
        return self.siteSizeGbV
	
    def dsetRank(self,set):
	return self.datasetRanks[set]

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

    def hasDataset(self,set):
        if set in self.datasetRanks.keys():
            return True
        else:
            return False

    def willDelete(self,set):
        if set in self.datasetsToDelete:
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

    def compare(self,item1, item2):
        r1 = self.datasetRanks[item1]
        r2 = self.datasetRanks[item2]
        if r1 < r2:
            return 1
        elif r1 > r2:
            return -1
        else:
            return 0
