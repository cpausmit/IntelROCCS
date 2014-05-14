#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script takes each site's ranked list and comes up with lists of datasets to be deleted. It
# makes sure that each dataset exists on at least 2 sites (there are datasets that currently exist
# on only one Tier2).
#
# The creation of a unified list is an iterative algorithm.  The deletion lists are made by looking
# at each site wish list of deletions.  Then the the datasets that can't be deleted will be
# determined and removed from deletion lists. Then the process repeats until the desired deleted
# space target is reached. Normally only 2 iterations are needed.
#
# Issue: printing of the deletion lists should be mode more compact
#---------------------------------------------------------------------------------------------------
import sys, os, re, glob, time, glob, shutil, MySQLdb
import datetime
import pickle
import siteStatus
from   datetime import date, timedelta

import siteProperties, datasetProperties

debug = 0

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# convert non-string environment variables
DETOX_NCOPY_MIN = int(os.environ['DETOX_NCOPY_MIN'])
DETOX_USAGE_MIN = float(os.environ['DETOX_USAGE_MIN'])
DETOX_USAGE_MAX = float(os.environ['DETOX_USAGE_MAX'])

# Fast has access to sites and datasets
sites = {}
datasets = {}

# Directories we work in
statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']

if len(sys.argv) < 2:
    allSites = siteStatus.getAllSites()
else:
    strAllSites = str(sys.argv[1])
    allSites = pickle.loads(strAllSites)

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def sortByProtected(item1,item2):
    r1 = sites[item1].spaceFree()
    r2 = sites[item2].spaceFree()
    if r1 < r2:
        return 1
    elif r1 > r2:
        return -1
    else:
        return 0

def makeDeletionLists(iteration):

    for site in sites.keys():
        sites[site].makeWishList()

    for datasetName in datasets.keys():
        dataset = datasets[datasetName]
        count_wishes = 0
        for site in sites.keys():
            siteObj = sites[site]
            if(siteObj.onWishList(datasetName)):
                count_wishes = count_wishes + 1

        if dataset.nSites()-dataset.nBeDeleted() - count_wishes > (DETOX_NCOPY_MIN-1):
            # grant wishes to all sites
            for site in sites.keys() :
                siteObj = sites[site]
                if siteObj.onWishList(datasetName):
                    siteObj.grantWish(datasetName)
                    dataset.addDelTarget(site)
        else:
            # here all sites want to delete this set need to pick one to host this set
            # and grant wishes to others pick one with the least space of protected
            # datasets
            nprotected = 0
            for site in sorted(sites.keys(), cmp=sortByProtected):
                siteObj = sites[site]
                if debug>0:
                    print site
                    print datasetName
                    print dataset.mySites()
                if site in dataset.mySites():
                    if debug>0:
                        print siteObj.datasetSizes.keys()
                    if siteObj.pinDataset(datasetName):
                        nprotected = nprotected + 1
                        if nprotected >= DETOX_NCOPY_MIN:
                            break
            for site in sites.keys() :
                siteObj = sites[site]
                if(siteObj.onWishList(datasetName)):
                    siteObj.grantWish(datasetName)
                    dataset.addDelTarget(site)

#====================================================================================================
#  M A I N
#====================================================================================================


# all sites that participate and create their objects

inputFiles = glob.glob(statusDirectory + '/*/' + os.environ['DETOX_DATASETS_TO_DELETE'])
for inputFile in inputFiles:
    site = inputFile.split('/')[-2]
    if debug>0:
        print " File: %s  Site: %s"%(inputFile,site)
    if site not in allSites.keys() or allSites[site].getStatus() == 0:
        continue
    sites[site] = siteProperties.SiteProperties(site)


# now open each site and read in all datasets that it has filling the site object created earlier

for inputFile in inputFiles:
    siteInput = inputFile.split('/')[-2]
    siteObj =  sites[siteInput]
    fileHandle = open(inputFile,"r")
    for line in fileHandle.xreadlines():
        items = line.split()
        datasetName = items[0]
        items.remove(datasetName)
        var = items[0]
        items.remove(var)
        datasetRank = float(var)
        var = items[0]
        items.remove(var)
        datasetSize = float(var)
        siteList = items

        siteObj.addDataset(datasetName,datasetRank,datasetSize)

        if datasetName not in datasets.keys():
            datasets[datasetName] = datasetProperties.DatasetProperties(datasetName,datasetSize)

        datasets[datasetName].append(siteList)

    fileHandle.close()

# once we have all datasets in, it is time to determine size for this site and figure out how much
# to be deleted

for site in sites:
    size = allSites[site].getSize()
    siteObj = sites[site]
    siteObj.setSiteSize(size)
    taken = siteObj.spaceTaken()
    if taken < DETOX_USAGE_MAX*size :
        siteObj.setSpaceToFree(-1)
    else:
        size2del = sites[site].spaceTaken() - size*DETOX_USAGE_MIN
        siteObj.setSpaceToFree(size2del)

for i in range(1, 4):         # this is not a safe implementation!
    makeDeletionLists(i)

# now it all done, calculate for each site space taken by last copies

for site in sorted(sites.keys(), key=str.lower, reverse=False):
    siteObj = sites[site]
    siteObj.lastCopySpace(datasets,DETOX_NCOPY_MIN)

# now print all the information but first wipe the directory

beDeleted = glob.glob(resultDirectory + "/*")
for subd in beDeleted:
    shutil.rmtree(subd)

today = str(datetime.date.today())
ttime = time.strftime("%H:%M")
for site in sorted(sites.keys(), key=str.lower, reverse=False):
    siteObj = sites[site]
    sitedir = resultDirectory + "/" + site
    if not os.path.exists(sitedir):
        os.mkdir(sitedir)
    file_timest = sitedir + "/Summary.txt"
    file_remain = sitedir + "/RemainingDatasets.txt"
    file_delete = sitedir + "/DeleteDatasets.txt"
    file_protected = sitedir + "/ProtectedDatasets.txt"

    outputFile = open(file_timest,'w')
    outputFile.write('#- ' + today + " " + ttime + "\n\n")
    outputFile.write("#- D E L E T I O N  P A R A M E T E R S ----\n\n")
    outputFile.write("Upper Threshold     : %8.2f\n"%(DETOX_USAGE_MAX))
    outputFile.write("Lower Threshold     : %8.2f\n"%(DETOX_USAGE_MIN))
    outputFile.write("\n")
    outputFile.write("#- S P A C E  S U M M A R Y ----------------\n\n")
    outputFile.write("Total Space     [TB]: %8.2f\n"%(siteObj.siteSizeGb()/1024))
    outputFile.write("Space Used      [TB]: %8.2f\n"%(siteObj.spaceTaken()/1024))
    outputFile.write("Space to delete [TB]: %8.2f\n"%(siteObj.spaceDeleted()/1024))
    outputFile.write("Space last CP   [TB]: %8.2f\n"%(siteObj.spaceLastCp()/1024))
    outputFile.close()

    outputFile = open(file_delete,'w')
    outputFile.write("# -- " + today + " " + ttime + "\n#\n")
    outputFile.write("#   Rank      Size nsites nsites  DatasetName \n")
    outputFile.write("#[~days]      [GB] before after               \n")
    outputFile.write("#---------------------------------------------\n")
    for dset in siteObj.delTargets():
        dataset = datasets[dset]
        rank = siteObj.dsetRank(dset)
        size = dataset.mySize()
        nsites = dataset.nSites()
        ndeletes = dataset.nBeDeleted()
        outputFile.write("%8.1f %9.1f %6d %6d  %s\n"\
                 %(rank,size,nsites,nsites-ndeletes,dset))
    outputFile.close()

    outputFile = open(file_remain,'w')
    outputFile.write("# -- " + today + " " + ttime + "\n\n")
    outputFile.write("#   Rank      Size nsites nsites  DatasetName \n")
    outputFile.write("#[~days]      [GB] before after               \n")
    outputFile.write("#---------------------------------------------\n")
    delTargets = siteObj.delTargets()
    for dset in siteObj.allSets():
        if dset in delTargets: continue
        dataset = datasets[dset]
        rank = siteObj.dsetRank(dset)
        size = dataset.mySize()
        nsites = dataset.nSites()
        ndeletes = dataset.nBeDeleted()
        outputFile.write("%8.1f %9.1f %6d %6d  %s\n"\
                 %(rank,size,nsites,nsites-ndeletes,dset))
    outputFile.close()
