#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script looks at the unified information from PhEDEx database and popularity servive and ranks
# the datasets based on their usage pattern at all sites where the dataset is hosted in AnalysisOps
#
# It prints the results in a format that is easily readable as web page The highiest ranks datasets
# will be targeted for deletions first.
#
# Issues: printing needs to be put in more compact form
#
# It handles only one group: AnalysisOps
#----------------------------------------------------------------------------------------------------
import os, sys, re, time, datetime, glob, pickle
import datasetProperties, siteStatus

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']

if len(sys.argv) < 2:
    allSites = siteStatus.getAllSites()
else:
    strAllSites = str(sys.argv[1])
    allSites = pickle.loads(strAllSites)

now = float(time.time())

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def compare(item1, item2):
    s1 = datasets[item1].getGlobalRank()
    s2 = datasets[item2].getGlobalRank()
    
    if s1 < s2:
        return 1
    elif s1 > s2:
        return -1
    else:
        return 0

#====================================================================================================
# M A I N
#====================================================================================================
secondsPerDay = 60*60*24

# find all sites that ranked this dataset locally and contruct global ranking and substitute local
# ranking for global ones.

datasets = {}

# look inside phedex output and log in all datasets
inputFile = statusDirectory + '/'+os.environ['DETOX_PHEDEX_CACHE']
fileHandle = open(inputFile,"r")
for line in fileHandle.xreadlines():

    items = line.split()
    datasetName = items[0]
    items.remove(datasetName)

    # only consider datasets in the AnalysisOps space
    group = items[0]
    if group != "AnalysisOps":
        continue
    
    # do not consider datasets made by USERs
    user = re.findall(r"USER",datasetName)
    if len(user) > 0:
        continue

    # read relevant information
    creationDate = int(items[1])
    size = float(items[2])
    t2Site = items[3]

    # check whether we need to add another dataset
    if datasetName not in datasets.keys():
        datasets[datasetName] = datasetProperties.DatasetProperties(datasetName,size)

    # update dataset with proper weight
    datasets[datasetName].append([t2Site])
    datasets[datasetName].setWeightAtSites(t2Site,1)

fileHandle.close()


# now for each site read in the local ranks and update datasets
for site in allSites:
    if allSites[site].getStatus() == 0:
        continue
    inputFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']+'-local'

    fileHandle = open(inputFile, "r" )
    for line in fileHandle.xreadlines():
        items = line.split()
        datasetName = items[0]
        items.remove(datasetName)
        localRank = float(items[0])
        datasets[datasetName].setRankAtSites(site,localRank)
    fileHandle.close()

# now we loop over all datasets and rank them globaly
for datasetName in datasets.keys():
    globalRank = 0
    sumweight = 0 
    sites = datasets[datasetName].mySites()
    for site in sites:
        if site not in allSites.keys() or allSites[site].getStatus() == 0:
            continue
        rank = datasets[datasetName].myRankAtSites(site)
        weight = datasets[datasetName].myWeightAtSites(site)
        globalRank = globalRank + weight*rank
        sumweight = sumweight + weight
    if sumweight == 0:
        datasets[datasetName].setGlobalRank(globalRank)
    else:
        datasets[datasetName].setGlobalRank(globalRank/sumweight)

# now we update the file with ranks that used downstream
for site in allSites:
    if allSites[site].getStatus() == 0:
        continue
    outputFile = open(statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE'],'w')
    for datasetName in sorted(datasets.keys(), cmp=compare):
        sites = datasets[datasetName].mySites()
        if site not in sites:
            continue
        datasetRank = datasets[datasetName].getGlobalRank()
        size = datasets[datasetName].mySize()
    
        outputFile.write(datasetName+" "+str(round(datasetRank,2))+" "+str(round(size,2))+" ");
        for siteName in sites:
            outputFile.write(siteName + " ");
        outputFile.write(" \n");
    outputFile.close();
