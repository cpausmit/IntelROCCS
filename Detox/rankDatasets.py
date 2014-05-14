#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script looks at the unified information from PhEDEx database and popularity servive and ranks
# the datasets based on their usage pattern and size at a particular site. It is "selfish" approach
# from the site point of view because it creates deletion suggestions based only on this site view.
#
# It prints the results in a format that is easily readable as web page The highiest ranks dayasets
# will be targeted for deletions first.
#
# Issues: printing needs to be put in more compact form
#
# WARNING: notice that rigth now it handles only one group: AnalysisOps (it is hardcoded)
#----------------------------------------------------------------------------------------------------
import os, sys, re, time, datetime, shutil

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

if len(sys.argv) < 2:
    print '\n not enough arguments\n\n' # please add a more useful output with example
    sys.exit()
else:
    site = str(sys.argv[1])

statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']

# Globally used variables (bad style.. should be all in arguments, needs restructuring)

allDatasets = {}
usedDatasets = {}
allGroups = {}
groupSizes= {}
sitesForDatasets = {}

groupTotal = 0
needsToClean = 40000
now = float(time.time())
groupLimits = {}

#====================================================================================================
#  H E L P E R S
#====================================================================================================

def translate(group):
    if group in groupLimits:
        return group
    else:
        return table[group]

def compare(item1, item2):
    s1 = (allDatasets[item1])[3]
    s2 = (allDatasets[item2])[3]
    if s1 < s2:
        return 1
    elif s1 > s2:
        return -1
    else:
        return 0

def printDatasets():
    outputFile = open(statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE'],'w')
    for datasetName in sorted(allDatasets.keys(), cmp=compare):
        group = (allDatasets[datasetName])[0]
        datasetRank = (allDatasets[datasetName])[3]
        size = (allDatasets[datasetName])[2]
        sites = sitesForDatasets[datasetName]

        if group != "AnalysisOps":
            continue

        outputFile.write(datasetName+" "+str(round(datasetRank,2))+" "+str(round(size,2))+" ");
        for siteName in sites:
             outputFile.write(siteName + " ");
        outputFile.write(" \n");
    outputFile.close();

    origFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']
    copyFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']+'-local'
    shutil.copy2(origFile,copyFile)
   
    return;

#====================================================================================================
#  M A I N
#====================================================================================================

inputFile = statusDirectory + '/' + site + '/' + os.environ['DETOX_USED_DATASETS']

if not os.path.exists(inputFile):
    print ' ERROR -- input file:  %s  did not open'%\
          (statusDirectory + '/' + site + '/' + os.environ['DETOX_USED_DATASETS'])
    sys.exit(0)

fileHandle = open(inputFile, "r" )
for line in fileHandle.xreadlines():
    items = line.split()
    datasetName = items[0]
    items.remove(datasetName)
    usedDatasets[datasetName] = items
fileHandle.close()

inputFile = statusDirectory + '/'+os.environ['DETOX_PHEDEX_CACHE']
fileHandle = open(inputFile,"r")
secondsPerDay = 60*60*24

for line in fileHandle.xreadlines():
    items = line.split()
    datasetName = items[0]
    items.remove(datasetName)
    
    group = items[0]
    if group != "AnalysisOps":
            continue
    
    creationDate = int(items[1])
    size = float(items[2])
    t2Site = items[3]

    if datasetName in sitesForDatasets.keys():
        sitesForDatasets[datasetName].append(t2Site)
    else:
        sitesForDatasets[datasetName] = [t2Site]

    
    if t2Site != site:
        continue

    used = 1
    nAccessed = 0
    lastAccessed = now
    if datasetName in usedDatasets.keys():
        nAccessed = float((usedDatasets[datasetName])[0])
        if size > 1:
            nAccessed = nAccessed/size
        date = (usedDatasets[datasetName])[1]
        dateTime = date+' 00:00:01'
        pattern = '%Y-%m-%d %H:%M:%S'
        lastAccessed = int(time.mktime(time.strptime(dateTime, pattern)))
        
        if (now-creationDate)/secondsPerDay < ((now - lastAccessed)/secondsPerDay-nAccessed):
            used = 0
    else:
        used = 0

    # calculate the rank of the given dataset according to its access patterns and size
    datasetRank = (1-used)*(now-creationDate)/(60*60*24) + \
		  used*( (now-lastAccessed)/(60*60*24)-nAccessed) - size/100
    allDatasets[datasetName] = [group,creationDate,size,datasetRank,used]
    
fileHandle.close();

# printout the datasets
printDatasets()
