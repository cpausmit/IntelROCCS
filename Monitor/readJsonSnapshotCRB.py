#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script reads the information from a set of JSON snapshot files that we use to cache our
# popularity information.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, glob, subprocess, time, json, pprint, MySQLdb
import datasetProperties
from   xml.etree import ElementTree
from findDatasetHistoryAll import *
import findDatasetProperties as fDP
genesis=1378008000

if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# get the dataset pattern to consider (careful the pattern will be translated, better implementation
# should be done at some point)
datasetPattern = os.environ.get('MONITOR_PATTERN')
datasetPattern = datasetPattern.replace("_",".*")
datasetPattern = r'/.*/.*/%s$'%(datasetPattern)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def processPhedexCacheFile(fileName,debug=0):
    # processing the contents of a simple file into a hash array

    sizesPerSite = {}
    iFile = open(fileName,'r')
    # loop through all lines
    for line in iFile.xreadlines():
        line = line[:-1]
        f = line.split()
        if len(f) == 9:
            dataset = f[0]
            group   = f[1]
            size    = float(f[2])
            nFiles  = int(f[3])
            custd   = int(f[4])
            site    = f[5]
            date    = int(f[6])
            valid   = int(f[8])

        else:
            print 'Columns not equal 9: \n %s'%(line)
            sys.exit(1)

        # first step, find the sizes per site per dataset hash array
        if site in sizesPerSite:
            sizesPerSitePerDataset = sizesPerSite[site]
        else:
            sizesPerSitePerDataset = {}
            sizesPerSite[site] = sizesPerSitePerDataset

        if dataset in sizesPerSitePerDataset:
            sizesPerSitePerDataset[dataset] += size
        else:
            sizesPerSitePerDataset[dataset]  = size

    iFile.close();

    # return the sizes per site
    return sizesPerSite

def processFile(fileName,debug=0):
    # processing the contents of a simple file into a hash array

    nSkipped  = 0
    nAccessed = {} # the result is a hash array

    # read the data from the json file
    with open(fileName) as data_file:
        data = json.load(data_file)

    # generic full print (careful this can take very long)
    if debug>1:
        pprint.pprint(data)

    if debug>0:
        print " SiteName: " + data["SITENAME"]

    # loop through the full json data an extract the accesses per dataset
    for entry in data["DATA"]:
        key = entry["COLLNAME"]
        value = entry["NACC"]

        if debug>0:
            print " NAccess - %6d %s"%(value,key)
        # filter out non-AOD data
        if not re.match(datasetPattern,key):
            if debug>0:
                print ' WARNING -- rejecting *USER* type data: ' + key
            nSkipped += 1
            continue

        # here is where we assign the values to the hash (checking if duplicates are registered)
        if key in nAccessed:
            print ' WARNING - suspicious entry - the entry exists already (continue)'
            nAccessed[key] += value
        else:
            nAccessed[key] = value

    # return the datasets
    return (nSkipped, nAccessed)

def addData(nAllAccessed,nAccessed,debug=0):
    # adding a hash array (nAccessed) to the mother of all hash arrays (nAllAccessed)

    # loop through the hash array
    for key in nAccessed:
        # add the entries to our all access hash array
        if key in nAllAccessed:
            nAllAccessed[key] += nAccessed[key]
        else:
            nAllAccessed[key] = nAccessed[key]

    # return the updated all hash array
    return nAllAccessed


def addSites(nSites,nAccessed,debug=0):
    # adding up the number of sites for each dataset

    # loop through the hash array
    for key in nAccessed:
        # add the entries to our all access hash array

        if key in nSites:
            nSites[key] += 1
        else:
            nSites[key] = 1
    # return the updated all hash array
    return nSites

def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    # this is the text including the size units, that need to be converted
    sizeGb  = float(sizeTxt[0:-2])
    units   = sizeTxt[-2:]
    # decide what to do for the given unit
    if   units == 'MB':
        sizeGb = sizeGb/1000.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1000.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(0)

    # return the size in GB as a float
    return sizeGb

def findDatasetSize(dataset,debug=0):
    # find the file size of a given data set (dataset) and return the number of files in the dataset
    # and its size in GB
    nFiles,sizeGb,averageSizeGb = fDP.findDatasetProperties(dataset,False)
    return nFiles, sizeGb

def findDatasetCreationTime(dataset,fileName,cTimes,debug=0):
    # determine the creation time for each dataset

    if dataset in cTimes:
        return cTimes[dataset]
    cmd = os.environ.get('MONITOR_BASE') + \
        '/das_client.py --format=plain --limit=0 --query="dataset=' + dataset + \
        ' | grep dataset.creation_time " '
    print cmd
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        try:
            cTime = time.mktime(time.strptime(line,'%Y-%m-%d %H:%M:%S\n'))
            with open(fileName,'a') as dataFile:
                dataFile.write("%s %i\n"%(dataset,cTime))
            cTimes[dataset] = cTime
            return cTime
        except ValueError:
            # bad response; assume it was always there
            print line
            cTime = genesis
            with open(fileName,'a') as dataFile:
                dataFile.write("%s %i\n"%(dataset,cTime))
            cTimes[dataset] = cTime
            return cTime

def readDatasetCreationTimes(fileName,debug=0):
    # read the creation time for each dataset from a given file

    creationTimes={}
    if not os.path.exists(fileName):
        return creationTimes
    dataFile = open(fileName,'r')
    for line in dataFile.readlines():
        line=re.split('[ \n]',line)
        try:
            creationTimes[line[0]] = int(line[1])
        except:
            print line
            raise ValueError
    dataFile.close()
    return creationTimes

def getDbCursor():
    # get access to the detox database

    # configuration
    db = os.environ.get('DETOX_SITESTORAGE_DB')
    server = os.environ.get('DETOX_SITESTORAGE_SERVER')
    user = os.environ.get('DETOX_SITESTORAGE_USER')
    pw = os.environ.get('DETOX_SITESTORAGE_PW')
    # open database connection
    db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    return db.cursor()

def readDatasetProperties():
    # read dataset properties fromt he database

    sizesGb     = {}
    fileNumbers = {}

    # get access to the database
    cursor = getDbCursor()
    sql  = "select Datasets.DatasetName, DatasetProperties.NFiles, DatasetProperties.Size "
    sql += "from Datasets join DatasetProperties "
    sql += "on Datasets.DatasetId = DatasetProperties.DatasetId";
    # go ahead and try
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            dataset = row[0]
            nFiles  = row[1]
            sizeGb  = row[2]
            # add to our memory
            # print dataset,nFiles,sizeGb
            fileNumbers[dataset] = int(nFiles)
            sizesGb[dataset]     = float(sizeGb)
    except:
        pass

    return (fileNumbers, sizesGb)

def calculateAverageNumberOfSites(sitePattern,datasetSet,datasetsOnSites,fullStart,end,cTimes={}):
    # calculate the average number of replicas (sites) for a dataset in a given time interval

    print ' Relevant time interval: %d %d  --> %d'%(fullStart,end,end-fullStart)

    # convert it into floats and take care of possible rounding issues
    fullInterval = end - fullStart
    datasetMovement = {}
    predictedDatasetsOnSites={}
    for datasetName in datasetSet:
        datasetMovement[datasetName]={}
        predictedDatasetsOnSites[datasetName]=set([])
    nowish = time.time()
    print "requesting time %i"%(int(genesis))
    fileName = os.environ.get('MONITOR_DB') + '/datasets/' + 'delRequests_%i'%(int(genesis))+'.json'
    parseRequestJson(fileName,genesis,end,False,datasetMovement,datasetPattern,datasetSet)
    fileName = os.environ.get('MONITOR_DB') + '/datasets/' + 'xferRequests_%i'%(int(genesis))+'.json'
    parseRequestJson(fileName,genesis,end,True,datasetMovement,datasetPattern,datasetSet)
    nSites = {}
    timeOnSites = {} #timeOnSites[dataset][site] = time

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for datasetName in datasetMovement:

        # don't penalize a dataset for not existing
        cTime = findDatasetCreationTime(datasetName,creationTimeCache,cTimes)
        start = max(fullStart,cTime)

        interval = end - start
        if not datasetName in nSites:
            nSites[datasetName] = 0
        if not datasetName in timeOnSites:
            timeOnSites[datasetName] = {}
        for siteName in datasetMovement[datasetName]:
            if not re.match(sitePattern,siteName):                      # only requested sites
                print ' WARNING - no site match. ' + siteName
                continue
            if not siteName in timeOnSites[datasetName]:
                timeOnSites[datasetName][siteName] = 0

            datasetMovement[datasetName][siteName][0].sort()            # sort lists to match items
            datasetMovement[datasetName][siteName][1].sort()

            xfers = datasetMovement[datasetName][siteName][0]
            dels =  datasetMovement[datasetName][siteName][1]
            xfers,dels = cleanHistories(xfers,dels,start,end)
            datasetMovement[datasetName][siteName][0] = xfers
            datasetMovement[datasetName][siteName][1] = dels
            lenXfer = len(xfers)
            lenDel  = len(dels)

            # find this site's fraction for nSites
            if not datasetName in nSites:
                nSites[datasetName] = 0

            siteSum = 0
            i       = 0
            if (lenXfer > 0 and lenDel > 0 and xfers[-1] > dels[-1]) or \
               (lenDel==0 and lenXfer > 0 and xfers[-1] < end):
                try:
                    predictedDatasetsOnSites[datasetName].add(siteName)
                except KeyError:
                    predictedDatasetsOnSites[datasetName] = set([siteName])
            # loop through the transfer/deletion intervals
            while i < lenXfer:
                try:
                    tXfer = datasetMovement[datasetName][siteName][0][i]
                    tDel  = datasetMovement[datasetName][siteName][1][i]
                except IndexError:
                    break
                i = i + 1                   # iterate before all continue statements
                # four ways to be in interval                 
                # (though this prevents you from having the same
                #  start and end date)
                if tXfer <= start <= end <= tDel:
                    siteSum += 1                                 # should happen at most once (?)
                elif tXfer <= start < tDel < end:
                    siteSum += float(tDel - start)/float(interval)
                elif start < tXfer < end <= tDel:
                    siteSum += float(end - tXfer)/float(interval)
                elif start < tXfer < tDel <= end:
                    siteSum += float(tDel - tXfer)/float(interval)
                else:                                            # have ensured tXfer > tDel
                    continue
            timeOnSites[datasetName][siteName] += siteSum
            nSites[datasetName] += siteSum
        if datasetName in datasetsOnSites:
            if datasetName in predictedDatasetsOnSites:
                diff = datasetsOnSites[datasetName] - predictedDatasetsOnSites[datasetName]
            else:
                diff = datasetsOnSites[datasetName]
        else:
            diff=set([])
        for siteName in diff:
            if not siteName in timeOnSites[datasetName]:
                timeOnSites[datasetName][siteName] = 0
            if siteName not in datasetMovement[datasetName]:
                # no phedex history, but the dataset exists (it was created there?)
                nSites[datasetName]+=max(0,(end - max(cTime,start))/interval)
                timeOnSites[datasetName][siteName]+=max(0,(end - max(cTime,start))/interval)
            elif len(datasetMovement[datasetName][siteName][1])==0:
                # it was never deleted...
                if len(datasetMovement[datasetName][siteName][0])==0:
                    # it was never transferred...
                    nSites[datasetName]+=max(0,(end - max(cTime,start))/interval)
                    timeOnSites[datasetName][siteName]+=max(0,(end - max(cTime,start))/interval)
                    # nSites[datasetName]+=1
                elif datasetMovement[datasetName][siteName][0][-1] < end:
                    # it was transferred recently?
                    nSites[datasetName] += \
                        (end-datasetMovement[datasetName][siteName][0][-1])/interval
                    timeOnSites[datasetName][siteName] += \
                        (end-datasetMovement[datasetName][siteName][0][-1])/interval

    n     = 0
    nSkip =  0
    Sum   = float(0)
    for datasetName in nSites:
        if nSites[datasetName] == 0:                            # dataset not on sites in interval
            nSkip += 1
            continue
        Sum += nSites[datasetName]
        n   += 1

    print '\n Dataset  --  average number of sites\n'

    if n != 0:
        avg = Sum/n
        print '\n overall average n sites: %f\n'%(avg)
        print '\n number of datasets:      %d\n'%(n)
        print '\n number of datasets skipped: %d\n'%(nSkip)
    else:
        print 'no datasets found in specified time interval'

    return nSites,timeOnSites

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
sizeAnalysis = True
addNotAccessedDatasets = True

usage  = "\n"
usage += " readJsonSnapshot.py  <sitePattern> [ <startDate> <endDate> ]\n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   startDate  - epoch time of starting date\n"
usage += "   endDate  - epoch time of ending date\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    start = 1378008000
    end = int(time.time())
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    start = int(sys.argv[2])
    end = int(time.time())
elif len(sys.argv)==4:
    site = str(sys.argv[1])
    start = int(sys.argv[2])
    end = int(sys.argv[3])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

siterx=re.sub("\*",".*",site) # to deal with stuff like T2* --> T2.*
# figure out which datases to consider using phedex cache as input
if addNotAccessedDatasets:
    fileName = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
           os.environ['DETOX_PHEDEX_CACHE']
    sizesPerSite = processPhedexCacheFile(fileName,debug=0)

# initialize our variables
nAllSkipped  = 0
nAllAccessed = {}
nSiteAccess  = {}
sizesGb      = {}
fileNumbers  = {}
if sizeAnalysis:
    (fileNumbers,sizesGb) = readDatasetProperties()

# form date regexps
starttmp = time.gmtime(start)
endtmp = time.gmtime(end)
dates=[]
for year in range(starttmp[0],endtmp[0]+1):
    for month in range(1,13):
        if year==starttmp[0] and month<starttmp[1]:
            continue
        elif year==endtmp[0] and month>endtmp[1]:
            continue
        else:
            dates.append("%i-%02i-??"%(year,month))

# --------------------------------------------------------------------------------------------------
# loop through all matching snapshot files
# --------------------------------------------------------------------------------------------------
# figure out which snapshot files to consider
workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
siteList = [ x.split("/")[-1] for x in glob.glob(workDirectory + '/'+site)]
files={}
for s in siteList:
    files[s] = []
    for date in dates:
        files[s] += glob.glob(workDirectory + '/' + s + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)
phedexFile = open(workDirectory+'/DatasetsInPhedexAtSites.dat','r')
creationTimeCache = os.environ['MONITOR_DB'] + '/datasets/creationTimes.txt'
# say what we are goign to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Pattern:      %s"%(datasetPattern)
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
if sizeAnalysis:
    print "\n Size Analysis is on. Please be patient, this might take a while!\n"


# datasetsOnSites[k]=v, where k is a dataset and v is a set of sites
# this can probably be done smarter and combined with nSites computation
datasetsOnSites={}
datasetSet=set([])
isDeleted={}
phedexSize=0
for line in phedexFile:
    l = line.split()
    # if not l[1]=="AnalysisOps":
    #     continue
    datasetName=l[0]
    siteName=l[5]
    if not re.match(datasetPattern,datasetName):
        continue
    if not re.match(siterx,siteName):
        continue
    datasetSet.add(datasetName) # we don't care if it doesn't currently exist on an interesting site
    isDeleted[datasetName]=0
    if datasetName in datasetsOnSites:
        datasetsOnSites[datasetName].add(siteName)
    else:
        datasetsOnSites[datasetName] = set([siteName])
    phedexSize+=float(l[2])
print "Phedex Size: ",phedexSize

getJsonFile("del",genesis) # all deletions
#delDatasetSet=getAnalysisOpsDeletions(start,end,datasetPattern) # use this if analysisops

delDatasetSet=getAllDeletions(start,end,datasetPattern)
datasetSet.update(delDatasetSet) # this should be every dataset, even the deleted ones
for ds in delDatasetSet:
    if ds not in isDeleted:
        isDeleted[ds] = 1 # if dataset is in delDatasetSet but was actually deleted

# removing blacklisted datasets (no DAS info)
blacklistFile = open(os.environ.get('MONITOR_DB')+'/datasets/blacklist.log','r')
blacklistSet = set(map(lambda x : x.split()[0], list(blacklistFile)))
datasetSet.difference_update(blacklistSet)
print "Removed blacklist"

for s in files:
    print ' Analyzing: '+s
    nAllAccessed[s]={}
    for fileName in sorted(files[s]):
        if debug>0:
          print ' Analyzing: ' + fileName
        g = fileName.split("/")
        siteName = g[-3]

        if siteName in nSiteAccess:
            nSiteAccessEntry = nSiteAccess[siteName]
        else:
            nSiteAccessEntry = {}
            nSiteAccess[siteName] = nSiteAccessEntry

        # analyze this file
        (nSkipped, nAccessed) = processFile(fileName,debug)
        # add the results to our 'all' record
        nAllSkipped      += nSkipped
        nSiteAccessEntry = addData(nSiteAccessEntry,nAccessed,debug)
        nAllAccessed[s]     = addData(nAllAccessed[s],    nAccessed,debug)
print "Determined access history"
# --------------------------------------------------------------------------------------------------
# add all datasets that are in phedex but have never been used
# --------------------------------------------------------------------------------------------------
if addNotAccessedDatasets:
    # these are number of non accessed samples with corresponding total size
    nAddedSamples = 0
    addedSize = 0.
    # these are number of non AOD samples we exclude
    nExcludedSamples = 0
    excludedSize = 0.
    for site in sizesPerSite:

        # get size specific record
        sizesPerSitePerDataset = sizesPerSite[site]

        # make sure that we are really interested in this site at all
        if site in nSiteAccess:
            nSiteAccessEntry = nSiteAccess[site]
        else:
            continue

        # loop through all datasets at the site and check'em
        for dataset in sizesPerSitePerDataset:
            # exclude non AOD samples
            if not re.match(datasetPattern,dataset):
                # update our local counters
                nExcludedSamples += 1
                excludedSize += sizesPerSitePerDataset[dataset]
                continue

            # only add information if the samples is not already available
            if dataset in nAllAccessed[site]:
                if debug>1:
                    print ' -> Not Adding : ' + dataset
            else:
                if debug>0:
                    print ' -> Adding : ' + dataset

                # update our local counters
                nAddedSamples += 1
                addedSize += sizesPerSitePerDataset[dataset]

                # make an entry in all of the relevant records
                nAllAccessed[site][dataset] = 0
                nSiteAccessEntry[dataset] = 0

    print " "
    print " - number of excluded datasets: %d"%(nExcludedSamples)
    print " - excluded sample size:        %d"%(excludedSize)
    print " "
    print " - number of added datasets:    %d"%(nAddedSamples)
    print " - added sample size:           %d"%(addedSize)

print "Determined unaccessed datasets"

# --------------------------------------------------------------------------------------------------
# create summary information and potentially print the contents
# --------------------------------------------------------------------------------------------------
nAll = 0
nAllAccess = 0
sizeTotalGb = 0.
nFilesTotal = 0
findDatasetHistoryAll(genesis)
counter=0
# for key in nAllAccessed:
for key in datasetSet:
    counter+=1
    nAll += 1
    if sizeAnalysis:
        if not key in fileNumbers:
            print counter
            (nFiles,sizeGb) = findDatasetSize(key)
            # add to our memory
            fileNumbers[key] = nFiles
            sizesGb[key]     = sizeGb
        else:
            nFiles = fileNumbers[key]
            sizeGb = sizesGb[key]
        # add up the total data volume/nFiles
        sizeTotalGb     += sizeGb
        nFilesTotal     += nFiles

# need to figure out how many replicas are out there
nSites = {}
for site in sorted(nSiteAccess):
    nSiteAccessEntry = nSiteAccess[site]
    nSites = addSites(nSites,nSiteAccessEntry,debug)

# load creation time cache
creationTimes=readDatasetCreationTimes(creationTimeCache)
for d in datasetSet:
    findDatasetCreationTime(d,creationTimeCache,creationTimes)

# figure out properly the 'average number of sites' in the given time interval
print "Computing average number of sites"
nAverageSites,timeOnSites = calculateAverageNumberOfSites(siterx,datasetSet,datasetsOnSites,start,end,creationTimes)

# adjust xrootd accesses
# ======================
# - determine all access per dataset that are through xrootd
# - remove recordings of sites that have xrootd access (remove by setting nAccess = -1)
# - find sites that hold the datasets for real
# - add the number of accesses equally distributed to all sites that have a copy of the dataset
for dataset in datasetSet:

    realSites = []
    nRealSites = 0
    nXrootdAccesses = 0
    timeDatasetOnSites = timeOnSites[dataset]

    # loop through all sites and see what the dataset has to contribute
    for site in siteList:
        try:
            nAccess = nAllAccessed[site][dataset]
        except KeyError:
            # when a dataset was not accessed at a given site
            nAccess = 0
            continue

        # now see whether the data is at the given site
        try:
            timeOnSite = timeDatasetOnSites[site]
            nRealSites += 1
            realSites.append(site)
        except KeyError:
            # these access numbers are from xrootd as the sample is not really at this site
            if nAccess > 0:
                nXrootdAccesses += nAccess
                nAllAccessed[site][dataset] = -1

    # redistribute the number of accesses
    if nRealSites > 0:
        nAdditional = float(nXrootdAccesses)/float(nRealSites)
        for site in realSites:
            nAllAccessed[site][dataset] += nAdditional
    else:
        print " No site records dataset %s -- xrootd at Tier-1?"%(dataset)


# last step: produce monitoring information
# ========================================
# ready to use: nAllAccessed[dataset], fileNumbers[dataset], sizesGb[dataset], nSites[dataset]

fileName = 'DatasetSummary.txt'
print ' Output file: ' + fileName
totalAccesses = open(fileName,'w')
totalAccessesByDS = open('DatasetSummaryByDS.txt','w')
for dataset in datasetSet:
    if dataset not in nSites:
        nSites[dataset] = 0 # if we haven't found it on a site, nSites=0

    nAccess = 0
    nSumAccess = 0
    timeDatasetOnSites = timeOnSites[dataset]
    for site in siteList:

        # number of accesses: could be zero if not used or present (real sites determined later)
        try:
            nAccess = nAllAccessed[site][dataset]
        except KeyError:
            # print "missing nAccesses for %s %s?"%(dataset,site)
            nAccess = 0

        # remove xrootd accesses, they were already added to the real sites before
        if nAccess<0:
            continue

        # add all accesses for per-dataset summary
        nSumAccess+=nAccess

        # determine time prorating and 'real sites'
        timeOnSite = 0
        try:
            timeOnSite = timeDatasetOnSites[site]
        except KeyError:
            pass

        # filter out cases when the sample was not at the site
        if timeOnSite == 0:
            continue               # this replica didn't exist in relevant interval

        # get relevant parameters (nFiles, sizeGb)
        try:
            nFiles = fileNumbers[dataset]
        except KeyError:
            print "missing nFiles for %s %s?"%(dataset)
            nFiles=0
        try:
            sizeGb = sizesGb[dataset]
        except KeyError:
            print "missing sizeGb for %s %s?"%(dataset)
            nFiles=0

        if nFiles>0 and sizeGb>0:
            # also only print if DAS didn't give nonsense
            if creationTimes[dataset] > start:
                totalAccesses.write("%d %d %f %.3f %s %s %d %d\n"%(nAccess,nFiles,sizeGb,timeOnSite,dataset,site,0,isDeleted[dataset]))
            else:
                totalAccesses.write("%d %d %f %.3f %s %s %d %d\n"%(nAccess,nFiles,sizeGb,timeOnSite,dataset,site,1,isDeleted[dataset]))

    if nFiles > 0 and sizeGb > 0:
        if creationTimes[dataset] > start:
            totalAccessesByDS.write("%d %d %f %d %s %d %d\n"%(nSumAccess,nFiles,sizeGb,nSites[dataset],dataset,0,isDeleted[dataset]))
        else:
            totalAccessesByDS.write("%d %d %f %d %s %d %d\n"%(nSumAccess,nFiles,sizeGb,nSites[dataset],dataset,1,isDeleted[dataset]))

totalAccesses.close()
totalAccessesByDS.close()

sys.exit(0)
