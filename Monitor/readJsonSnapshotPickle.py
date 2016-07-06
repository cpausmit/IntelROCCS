#!/usr/bin/python
#-------------------------------------------------------------------------------------------------
#
# This script reads the information from a set of JSON snapshot files that we use to cache our
# popularity information.
# And puts all of that information in a salt solution. For best results, store in a cold, dry
# place and enjoy after 2-4 weeks.
#
#---------------------------------------------------------------------------------------------------
import os, sys
if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

import re, glob, subprocess, time, json, pprint, MySQLdb
import datasetProperties
from findDatasetHistoryAll import *
import findDatasetProperties as fDP
import cPickle as pickle
from Dataset import *
import getAccessInfo
from StringIO import StringIO

genesis=1378008000
# genesis=int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
nowish = time.time()
sPerYear = 365*24*60*60

# get the dataset pattern to consider (careful the pattern will be translated, better implementation
# should be done at some point)
datasetPattern = os.environ.get('MONITOR_PATTERN')
datasetPattern = datasetPattern.replace("_",".*")
# datasetPattern=r'/.*/.*/.*AOD.*'
# datasetPattern=r'(?!/.*/.*/USER.*)' # exclude USER since das_client doesn't return useful info

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
    try:
        with open(fileName) as data_file:
            data = json.load(data_file)
    except ValueError:
        # if the file doesn't exist, then there were no accesses that day
    #    print fileName
        return (0,{})

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
            print ' WARNING - suspicious entry - the entry exists already (continue)',key
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
    # query dbs
    keypath = os.environ['USERKEY']
    certpath = os.environ['USERCERT']
    cmd = 'curl -k -H "Accept: application/json" --cert %s --key %s "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/blocks?dataset=%s&detail=true"'%(certpath,keypath,dataset)
    print cmd
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
      print line
      payload = json.load(StringIO(line))
    creation = 0
    for block in payload:
      creation = max(creation,block['creation_date'])
    if creation==0:
      return genesis
    else:
      with open(fileName,'a') as dataFile:
          dataFile.write("%s %i\n"%(dataset,creation))
      cTimes[dataset] = creation
      return creation
    #return genesis
    '''
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
    '''

def readDatasetCreationTimes(fileName,debug=0):
    # read the creation time for each dataset from a given file

    creationTimes={}
    if not os.path.exists(fileName):
        return creationTimes
    dataFile = open(fileName,'r')
    for line in dataFile.readlines():
        line=re.split('[ \t\n]',line)
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
    print 'server="%s", db="%s", user="%s", passwd="%s"'%(server,db,user,pw)
    #server='localhost'
    db = MySQLdb.connect(read_default_file = '/etc/my.cnf', read_default_group = 'mysql-ddm', db = 'IntelROCCS')
    #db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    return db.cursor()

def readDatasetProperties():
    # read dataset properties fromt the database

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

def calculateDatasetMovement(sitePattern,datasetSet,cTimes={}):
    # calculate the movement of datasets on sites

    predictedDatasetsOnSites={}
    for datasetName in datasetSet:
        predictedDatasetsOnSites[datasetName]=set([])
    parseRequestJson(genesis,nowish,False,datasetPattern,datasetSet,sitePattern)
    parseRequestJson(genesis,nowish,True,datasetPattern,datasetSet,sitePattern)

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    nDatasets = len(datasetSet)
    datasetCounter=0
    for datasetName,datasetObject in datasetSet.iteritems():
        # don't penalize a dataset for not existing
        if datasetCounter%10000==0:
          print datasetCounter,'/',nDatasets
        datasetCounter += 1
        cTime = findDatasetCreationTime(datasetName,creationTimeCache,cTimes)
        datasetObject.cTime = cTime
        datasetMovement = datasetObject.movement
        for siteName in datasetMovement:
            start = max(genesis,cTime)
            if not re.match(sitePattern,siteName) or re.search(r'$X',siteName):                   # only requested sites
                # print ' WARNING - no site match. ' + siteName
                continue

            datasetMovement[siteName][0].sort()            # sort lists to match items
            datasetMovement[siteName][1].sort()
            datasetMovement[siteName] = cleanHistories(datasetMovement[siteName][0],datasetMovement[siteName][1],start,nowish)
            xfers = datasetMovement[siteName][0]
            dels = datasetMovement[siteName][1]
            lenXfer = len(xfers)
            lenDel  = len(dels)

            siteSum = 0
            i       = 0
            if (lenXfer > 0 and lenDel > 0 and xfers[-1] > dels[-1]) or (lenDel==0 and lenXfer > 0 and xfers[-1] < nowish):
                if datasetName in predictedDatasetsOnSites:
                    predictedDatasetsOnSites[datasetName].add(siteName)
                else:
                    predictedDatasetsOnSites[datasetName] = set([siteName])
        try:
            if datasetName in predictedDatasetsOnSites:
                diff = datasetObject.currentSites - predictedDatasetsOnSites[datasetName]
            else:
                diff = datasetObject.currentSites
        except KeyError:
            diff=set([])
        for siteName in diff:
            if siteName not in datasetMovement:
                newStart = max(cTime,genesis)
                if nowish-newStart > 0:
                    datasetMovement[siteName] = ([newStart],[])
                else:
                    datasetMovement[siteName] = ([],[])
            elif len(datasetMovement[siteName][1])==0:
                # it was never deleted...
                if len(datasetMovement[siteName][0])==0:
                    newStart = max(cTime,genesis)
                    if nowish-newStart > 0:
                        datasetMovement[siteName] = ([newStart],[])
        datasetObject.movement = datasetMovement
        datasetMovement = None


#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
sizeAnalysis = True
addNotAccessedDatasets = True
pickleDict = {}
usage  = "\n"
usage += " readJsonSnapshotPickle.py  <sitePattern> \n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

siterx=re.sub("\*",".*",site) # to deal with stuff like T2* --> T2.*
# figure out which datases to consider using phedex cache as input
if addNotAccessedDatasets:
    file = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
           os.environ['DETOX_PHEDEX_CACHE']
    sizesPerSite = processPhedexCacheFile(file,debug=0)

# initialize our variables
nAllSkipped  = 0
nSiteAccess  = {}
sizesGb      = {}
fileNumbers  = {}
if sizeAnalysis:
    (fileNumbers,sizesGb) = readDatasetProperties()

# form date regexps
starttmp = time.gmtime(genesis)
endtmp = time.gmtime(nowish)
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
files=[]
siteList = [ x.split("/")[-1] for x in glob.glob(workDirectory + '/'+site)]
monitorDB = os.environ['MONITOR_DB']
Dataset.siteList = siteList
if os.environ['UPDATE_CACHE']=="True":
    # should only be set to False for testing purposes
    getAccessInfo.get()  # update access cache
for date in dates:
    files += glob.glob(monitorDB+'/sitesInfo/'+site+'/'+date)
    # files += glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)
phedexFile = open(workDirectory+'/DatasetsInPhedexAtSites.dat','r')
creationTimeCache = os.environ['MONITOR_DB'] + '/datasets/creationTimes.txt'
groupPattern = os.environ['MONITOR_GROUP']
groupPattern = groupPattern.replace("_",".*")
# say what we are goign to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Pattern:      %s"%(datasetPattern)
print " Group:        %s"%(groupPattern)
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
if sizeAnalysis:
    print "\n Size Analysis is on. Please be patient, this might take a while!\n"


# datasetsOnSites[k]=v, where k is a dataset and v is a set of sites
# this can probably be done smarter and combined with nSites computation
datasetsOnSites={}
datasetSet={}
isDeleted={}
phedexSize=0
for line in phedexFile:
    l = line.split()
    if not re.match(groupPattern,l[1]):
        continue
    datasetName=l[0]
    siteName=l[5]
    if not re.match(datasetPattern,datasetName):
        continue
    if re.match(".*BUNNIES.*",datasetName):  
        # get rid of T0 testing datasets
        # why are these even in DDM?
        continue
    if not re.match(siterx,siteName):
        continue
    if datasetName not in datasetSet:
        datasetSet[datasetName] = Dataset(datasetName)
    datasetObject = datasetSet[datasetName]
    datasetObject.isDeleted = False
    datasetObject.addCurrentSite(siteName,l[6],l[7])
    phedexSize+=float(l[2])
    datasetObject = None
print "Phedex Size: ",phedexSize
getJsonFile("del",genesis) # all deletions
# remove blacklisted datasets
print 'removing blacklist...'
blacklistFile = open(os.environ.get('MONITOR_DB')+'/datasets/blacklist.log','r')
blacklistSet = set(map(lambda x : x.split()[0], list(blacklistFile)))
removeByKey(datasetSet,blacklistSet)

print 'analyzing accesses...'
for fileName in sorted(files):
    if debug>0:
        print ' Analyzing: ' + fileName
    g = fileName.split("/")
    siteName = g[-2]

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
    utime = time.mktime(time.strptime(g[-1],"%Y-%m-%d"))
    for datasetName in nAccessed:
        try:
            datasetSet[datasetName].addAccesses(siteName,nAccessed[datasetName],utime)
        except KeyError:
            pass

print 'analyzing unaccessed datasets...'
# --------------------------------------------------------------------------------------------------
# add all datasets that are in phedex but have never been used
# --------------------------------------------------------------------------------------------------
if addNotAccessedDatasets:
    # these are number of non accessed samples with corresponding total size
    nAddedSamples = 0
    addedSize = 0.
    # these are number of samples we exclude
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
            if dataset not in datasetSet:
                # update our local counters
                nExcludedSamples += 1
                excludedSize += sizesPerSitePerDataset[dataset]
                continue

            # only add information if the samples is not already available
            if site in datasetSet[dataset].nAccesses:
                if debug>1:
                    print ' -> Not Adding : ' + dataset
            else:
                if debug>0:
                    print ' -> Adding : ' + dataset

                # update our local counters
                nAddedSamples += 1
                addedSize += sizesPerSitePerDataset[dataset]

                # make an entry in all of the relevant records
                # try:
                #     datasetSet[dataset].addAccesses(site,0)
                # except KeyError:
                #     pass
                nSiteAccessEntry[dataset] = 0

    print " "
    print " - number of excluded datasets: %d"%(nExcludedSamples)
    print " - excluded sample size:        %d"%(excludedSize)
    print " "
    print " - number of added datasets:    %d"%(nAddedSamples)
    print " - added sample size:           %d"%(addedSize)


pickleDict["nSiteAccess"] = nSiteAccess

# --------------------------------------------------------------------------------------------------
# create summary information and potentially print the contents
# --------------------------------------------------------------------------------------------------
print 'checking dataset history...'
nAll = 0
nAllAccess = 0
sizeTotalGb = 0.
nFilesTotal = 0
findDatasetHistoryAll(genesis)

print 'checking dataset properties...'
counter=0
for key in datasetSet:
    counter+=1
    if sizeAnalysis:
        if not key in fileNumbers:
            print counter,'/',len(datasetSet)
            # update the dataset properties
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

for d in fileNumbers:
    try:
        datasetObject = datasetSet[d]
        datasetObject.nFiles = fileNumbers[d]
        datasetObject.sizeGB = sizesGb[d]
        datasetObject = None
    except KeyError:
        datasetObject = None
        pass


# load creation time cache
print 'checking creation times...'
creationTimes=readDatasetCreationTimes(creationTimeCache)
# figure out properly the movement of the datasets
print 'calculating movement...'
calculateDatasetMovement(siterx,datasetSet,creationTimes)

for k,v in datasetSet.iteritems():
    # make sure Xrootd accesses are properly redistributed
    v.fixXrootd()

pickleDict["datasetSet"] = datasetSet
if groupPattern == '.*':
    groupPattern = "All"
pickleJar = open(monitorDB+'/monitorCache'+ groupPattern+'.pkl',"wb")
pickle.dump(pickleDict,pickleJar,2) # put the pickle in a jar
pickleJar.close() # close the jar
sys.exit(0)
