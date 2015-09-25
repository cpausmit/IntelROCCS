#!/usr/bin/python
#-------------------------------------------------------------------------------------------------
#quick script to give basic information about usage of a class of datasets
#originally written to provide PromptReco RECO info to Dima
#---------------------------------------------------------------------------------------------------
import os, sys
if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

import re, glob, subprocess, time, json, MySQLdb
from Dataset import *
import getAccessInfo

genesis=1378008000
# genesis=int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
nowish = time.time()

# get the dataset pattern to consider (careful the pattern will be translated, better implementation
# should be done at some point)
datasetPattern = '/.*/.*PromptReco.*/RECO'

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
        print fileName
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


#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
site = 'T2*'
siterx=re.sub("\*",".*",site) # to deal with stuff like T2* --> T2.*

nSiteAccess  = {}
monitorDB = os.environ['MONITOR_DB']

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
Dataset.siteList = [ x.split("/")[-1] for x in glob.glob(workDirectory + '/'+site)]
if os.environ['UPDATE_CACHE']=="True":
    # should only be set to False for testing purposes
    getAccessInfo.get()  # update access cache
for date in dates:
    files += glob.glob(monitorDB+'/sitesInfo/'+site+'/'+date)
    # files += glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)
phedexFile = open(workDirectory+'/DatasetsInPhedexAtSites.dat','r')
groupPattern = '.*'

# say what we are goign to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Pattern:      %s"%(datasetPattern)
print " Group:        %s"%(groupPattern)
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(siterx)

fileNumbers,sizesGb = readDatasetProperties()

# datasetsOnSites[k]=v, where k is a dataset and v is a set of sites
# this can probably be done smarter and combined with nSites computation
datasetsOnSites={}
datasetSet={}
for line in phedexFile:
    l = line.split()
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
    	datasetObject = Dataset(datasetName)
        datasetSet[datasetName] = datasetObject
    else:
    	datasetObject = datasetSet[datasetName]
    datasetObject.isDeleted = False
    datasetObject.addCurrentSite(siteName,l[6],l[7])
    datasetObject = None

# remove blacklisted datasets
blacklistFile = open(os.environ.get('MONITOR_DB')+'/datasets/blacklist.log','r')
blacklistSet = set(map(lambda x : x.split()[0], list(blacklistFile)))
removeByKey(datasetSet,blacklistSet)

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
    nSiteAccessEntry = addData(nSiteAccessEntry,nAccessed,debug)
    utime = time.mktime(time.strptime(g[-1],"%Y-%m-%d"))
    for datasetName in nAccessed:
        try:
            datasetSet[datasetName].addAccesses(siteName,nAccessed[datasetName],utime)
        except KeyError:
            pass

for d in datasetSet:
    try:
        datasetObject = datasetSet[d]
        datasetObject.nFiles = fileNumbers[d]
        datasetObject.sizeGB = sizesGb[d]
        datasetObject = None
    except KeyError:
        datasetObject = None
        pass
for run in ['2015A-','2015B-','2015C-','2015D-','2013-','2013A','2012-','2012A-','2012C-','2012D-']:
	for k,v in datasetSet.iteritems():
		if k.find(run)<0:
			continue
		s='<tr><td>%s</td>'%(k)
		for startTime in [nowish-30*86400,nowish-90*86400,nowish-180*86400,genesis]:
			s+='<td>%.1f</td>'%(v.getTotalAccesses(startTime,nowish)/float(v.nFiles))
		s+='</tr>'
		print s
sys.exit(0)
