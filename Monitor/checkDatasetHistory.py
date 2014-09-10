#!/usr/bin/python
#-------------------------------------------------------------------------------------------------
#
# This script reads the information from a set of JSON snapshot files that we use to cache our
# popularity information.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, glob, subprocess, time, json, pprint, MySQLdb
import datasetProperties
from   xml.etree import ElementTree

if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def processPhedexCacheFile(fileName,debug=0):
    # processing the contents of a simple file into a hash array

    sizesPerSite = {}
    #print "fileName: %s"%(fileName)
    iFile = open(fileName,'r')
    # loop through all lines
    for line in iFile.xreadlines():
        line = line[:-1]
        f = line.split()
        if len(f) == 8:
            dataset = f[0]
            group   = f[1]
            date    = int(f[2])
            size    = float(f[3])
            nFiles  = int(f[4])
            valid   = int(f[5])
            custd   = int(f[6])
            site    = f[7]

        else:
            print 'Columns not equal 8: \n %s'%(line)
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
        if not re.search(r'/.*/.*/.*AOD.*',key):
            if debug>0:
                print ' WARNING -- rejecting non *AOD* type data: ' + key
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
        sizeGb = sizeGb/1024.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1024.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(0)

    # return the size in GB as a float
    return sizeGb

def findDatasetSize(dataset,debug=0):
    # find the file size of a given data set (dataset) and return the number of files in the dataset
    # and its size in GB

    cmd = os.environ.get('MONITOR_BASE') + '/findDatasetProperties.py ' + dataset + ' short | tail -1'
    if debug>-1:
        print ' CMD: ' + cmd
    nFiles = 0
    sizeGb = 0.
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        line = line[:-1]
        g = line.split()
        try:
            nFiles = int(g[0])
            sizeGb = float(g[1])
        except:
            # when the decoding fails either there was an extra line or no information found
            # which means the counts are zero
            print ' WARNING - decoding failed: %d %f.3 -- %s'%(nFiles,sizeGb,dataset)
            pass
    return nFiles, sizeGb

def getDbCursor():
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
    sizesGb     = {}
    fileNumbers = {}

    #print " START reading database"

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
            fileNumbers[dataset] = int(nFiles)
            sizesGb[dataset]     = float(sizeGb)
    except:
        pass

    #print " DONE reading database"

    return (fileNumbers, sizesGb)


def calculateAverageNumberOfSites(sitePattern,start,end):

    print ' Relevant time interval: %d %d  --> %d'%(start,end,end-start)

    # convert it into floats and take care of possible rounding issues
    interval = end - start

    # define our variables
    #phedDirectory = '/local/cmsprod/IntelROCCS/Monitor/datasets/' # FIX: use env from setupMonitor.sh
    phedDirectory = os.environ['MONITOR_DB']+'/datasets/'
    filePattern = '%*%*%*'                                        # consider only xml filename pattern
    #filePattern = '%GJet_Pt-15to3000_Tune4C_13TeV_pythia8%Spring14dr-PU20bx25_POSTLS170_V5-v1%AODSIM'
    files = glob.glob(phedDirectory + filePattern)
    datasetMovement = {}
    nSites = {}

    # read in the data from the phedex history database cache
    #========================================================
    for file in files:
        try:                                                      # check that we can parse the xml file
            document = ElementTree.parse(file)
        except:
            print ' WARNING - file reading failed (%s).'%file
            continue
        filename = file.split('/')[-1].replace('%','/')
        for request in document.findall('request'):
            type = request.attrib['type']

            for node in request.findall('node'):
                site = node.attrib['name']
                time = node.attrib['time_decided']

                #print ' Checking %s versus %s'%(sitePattern,site)

                if not re.match(sitePattern,site):                # consider only specified sites
                    #print ' WARNING - no site match.'
                    continue

                if not filename in datasetMovement:               # only for datasets from correct sites
                    datasetMovement[filename] = {}

                if node.attrib['decision'] == 'approved':         # only consider things that happened

                    try:                                          # check for bad times
                        time = int(float(node.attrib['time_decided']))   # whole number for easy binning
                    except:
                        print ' WARNING - time decided is corrupt.'
                        continue

                    if not site in datasetMovement[filename]:     # initialization
                        datasetMovement[filename][site] = [[],[]] # allow multiple dataset instances/site

                    if type == 'xfer':                            # (datasets can be requested again)
                        datasetMovement[filename][site][0].append(time)     # time transferred
                        #print ' transfer [%s]: %d'%(site,time)
                    elif type == 'delete':
                        datasetMovement[filename][site][1].append(time)     # time deleted
                        #print ' delete   [%s]: %d'%(site,time)
                    else:
                        continue

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for filename in datasetMovement:
        # ensure datasetMovement entry is complete
        for site in datasetMovement[filename]:
            #print ' Adding site: ' + site
            if not re.match(sitePattern,site):                   # only requested sites
                print ' WARNING - no site match. ' + site
                continue

            datasetMovement[filename][site][0].sort()            # sort lists to match items
            datasetMovement[filename][site][1].sort()

            lenXfer = len(datasetMovement[filename][site][0])
            lenDel  = len(datasetMovement[filename][site][1])

            if lenXfer == lenDel:                                # ensure reasonable time lists
                pass
            elif lenXfer == lenDel - 1:
                datasetMovement[filename][site][0].insert(0,0)
            elif lenXfer == lenDel + 1:
                #print 'Add interval end: %d'%end
                datasetMovement[filename][site][1].append(end)
            else:
                # doesn't make sense, so skip
                continue

            # find this site's fraction for nSites
            if not filename in nSites:
                nSites[filename] = 0

            siteSum = 0
            i       = 0

            # loop through the transfer/deletion intervals
            while i < lenXfer:
                tXfer = datasetMovement[filename][site][0][i]
                tDel  = datasetMovement[filename][site][1][i]

                i = i + 1                                        # iterate before all continue statements

                if tXfer == 0 and tDel == end:                   # skip if defaults, meaning no xfer or del time found
                    continue                                     # shouldn't happen, but just to be sure

                # four ways to be in interval                    # (though this prevents you from having the same
                                                                 #  start and end date)
                if tXfer <= start <= end <= tDel:
                    siteSum += 1                                 # should happen at most once, but okay
                elif tXfer <= start < tDel < end:
                    siteSum += float(tDel - start)/float(interval)
                    #print ' Adding: %d %f'%(i,float(tDel - start)/float(interval))
                elif start < tXfer < end <= tDel:
                    siteSum += float(end - tXfer)/float(interval)
                    #print ' Adding: %d %f'%(i,float(end - tXfer)/float(interval))
                elif start < tXfer < tDel <= end:
                    siteSum += float(tDel - tXfer)/float(interval)
                    #print ' Adding: %d %f'%(i,float(tDel - tXfer)/float(interval))
                else:                                            # have ensured tXfer < tDel
                    continue

            if siteSum < 0:                                      # how can this happen??
                print " WARNING - siteNum < 0 ??"
                continue

            nSites[filename] += siteSum

    n     = 0
    nSkip =  0
    sum   = float(0)
    for filename in nSites:
        if nSites[filename] == 0:                                # dataset not on sites in interval
            nSkip += 1
            continue
        sum += nSites[filename]
        n   += 1
        #print ' %s \t%f'%(filename,nSites[filename])

    print '\n Dataset  --  average number of sites\n'

    if n != 0:
        avg = sum/n
        print '\n overall average n sites: %f\n'%(avg)
        print '\n number of datasets:      %d\n'%(n)
        print '\n number of datasets skip: %d\n'%(nSkip)
    else:
        print 'no datasets found in specified time interval'

    return nSites

#===================================================================================================
#  MAIN
#===================================================================================================
debug = 0
sizeAnalysis = True
addNotAccessedDatasets = True

usage  = "\n"
usage += " readJsonSnapshot.py  <sitePattern> [ <datePattern>=????-??-?? ]\n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   datePattern  - pattern to select date pattern (ex. 201[34]-0[0-7]-??)\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    date = '????-??-??'
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    date = str(sys.argv[2])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

# figure out which datases to consider using phedex cache as input
if addNotAccessedDatasets:
    file = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
           os.environ['DETOX_PHEDEX_CACHE']
    sizesPerSite = processPhedexCacheFile(file,debug=0)

# initialize our variables
nAllSkipped  = 0
nAllAccessed = {}
nSiteAccess  = {}
sizesGb      = {}
fileNumbers  = {}
if sizeAnalysis:
    (fileNumbers,sizesGb) = readDatasetProperties()

# --------------------------------------------------------------------------------------------------
# loop through all matching snapshot files
# --------------------------------------------------------------------------------------------------
# figure out which snapshot files to consider
workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
files = glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)
# say what we are goign to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
print " Date pattern: %s"%(date)
if sizeAnalysis:
    print "\n Size Analysis is on. Please be patient, this might take a while!\n"

firstFile = ''
lastFile = ''

for fileName in sorted(files):
    if debug>0:
        print ' Analyzing: ' + fileName

    if firstFile == '':
        firstFile = fileName

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
    nAllAccessed     = addData(nAllAccessed,    nAccessed,debug)

if lastFile == '':
    lastFile = fileName

#print " First: %s  Last: %s"%(firstFile,lastFile)

# find start and end times

dir = '/'.join(lastFile.split("/")[0:-1])
startDate = firstFile.split("/")[-1]

files = glob.glob(dir + '/????-??-??')
last = ''
endDate = ''
for fileName in sorted(files):
    #print ' Filename: ' + fileName
    #print " Compare: " + last + ' ' + lastFile.split("/")[-1]
    if last == lastFile.split("/")[-1]:
        endDate = fileName.split("/")[-1]
    last = fileName.split("/")[-1]

# convert to epoch time
start = int(time.mktime(time.strptime(startDate,'%Y-%m-%d')))
if endDate != '':
    end = int(time.mktime(time.strptime(endDate,'%Y-%m-%d')))
else:
    endDate = lastFile.split("/")[-1]
    end = int(time.mktime(time.strptime(endDate,'%Y-%m-%d'))) + 7*24*3600  # one week

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
            if not re.search(r'/.*/.*/.*AOD.*',dataset):
                # update our local counters
                nExcludedSamples += 1
                excludedSize += sizesPerSitePerDataset[dataset]
                continue

            # only add information if the samples is not already available
            if dataset in nAllAccessed:
                if debug>1:
                    print ' -> Not Adding : ' + dataset
            else:
                if debug>0:
                    print ' -> Adding : ' + dataset

                # update our local counters
                nAddedSamples += 1
                addedSize += sizesPerSitePerDataset[dataset]

                # make an entry in all of the relevant records
                nAllAccessed[dataset] = 0
                nSiteAccessEntry[dataset] = 0

    print " "
    print " - number of excluded datasets: %d"%(nExcludedSamples)
    print " - excluded sample size:        %d"%(excludedSize)
    print " "
    print " - number of added datasets:    %d"%(nAddedSamples)
    print " - added sample size:           %d"%(addedSize)

# --------------------------------------------------------------------------------------------------
# create summary information and potentially print the contents
# --------------------------------------------------------------------------------------------------
nAll = 0
nAllAccess = 0
sizeTotalGb = 0.
nFilesTotal = 0
for key in nAllAccessed:
    value = nAllAccessed[key]
    nAllAccess += value
    nAll += 1
    if sizeAnalysis:
        if not key in fileNumbers:
            # update the dataset history
            cmd = os.environ.get('MONITOR_BASE') + '/findDatasetHistory.py ' + key + ' 2> /dev/null'
            print ' CMD: ' + cmd
            os.system(cmd)
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

# printout the summary
print " "
print " = = = = S U M M A R Y = = = = "
print " "
print " - number of skipped datasets: %d"%(nAllSkipped)
print " "
print " - number of datasets:         %d"%(nAll)
print " - number of accesses:         %d"%(nAllAccess)
if sizeAnalysis:
    print " - data volume [TB]:           %.3f"%(sizeTotalGb/1024.)

# careful watch cases where no datasets were found
if nAll>0:
    print " "
    print " - number of accesses/dataset: %.2f"%(float(nAllAccess)/float(nAll))
if sizeAnalysis:
    if sizeTotalGb>0:
        print " - number of accesses/GB:      %.2f"%(float(nAllAccess)/sizeTotalGb)
    if nFilesTotal>0:
        print " - number of accesses/file:    %.2f"%(float(nAllAccess)/nFilesTotal)
print " "

# need to figure out how many replicas are out there
nSites = {}
for site in sorted(nSiteAccess):
    nSiteAccessEntry = nSiteAccess[site]
    ## print " - number of datasets:         %-8d at %s"%(len(nSiteAccessEntry),site)
    # count the number of sites carrying a given dataset
    nSites = addSites(nSites,nSiteAccessEntry,debug)

# figure out properly the 'average number of sites' in the given time interval
nAverageSites = calculateAverageNumberOfSites('T2',start,end)

# last step: produce monitoring information
# ========================================
# ready to use: nAllAccessed[dataset], fileNumbers[dataset], sizesGb[dataset], nSites[dataset]

fileName = 'DatasetSummary.txt'
print ' Output file: ' + fileName
totalAccesses = open(fileName,'w')
for dataset in nAllAccessed:
    nSite = nSites[dataset]
    if dataset in nAverageSites:
        nAverageSite = float(nAverageSites[dataset])
    else:
        #print ' Fix dataset: ' + dataset
        cmd = os.environ.get('MONITOR_BASE') + '/findDatasetHistory.py ' + dataset
        os.system(cmd)
        nAverageSite = 0
    nAccess = nAllAccessed[dataset]
    nFiles = fileNumbers[dataset]
    sizeGb = sizesGb[dataset]
    #if nAccess == 0:
    #    print " NOT USED - %d %d %d %f"%(nSite,nAccess,nFiles,sizeGb)
    totalAccesses.write("%d %f %d %d %f %s\n"%(nSite,nAverageSite,nAccess,nFiles,sizeGb,dataset))
    if nSite>42:
        print " WARNING - nSites suspicious: %3d %s"%(nSite,dataset)
    if nAverageSite>42:
        print " WARNING - nAverageSites suspicious: %3d %s"%(nAverageSite,dataset)
totalAccesses.close()

sys.exit(0)
