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
from math import log
from findDatasetHistoryAll import *

if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

datasetPattern=r'/.*/.*/.*AOD.*'
# datasetPattern='/wprime_oppsign_mu_M2200_g1_ptl1000to1d5_8TeV/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM'
#===================================================================================================
#  H E L P E R S
#===================================================================================================

def processFile(fileName,debug=0):
    # processing the contents of a simple file into a set
    datasetsOnSite = set([])

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
        if not re.search(datasetPattern,key):
            if debug>0:
                print ' WARNING -- rejecting non *AOD* type data: ' + key
            continue

        # here is where we assign the values to the hashed set (checking if duplicates are registered)
        if key in datasetsOnSite:
            print ' WARNING - suspicious entry - the entry exists already (continue)'
        else:
            datasetsOnSite.add(key)

    # return the datasets
    return datasetsOnSite

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

def checkHistory(sitePattern,datasetsOnSites,start,end,cleaned=True):
    # predictedSiteDatasets[k]=v, where k is a dataset and v is a set of sites containing k, as predicted by transfer/delete history
    predictedDatasetsOnSites={}

    datasetMovement = {}
    for datasetName in datasetsOnSites:
        datasetMovement[datasetName]={}
    sitePattern=re.sub("\*",".*",sitePattern) # to deal with stuff like T2* --> T2.*
    # read in the data from the phedex history database cache
    fileName = os.environ.get('MONITOR_DB') + '/datasets/' + 'delRequests_%i'%(int(start))+'.json'
    parseRequestJson(fileName,start,end,False,datasetMovement,datasetPattern)
    fileName = os.environ.get('MONITOR_DB') + '/datasets/' + 'xferRequests_%i'%(int(start))+'.json'
    parseRequestJson(fileName,start,end,True,datasetMovement,datasetPattern)
    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for datasetName in datasetMovement:
        # historyFile.write("%s\n"%(datasetName))
        #only look at datasets which we know are on sites
        for siteName in datasetMovement[datasetName]:
            if not re.match(sitePattern,siteName):                   # only requested sites
                print ' WARNING - no site match. ' + siteName
                continue
            datasetMovement[datasetName][siteName][0].sort()            # sort lists to match items
            datasetMovement[datasetName][siteName][1].sort()
            lenXfer = len(datasetMovement[datasetName][siteName][0])
            lenDel  = len(datasetMovement[datasetName][siteName][1])

            xfers= datasetMovement[datasetName][siteName][0]
            dels=  datasetMovement[datasetName][siteName][1]
            if not cleaned:
                if len(dels)==0:
                    if len(xfers)==0:
                        continue
                    elif xfers[-1] < nowish:
                        if datasetName in predictedDatasetsOnSites:
                            predictedDatasetsOnSites[datasetName].add(siteName)
                        else:
                            predictedDatasetsOnSites[datasetName]=set([siteName])
                    else:
                        continue
                else:
                    if len(xfers)==0:
                        continue
                    elif nowish > xfers[-1] > dels[-1]:
                        if datasetName in predictedDatasetsOnSites:
                            predictedDatasetsOnSites[datasetName].add(siteName)
                        else:
                            predictedDatasetsOnSites[datasetName]=set([siteName])
                continue

            xfers,dels = cleanHistories(xfers,dels,start,end)
            datasetMovement[datasetName][siteName][0] = xfers
            datasetMovement[datasetName][siteName][1] = dels
            # sys.exit(-1)
            if len(dels)==0:
                # there is no deletion history
                if len(xfers)==0 and siteName in datasetsOnSites[datasetName]:
                    # if there are no transfers but we know the dataset 
                    # is there right now, assume it's always been there
                    if datasetName in predictedDatasetsOnSites:
                        predictedDatasetsOnSites[datasetName].add(siteName)
                    else:
                        predictedDatasetsOnSites[datasetName]=set([siteName])
                elif xfers[-1] < nowish:
                    # or if there was a recent transfer
                    if datasetName in predictedDatasetsOnSites:
                        predictedDatasetsOnSites[datasetName].add(siteName)
                    else:
                        predictedDatasetsOnSites[datasetName]=set([siteName])
            else:
                lastXfer=xfers[-1]
                lastDel=dels[-1]
                if  lastXfer > lastDel or lastDel > nowish-5*60: 
                    # last transfer was more recent than last deletion
                    # or last deletion was at no more than 5 minutes ago
                    if datasetName in predictedDatasetsOnSites:
                        predictedDatasetsOnSites[datasetName].add(siteName)
                    else:
                        predictedDatasetsOnSites[datasetName]=set([siteName])
        if not cleaned:
            continue
        try:
            diff = datasetsOnSites[datasetName] - predictedDatasetsOnSites[datasetName]
        except KeyError:
            if datasetName in datasetsOnSites:
                diff = datasetsOnSites[datasetName]
            else:
                diff=set([])
        for siteName in diff:
            if siteName not in datasetMovement[datasetName]:
                # no phedex history, but the dataset exists (it was created there?)
                if datasetName in predictedDatasetsOnSites:
                    predictedDatasetsOnSites[datasetName].add(siteName)
                else:
                    predictedDatasetsOnSites[datasetName]=set([siteName])
            elif len(datasetMovement[datasetName][siteName][1])==0:
                # it was never deleted...
                if len(datasetMovement[datasetName][siteName][0])==0:
                    # it was never transferred...
                    if datasetName in predictedDatasetsOnSites:
                        predictedDatasetsOnSites[datasetName].add(siteName)
                    else:
                        predictedDatasetsOnSites[datasetName]=set([siteName])
                elif datasetMovement[datasetName][siteName][0][-1] < nowish:
                    # it was transferred recently?
                    if datasetName in predictedDatasetsOnSites:
                        predictedDatasetsOnSites[datasetName].add(siteName)
                    else:
                        predictedDatasetsOnSites[datasetName]=set([siteName])

    return predictedDatasetsOnSites

def fillDict(d,k,v):
    # stupid but useful
    if k in d:
        d[k]+=v
    else:
        d[k]=v

def set2str(s):
    # formatting
    if s is None:
        return "set(  )"
    else:
        r="set( "
        for x in s:
            r+="%20s"%(x)
        r+=" )"
    return r

#===================================================================================================
#  MAIN
#===================================================================================================
debug = 0

usage  = "\n"
usage += " checkDatasetHistory.py  <sitePattern> [ <datePattern>=????-??-?? ]\n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   datePattern  - pattern to select date pattern (ex. 201[34]-0[0-7]-??)\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    sys.stderr.write('ERROR - not enough arguments\n' + usage)
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    date = '????-??-??'
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    date = str(sys.argv[2])
else:
    sys.stderr.write('ERROR - too many arguments\n' + usage)
    sys.exit(2)

nowish         = int(round(time.time()/300)*300)               # now to the nearest five min
# --------------------------------------------------------------------------------------------------
# loop through all matching snapshot files
# --------------------------------------------------------------------------------------------------
# figure out which snapshot files to consider
workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
phedexFile = open(workDirectory+'/DatasetsInPhedexAtSites.dat','r')
files = glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)
# say what we are going to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
print " Date pattern: %s"%(date)
fileName = 'HistorySummary_corrected.txt'
print ' Output file: ' + fileName
historyFile = open(fileName,'w')
firstFile = ''
lastFile = ''
for fileName in sorted(files):
    if debug>0:
        print ' Analyzing: ' + fileName
    if firstFile == '':
        firstFile = fileName
        break
if lastFile == '':
    lastFile = fileName
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

findDatasetHistoryAll(start)

# datasetsOnSites[k]=v, where k is a dataset and v is a set of sites
datasetsOnSites={}
site=re.sub("\*",".*",site) # to deal with stuff like T2* --> T2.*
for line in phedexFile:
    l = line.split()
    datasetName=l[0]
    siteName=l[7]
    if not re.match(site,siteName):
        continue
    # historyFile.write("Matched %s with %s\n"%(site,siteName))
    # if datasetName.find("AOD")==-1:
    if not re.search(datasetPattern,datasetName):
        continue
    if datasetName in datasetsOnSites:
        datasetsOnSites[datasetName].add(siteName)
    else:
        datasetsOnSites[datasetName] = set([siteName])

# figure out if a dataset is on a site or not based on history 
# but do it really, really poorly
predictedDatasetsOnSites = checkHistory(site,datasetsOnSites,start,nowish,True)

# last step: produce monitoring information
# ========================================
# ready to use: predictedDatasetsOnSites[sites] datasetsOnSites[sites]
histErrors={}
nTot=0
nErrors=0
for datasetName in datasetsOnSites:
    nTot+=1
    phedexSites = datasetsOnSites[datasetName] 
    try:
        predictedSites = predictedDatasetsOnSites[datasetName]
        n = len(predictedSites ^ phedexSites)
    except KeyError:
        predictedSites = None
        n = len(phedexSites)
    fillDict(histErrors,n,1)
    if not predictedSites == phedexSites:
        nErrors+=1
        historyFile.write("DATASET: %s\n"%(datasetName))
        historyFile.write("\tACTUAL    :\t%s\n"%(set2str(phedexSites)))
        historyFile.write("\tPREDICTED :\t%s\n"%(set2str(predictedSites)))
        historyFile.write("\tDIFFERENCE:\t%i\n"%(n))
historyFile.write("\nSUMMARY: %i/%i Errors\n"%(nErrors,nTot))
for k in sorted(histErrors):
    v=histErrors[k]
    historyFile.write("%2i:\t%5i\n"%(k,v))
historyFile.close()
sys.exit(0)
