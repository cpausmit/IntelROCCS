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

if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

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
        if not re.search(r'/.*/.*/.*AOD.*',key):
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

def cleanHistories(xfers,dels,end):
    # used to clean up messy histories
    i=0
    j=0
    ld=-1
    if len(dels)==0:
        # if there are no deletions in the history, assume it's still there
        # if there are no transfers in the history, and you are here, SOMETHING IS VERY WRONG
        dels=[end]
    if xfers[0] > dels[0]:
        xfers.insert(0,0)
    while True:
        nx=len(xfers)
        nd=len(dels)
        try:
            if i==nx and j==nd:
                break
            elif xfers[i] < ld:
                xfers.pop(i)
            elif xfers[i] > dels[j]:
                dels.pop(j)
            else:
                i+=1
                j+=1
                ld=dels[j-1]
                if i>=len(xfers):
                    while j<len(dels):
                        dels.pop(j)
                        j+=1
                    break
                elif j>=len(dels):
                    while i<len(xfers):
                        xfers.pop(i)
                        i+=1
                    break
        except IndexError:
            break
        #    print xfers
        #    print dels
        #    print i,j
        #    sys.exit(-1)
    #    print xfers
    #    print dels
    return xfers,dels

def checkHistory(sitePattern,datasetsOnSites):
    # predictedSiteDatasets[k]=v, where k is a dataset and v is a set of sites containing k, as predicted by transfer/delete history
    predictedDatasetsOnSites={}

    # define our variables
    phedDirectory = os.environ['MONITOR_DB']+'/datasets/'
    datasetPattern = '%*%*%*'                                        # consider only xml filename pattern
    #datasetPattern = '%GJet_Pt-15to3000_Tune4C_13TeV_pythia8%Spring14dr-PU20bx25_POSTLS170_V5-v1%AODSIM'
    files = glob.glob(phedDirectory + datasetPattern)
    datasetMovement = {}
    sitePattern=re.sub("\*",".*",sitePattern) # to deal with stuff like T2* --> T2.*
    # read in the data from the phedex history database cache
    #========================================================
    for dataset in files:
        if dataset.find("AOD")==-1:
            continue
        try:                                                      # check that we can parse the xml file
            document = ElementTree.parse(dataset)
        except:
            sys.stderr.write(' WARNING - file reading failed (%s).\n'%dataset)
            continue
        datasetName = dataset.split('/')[-1].replace('%','/')
        for request in document.findall('request'):
            type = request.attrib['type']

            for node in request.findall('node'):
                site = node.attrib['name']
                time = node.attrib['time_decided']
                
                if not re.match(sitePattern,site):                # consider only specified sites
                    #print ' WARNING - no site match.'
                    continue
                if not datasetName in datasetMovement:               # only for datasets from correct sites
                    datasetMovement[datasetName] = {}

                if node.attrib['decision'] == 'approved':         # only consider things that happened

                    try:                                          # check for bad times
                        time = int(float(node.attrib['time_decided']))   # whole number for easy binning
                    except:
                        print ' WARNING - time decided is corrupt.'
                        continue

                    if not site in datasetMovement[datasetName]:     # initialization
                        datasetMovement[datasetName][site] = [[],[]] # allow multiple dataset instances/site

                    if type == 'xfer':                            # (datasets can be requested again)
                        datasetMovement[datasetName][site][0].append(time)     # time transferred
                        #print ' transfer [%s]: %d'%(site,time)
                    elif type == 'delete':
                        datasetMovement[datasetName][site][1].append(time)     # time deleted
                        #print ' delete   [%s]: %d'%(site,time)
                    else:
                        continue

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for filename in datasetMovement:
        historyFile.write("%s\n"%(filename))
        #only look at datasets which we know are on sites
        for siteName in datasetMovement[filename]:
            if not re.match(sitePattern,siteName):                   # only requested sites
                print ' WARNING - no site match. ' + siteName
                continue
            datasetMovement[filename][siteName][0].sort()            # sort lists to match items
            datasetMovement[filename][siteName][1].sort()
            lenXfer = len(datasetMovement[filename][siteName][0])
            lenDel  = len(datasetMovement[filename][siteName][1])

            xfers= datasetMovement[filename][siteName][0]
            dels=  datasetMovement[filename][siteName][1]
            if lenXfer == lenDel:                                # ensure reasonable time lists
                pass
            elif lenXfer == lenDel - 1:
                datasetMovement[filename][siteName][0].insert(0,0)
                lenXfer+=1
            elif lenXfer == lenDel + 1:
                #print 'Add interval end: %d'%end
                datasetMovement[filename][siteName][1].append(nowish)
            elif lenXfer==0:
                # this actually doesn't make sense
                continue
            elif lenDel==0:
                datasetMovement[filename][siteName][1].append(nowish)
            else:
                # doesn't make sense, so skip
                # continue
                # or don't skip
                datasetMovement[filename][siteName][0],datasetMovement[filename][siteName][1] = cleanHistories(xfers,dels,nowish)
            lastXfer=xfers[-1]
            lastDel=dels[-1]
#            historyFile.write("%25s\t%s\n"%(siteName,str(xfers)))
#            historyFile.write("%25s\t%s\n"%(" ",str(dels)))
#            historyFile.write("%25s\t%i -> %i ; %i\n"%(" ",lastXfer,lastDel,nowish))
            if  lastXfer > lastDel or lastDel > nowish-5*60: 
                # last transfer was more recent than last deletion
                # or last deletion was at no more than 5 minutes ago
                if filename in predictedDatasetsOnSites:
                    predictedDatasetsOnSites[filename].add(siteName)
                else:
                    predictedDatasetsOnSites[filename]=set([siteName])
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
# say what we are going to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
print " Date pattern: %s"%(date)
fileName = 'HistorySummary.txt'
print ' Output file: ' + fileName
historyFile = open(fileName,'w')

# datasetsOnSites[k]=v, where k is a dataset and v is a set of sites
datasetsOnSites={}
for line in phedexFile:
    l = line.split()
    datasetName=l[0]
    siteName=l[7]
    if not re.match(site,siteName):
        continue
    historyFile.write("Matched %s with %s\n"%(site,siteName))
    if datasetName.find("AOD")==-1:
        continue
    if datasetName in datasetsOnSites:
        datasetsOnSites[datasetName].add(siteName)
    else:
        datasetsOnSites[datasetName] = set([siteName])

# figure out if a dataset is on a site or not based on history 
predictedDatasetsOnSites = checkHistory(site,datasetsOnSites)

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
        n = len(predictedSites) - len(phedexSites)
    except KeyError:
        predictedSites = None
        n = len(phedexSites)
    fillDict(histErrors,n,1)
    if not predictedSites == phedexSites:
        nErrors+=1
        historyFile.write("DATASET: %s\n"%(datasetName));
        historyFile.write("\tACTUAL    :\t%s\n"%(set2str(phedexSites)))
        historyFile.write("\tPREDICTED :\t%s\n"%(set2str(predictedSites)))
        historyFile.write("\tDIFFERENCE:\t%i\n"%(n))
historyFile.write("\nSUMMARY: %i/%i Errors\n"%(nErrors,nTot))
for k in sorted(histErrors):
    v=histErrors[k]
    historyFile.write("%2i:\t%5i\n"%(k,v))
historyFile.close()
sys.exit(0)
