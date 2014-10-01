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

def processFile(fileName,fileNumbers,debug=0):
    # processing the contents of a simple file into a hash array

    nCSA=0
    nDatasetsCSA=0
    nOthers=0
    nDatasetsOthers=0

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
        rx1=r'/.*/Spring14.*PU20bx25_POSTLS170.*/AODSIM'
        rx2=r'/.*/Spring14.*S14_POSTLS170.*/AODSIM'
        rx3=r'/.*/Spring14.*PU40bx25_POSTLS170.*/AODSIM'
        rx4=r'/.*/Spring14.*/MINIAODSIM'
        rx5=r'/.*/.*/USER'
        try:
            if fileNumbers[key]:
                value=float(value)/fileNumbers[key]
            else:
                # if a dataset has no files, why is it being accessed?
#                print "Warning, no files in %s"%(key)
                value = 0
        except KeyError:
            pass
        if re.search(rx1,key) or re.search(rx2,key) or re.search(rx3,key) or re.search(rx4,key) or re.search(rx5,key):
            # this is CSA14 [MINI]AODSIM
            nCSA += value
            nDatasetsCSA+=1
        elif re.search(r'/.*/.*/.*AOD.*',key):
            # this is any other AODSIM
            nOthers += value
            nDatasetsOthers+=1
    # return the datasets
    if nDatasetsCSA:
        nCSA=nCSA/nDatasetsCSA
    if nDatasetsOthers:
        nOthers=nOthers/nDatasetsOthers
    return (nCSA, nOthers)

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

def cleanHistories(xfers,dels,end):
    orig_xfers=xfers[:]
    orig_dels=dels[:]
    # used to clean up messy histories
    i=0
    j=0
    ld=-1
    if len(dels)==0:
        # if there are no deletions in the history, assume it's still there
        # if there are no transfers in the history, and you are here, SOMETHING IS VERY WRONG
        dels=[end]
    if xfers[0] > dels[0] and xfers[0] <= end:
        # if there's a deletion before the first transfer and the first transfer is in the window
        # assume the sample has been here since the beginning of time
        xfers.insert(0,0)
    while len(xfers) > len(dels):
        dels.append(end)
    while True:
        nx=len(xfers)
        nd=len(dels)
        try:
            if i==nx and j==nd:
                break
            elif i < len(xfers) and xfers[i] < ld:
                xfers.pop(i)
            elif i < len(xfers) and j < len(dels) and xfers[i] > dels[j] and xfers[i] <= end:
                dels.pop(j)
            elif i < len(xfers) and j < len(dels) and xfers[i] > end:
                xfers.pop(i)
            else:
                i+=1
                j+=1
                if j <= len(dels):
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
            sys.stderr.write("Exception IndexError. Array info:")
            sys.stderr.write("%s ==> %s\n"%(orig_xfers,xfers))
            sys.stderr.write("%s ==> %s\n"%(orig_dels,dels))
            sys.stderr.write("%i %i\n"%( i,j))
            sys.exit(-1)
    #    print xfers
    #    print dels
    if xfers==[]:
            dels=[]
    return xfers,dels


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
    nSitesCSA = 0
    nSitesOthers = 0
    nCSA=0
    nOthers=0

    # read in the data from the phedex history database cache
    #========================================================
    isCSA={}
    rx1=r'/.*/Spring14.*PU20bx25_POSTLS170.*/AODSIM'
    rx2=r'/.*/Spring14.*S14_POSTLS170.*/AODSIM'
    rx3=r'/.*/Spring14.*PU40bx25_POSTLS170.*/AODSIM'
    rx4=r'/.*/Spring14.*/MINIAODSIM'
    rx5=r'/.*/.*/USER'
    for file in files:
        filename = file.split('/')[-1].replace('%','/')
        if re.search(rx1,filename) or re.search(rx2,filename) or re.search(rx3,filename) or re.search(rx4,filename):
        #if re.search(rx1,filename) or re.search(rx2,filename) or re.search(rx3,filename) or re.search(rx4,filename) or re.search(rx5,filename):
            isCSA[filename]=True
        elif re.search(r'/.*/.*/.*AOD.*',filename):
            isCSA[filename]=False
        else:
            #neither CSA nor AOD
            continue
        try:                                                      # check that we can parse the xml file
            document = ElementTree.parse(file)
        except:
            print ' WARNING - file reading failed (%s).'%file
            continue
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
        firstTransfer=end
        lastDeletion=0
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

            if lenXfer==0:
                # this actually doesn't make sense
                continue
            xfers= datasetMovement[filename][site][0]
            dels=  datasetMovement[filename][site][1]
            datasetMovement[filename][site][0],datasetMovement[filename][site][1] = cleanHistories(xfers,dels,end)
            while len(datasetMovement[filename][site][0]) < len(datasetMovement[filename][site][1]):
                datasetMovement[filename][site][0].insert(0,0)
            while len(datasetMovement[filename][site][0]) > len(datasetMovement[filename][site][1]):
                #print 'Add interval end: %d'%end
                datasetMovement[filename][site][1].append(end)
            if len(datasetMovement[filename][site][0]):
                firstTransfer = min(firstTransfer,min(datasetMovement[filename][site][0]))
                lastDeletion = max(lastDeletion,max(datasetMovement[filename][site][1]))

        datasetSum = 0

        for site in datasetMovement[filename]:
            #print ' Adding site: ' + site
            if not re.match(sitePattern,site):                   # only requested sites
                print ' WARNING - no site match. ' + site
                continue
            lenXfer = len(datasetMovement[filename][site][0])
            if lenXfer==0:
                continue
            lenDel  = len(datasetMovement[filename][site][1])
            

            siteSum = 0
            i       = 0

            # the nSitesAverage should never (in principle) be below 1. This can erroneously happen
            # if the dataset is created after start or deleted (i.e. not present on any T2s) before end
            # we redefine [start,end] as the intersection of [start,end] and [created,deleted]
            start_tmp = max(start,firstTransfer)
            end_tmp = min(end,lastDeletion)
            interval_tmp = end_tmp - start_tmp
            #start_tmp=start
            #end_tmp=end
            #interval_tmp=interval

            # loop through the transfer/deletion intervals
            while i < lenXfer:
                try:
                    tXfer = datasetMovement[filename][site][0][i]
                    tDel  = datasetMovement[filename][site][1][i]
                except IndexError:
                    sys.stderr.write("Exception IndexError. Index info:")
                    sys.stderr.write("%i %i %s\n"%(lenXfer,lenDel,filename))
                    sys.exit(-1)

                i = i + 1                                        # iterate before all continue statements

                if tXfer == 0 and tDel == end:                   # skip if defaults, meaning no xfer or del time found
                    continue                                     # shouldn't happen, but just to be sure

                # four ways to be in interval                    # (though this prevents you from having the same
                                                                 #  start and end date)

                if tXfer <= start_tmp <= end_tmp <= tDel:
                    siteSum += 1                                 # should happen at most once, but okay
                elif tXfer <= start_tmp < tDel < end_tmp:
                    siteSum += float(tDel - start_tmp)/float(interval_tmp)
#                    print ' Adding: %d %f'%(i,float(tDel - start)/float(interval))
                elif start_tmp < tXfer < end_tmp <= tDel:
                    siteSum += float(end_tmp - tXfer)/float(interval_tmp)
 #                   print ' Adding: %d %f'%(i,float(end - tXfer)/float(interval))
                elif start_tmp < tXfer < tDel <= end_tmp:
                    siteSum += float(tDel - tXfer)/float(interval_tmp)
  #                  print ' Adding: %d %f'%(i,float(tDel - tXfer)/float(interval))
                else:                                            # have ensured tXfer < tDel
                    continue
                '''if tXfer <= start <= end <= tDel:
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
                    continue'''

            if siteSum < 0:                                      # how can this happen??
                print " WARNING - siteNum < 0 ??"
                continue
            datasetSum+=siteSum
        #print filename,isCSA[filename],datasetSum
        if isCSA[filename] and datasetSum:
            print datasetSum,filename,"\n\t",datasetMovement[filename]
            # only look at those files which are actually on the tier2s at the time of query
            nSitesCSA += datasetSum
            nCSA+=1
        elif datasetSum:
            nSitesOthers += datasetSum
            nOthers+=1
    return float(nSitesCSA)/nCSA,float(nSitesOthers)/nOthers

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
sizeAnalysis = True
addNotAccessedDatasets = True

usage  = "\n"
usage += " %s  <sitePattern> [ <datePattern>=????-??-?? ]\n"%(sys.argv[0])
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   datePattern  - pattern to select date pattern (ex. 201[34]-0[0-7]-??)\n\n"

date=[] # all date patterns to consider
# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    date.append('????-??-??')
elif len(sys.argv)>=3:
    site = str(sys.argv[1])
    for arg in sys.argv[2:]:
        date.append(str(arg))

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
files=[]
for d in date:
    files += glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + d)
# say what we are goign to do
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Logfiles in:  %s"%(workDirectory)
print " Site pattern: %s"%(site)
print " Date pattern: %s"%(date)
if sizeAnalysis:
    print "\n Size Analysis is on. Please be patient, this might take a while!\n"

firstFile = ''
lastFile = ''
accessesByDate={}
startDate=''
for fileName in sorted(files):
    if debug>0:
        print ' Analyzing: ' + fileName

    g = fileName.split("/")
    siteName = g[-3]
    
    if firstFile == '':
        firstFile = fileName
        startDate = g[-1]

    
    if siteName in nSiteAccess:
        nSiteAccessEntry = nSiteAccess[siteName]
    else:
        nSiteAccessEntry = {}
        nSiteAccess[siteName] = nSiteAccessEntry

    # analyze this file
    (nCSA, nOthers) = processFile(fileName,fileNumbers,debug)
    if g[-1] in accessesByDate:
        accessesByDate[g[-1]] = (accessesByDate[g[-1]][0] + nCSA , accessesByDate[g[-1]][1] + nOthers )
    else:
        accessesByDate[g[-1]] = ( nCSA , nOthers )

if lastFile == '':
    lastFile = fileName

#print " First: %s  Last: %s"%(firstFile,lastFile)

# find start and end times

dir = '/'.join(lastFile.split("/")[0:-1])

files = glob.glob(dir + '/????-??-??')
endDate =  lastFile.split("/")[-1]

# convert to epoch time
start={}
end={}
for d in sorted(accessesByDate):
    start[d] = int(time.mktime(time.strptime(d,'%Y-%m-%d')))
    end[d] = int(time.mktime(time.strptime(d,'%Y-%m-%d'))) + 7*24*3600  # one month

# --------------------------------------------------------------------------------------------------
# create summary information and potentially print the contents
# --------------------------------------------------------------------------------------------------

# figure out properly the 'average number of sites' in the given time interval
nAverageSites={}
for d in sorted(accessesByDate):
    nAverageSites[d] = calculateAverageNumberOfSites('T2',start[d],end[d])

# last step: produce monitoring information
# ========================================
# ready to use: nAllAccessed[dataset], fileNumbers[dataset], sizesGb[dataset], nSites[dataset]

fileName = 'CSASummary.txt'
print ' Output file: ' + fileName
totalAccesses = open(fileName,'w')
totalCSA=0
totalOthers=0
totalAccesses.write("date\taccessesCSA\taccessesOthers/nSitesAvCSA\tnSitesAvOthers\n")
for d in sorted(accessesByDate):
    totalCSA+=accessesByDate[d][0]
    totalOthers+=accessesByDate[d][1]
    totalAccesses.write("%s\t%f\t%f\t%f\t%f\n"%(d,totalCSA,totalOthers,nAverageSites[d][0],nAverageSites[d][1]))
totalAccesses.close()

sys.exit(0)
