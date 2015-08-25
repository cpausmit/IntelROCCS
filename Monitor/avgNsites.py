#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# Function to calculate average number of sites per dataset during a given time period and list of
# sites.
#
#---------------------------------------------------------------------------------------------------
import sys, os, re, time, glob
import phedexRequestInfo
from   time      import mktime
from   xml.etree import ElementTree

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def readRequestInfo(reqId):
    # for a given request Id read the full information into a xml structure

    file = 'phedex-' + reqId + '.xml' 
    cmd = "wget --no-check-certificate -O " + file + " " \
        + "https://cmsweb.cern.ch/phedex/datasvc/xml/prod/transferrequests?request=" + reqId \
        + " 2> /dev/null"
    print " CMD: " + cmd
    os.system(cmd)

    # initialize our container
    document = {}
    try:                                                      # check that we can parse the xml file
        document = ElementTree.parse(file)
    except:
        print ' WARNING - file reading failed (%s).'%file

    # cleanup the file we used
    os.system("rm -f " + file)

    # return the resulting xml structure (just the request and there can be only one)
    if len(document.findall('request')) != 1:
        return {}
    else:
        return document.findall('request')[0]

def isMove(requestInfo):
    # for a given xml structure of a requestInfo extract whether it is a move
    return requestInfo.attrib['move'] == 'y'

def getGroup(requestInfo):
    # for a given xml structure of a requestInfo extract responsible group
    return requestInfo.attrib['group']

def getSources(requestInfo):
    # for a given xml structure of a requestInfo extract the source(s)

    sources = []
    for source in requestInfo.findall('move_sources'):
        for node in source.findall('node'):
            site = node.attrib['name']
            #print ' Move_Source: ' + site
            sources.append(site)
    return sources

def getDestinations(requestInfo):
    # for a given xml structure of a requestInfo extract the destination(s)

    destinations = []
    for destination in requestInfo.findall('destinations'):
        for node in destination.findall('node'):
            site = node.attrib['name']
            #print ' Destination: ' + site
            destinations.append(site)
    return destinations

def getDatasets(requestInfo):
    # for a given xml structure of a requestInfo extract the datasets affected

    datasets = []
    for data in requestInfo.findall('data'):
        for dbs in data.findall('dbs'):
            for dataset in dbs.findall('dataset'):
                datasetName= dataset.attrib['name']
                #print ' Dataset: ' + datasetName
                datasets.append(datasetName)
    return datasets


#===================================================================================================
#  M A I N
#===================================================================================================

usage = '\n usage: ./avgNsites.py sitePattern startDate endDate\n'

# command line arguments
if   len(sys.argv) != 4:
    print usage
    sys.exit(1)
else:
    sitePattern = str(sys.argv[1])
    try:
        sdate    = str(sys.argv[2])
        edate    = str(sys.argv[3])
        start    = int(mktime(time.strptime(sdate,'%Y-%m-%d')))  # convert to epoch time
        end      = int(mktime(time.strptime(edate,'%Y-%m-%d')))
        # determine the length of the interval
        interval = end - start
        print ' Relevant time interval: %d %d  --> %d'%(start,end,end-start)
    except:
        print usage
        print '\n time format: yyyy-mm-dd\n'
        sys.exit(2)

# define our variables
phedDirectory = '/local/cmsprod/IntelROCCS/Monitor/datasets/' # FIX: use env from setupMonitor.sh
filePattern   = '%*%*%*'                                      # consider only xml filename pattern
filePattern   = '%SMS-T2bw_2J_mStop*%*%*'                     # consider only xml filename pattern
#filePattern = '%GJet_Pt-15to3000_Tune4C_13TeV_pythia8%Spring14dr-PU20bx25_POSTLS170_V5-v1%AODSIM'
#filePattern = '%QCD_Pt-120to170_MuEnrichedPt5_TuneZ2star_13TeV_pythia6%Spring14dr-PU_S14_POSTLS170_V6-v1%AODSIM'
files         = glob.glob(phedDirectory + filePattern)
nowish        = int(round(time.time()/300)*300)               # now to the nearest five min
datasetMovement          = {}
nSites        = {}

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

        # investigate what was done in this request
        requestInfo = phedexRequestInfo.PhedexRequestInfo(request.attrib['id'])
        
        if not requestInfo.isValid():
            continue
            
        #=========================================================================
        # filter out requests that are not relevant (DataOps movement or similar)
        #
        # - replication: interesting as long as destination does contain a T2 site
        # - move: generally not interesting
        # - deletion: interesting as long as it is at a T2
        #=========================================================================

        # skip quickly what we can skip
        move = requestInfo.isMove()
        if move:
            continue
        group = requestInfo.getGroup()
        if group == 'DataOps':
            continue

        # now it becomes more labor intensive
        sources = requestInfo.getSources()
        destinations = requestInfo.getDestinations()
        datasets = requestInfo.getDatasets()


        type = request.attrib['type']        

        for node in request.findall('node'):
            site = node.attrib['name']
            time = node.attrib['time_decided']

            #print ' Checking %s versus %s'%(sitePattern,site)

            if not re.match(sitePattern,site):                      # consider only specified sites
                #print ' WARNING - no site match.'
                continue

            if not filename in datasetMovement:                     # only for datasets from correct sites
                datasetMovement[filename] = {}

            if node.attrib['decision'] == 'approved':               # only consider things that happened
                try:                                                # check for bad times
                    time = int(float(node.attrib['time_decided']))  # whole number for easy binning
                except:
                    print ' WARNING - time decided is corrupt.'
                    continue
                if not site in datasetMovement[filename]:           # initialization
                    datasetMovement[filename][site] = [[],[]]       # allow multiple dataset instances/site
                if type == 'xfer':                                  # (datasets can be requested again)
                    datasetMovement[filename][site][0].append(time) # time transferred
                    #print ' transfer [%s]: %d'%(site,time)
                elif type == 'delete':
                    datasetMovement[filename][site][1].append(time) # time deleted
                    #print ' delete   [%s]: %d'%(site,time)
                else:
                    continue

# match the intervals from the phedex history to the requested time interval
#===========================================================================
print ' Match intervals and calculate the effective number of sites (or copies)'

for filename in datasetMovement:
    # ensure datasetMovement entry is complete
    for site in datasetMovement[filename]:
        print ' Adding site: ' + site
        if not re.match(sitePattern,site):                   # only requested sites
            #print ' WARNING - no site match.'
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
            i=i+1                                            # iterate before all continue statements

            if tXfer == 0 and tDel == end:                   # skip if defaults, meaning no xfer or del time found
                continue                                     # shouldn't happen, but just to be sure

            # four ways to be in interval                    # (though this prevents you from having the same
                                                             #  start and end date)
            if tXfer <= start <= end <= tDel:
                siteSum += 1                                 # should happen at most once, but okay
                print ' Adding: %d %f'%(i,1)
            elif tXfer <= start < tDel < end:
                siteSum += float(tDel - start)/float(interval)
                print ' Adding: %d %f'%(i,float(tDel - start)/float(interval))
            elif start < tXfer < end <= tDel:
                siteSum += float(end - tXfer)/float(interval)
                print ' Adding: %d %f'%(i,float(end - tXfer)/float(interval))
            elif start < tXfer < tDel <= end:
                siteSum += float(tDel - tXfer)/float(interval)
                print ' Adding: %d %f'%(i,float(tDel - tXfer)/float(interval))
            else:                                            # have ensured tXfer < tDel
                continue

        if siteSum < 0:                                      # how can this happen??
            continue
        
        nSites[filename] += siteSum

# find overall average and print results
#=======================================

n     = 0
nSkip = 0
sum   = float(0)

for filename in nSites:
    if nSites[filename] == 0:                                # dataset not on sites in interval
        nSkip += 1
        continue
    sum += nSites[filename]
    n   += 1
    print ' %s \t%f'%(filename,nSites[filename])

print '\n Dataset  --  average number of sites\n'

if n != 0:
    avg = sum/n
    print '\n overall average n sites: %f\n'%(avg)
    print '\n number of datasets:      %d\n'%(n)
    print '\n number of datasets skip: %d\n'%(nSkip)
else:
    print 'no datasets found in specified time interval'
