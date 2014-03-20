#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script talks to the PhEDEx database and writes down the dataset name, group it belongs to,
# size of the dataset, it's creation time, and on what Tier2 sites it resides. If dataset is marked
# as custodial it will be skipped.  None of the datasets located on Tier2s should be marked as
# custodial.  If dataset is inclomplete anywhere it will be removed from consideration.
#
# The script takes as arguments what federations of sites it should consider. For all Tier2s, for
# example, enter T2. For Tier-2 and Tier-1 use arguments T1 T2.
#
#----------------------------------------------------------------------------------------------------
import subprocess, sys, os, re, glob, time, datetime
from datetime import datetime, timedelta
from phedexDataset import PhedexDataset

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

if len(sys.argv) < 2:
    print 'not enough arguments\n'
    sys.exit()
else:
    federations = sys.argv[1:]

phedexDatasets = {}
renewMinInterval = int(os.environ.get('DETOX_CYCLE_HOURS'))     # number of hours until it will rerun

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def getDatasetsInPhedexAtSites(federation):

    timeStart = time.time()
    timeNow   = timeStart

    webServer = 'https://cmsweb.cern.ch/'
    phedexBlocks = 'phedex/datasvc/xml/prod/blockreplicas'
    args = 'subscribed=y&node=' + federation + '*'

    cert = os.environ['DETOX_X509UP']

    url = '"'+webServer+phedexBlocks+'?'+args+'"'
    cmd = 'curl --cert ' + cert + ' -k -H "Accept: text/xml" ' + url
    
    print ' Access phedexDb: ' + cmd 

    # setup the shell command
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   shell=True)
    # launch the shell command
    strout, error = process.communicate()

    timeNow = time.time()
    print '   - Phedex query took: %d seconds'%(timeNow-timeStart) 

    timeStart = time.time()
    records = re.split("block\ bytes",strout)
    for line in records:

        if "name" not in line:
            continue

        datasetName = (re.findall(r"name='(\S+)'", line))[0]

        if "#" in datasetName:
            datasetName = (re.split("#",datasetName))[0]

    phedexSet = None
    if datasetName not in phedexDatasets.keys():
        phedexSet = PhedexDataset(datasetName)
        phedexDatasets[datasetName] = phedexSet
    else:
        phedexSet = phedexDatasets[datasetName]

    sublines = re.split("<replica\ ",line)
    for subline in sublines[1:]:
        
        group = None
        size = None
        creationTime = None
        isValid = True
        isCustodial = False
        
        t2Site = re.findall(r"node='(\w+)'",subline)[0]
        res = re.findall(r"group='(\S+)'",subline)
        if len(res) > 0:
            group = res[0]
        else:
            group = "undef"
            
        custodial = re.findall(r"custodial='(\w+)'",subline)
        if custodial == 'y':
            isCustdial = True

        complete = re.findall(r"complete='(\w+)'",subline)
        if complete == 'n':
            isValid = False

        creationTime = re.findall(r"time_update='(\d+)",subline)[0]
        creationTime = int(re.findall(r"time_update='(\d+)",subline)[0])
        size   = float(re.findall(r"bytes='(\d+)'",subline)[0])
        size   = size/1024/1024/1024

        phedexSet.updateForSite(t2Site,size,group,creationTime,isCustodial,isValid)

    timeNow = time.time()
    print '   - Analysis of phedex output took: %d seconds'%(timeNow-timeStart) 

#====================================================================================================
#  M A I N
#====================================================================================================
statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
filename = os.environ['DETOX_PHEDEX_CACHE']

timeNow = datetime.now()
deltaNhours = timedelta(seconds = 60*60*renewMinInterval)
modTime = datetime.fromtimestamp(0)
if os.path.isfile(statusDir+'/'+filename):
    modTime = datetime.fromtimestamp(os.path.getmtime(statusDir+'/'+filename))

if (timeNow-deltaNhours) < modTime:
    sys.exit(0)

if os.path.isfile(statusDir+'/'+filename):
    os.remove(statusDir+'/'+filename)
        
# New zero for timing
timeStart = time.time()

# Loop through requested federations to find phedex information
for federation in federations:
    getDatasetsInPhedexAtSites(federation)

timeNow = time.time()
print ' - Phedex access and analysis took: %d seconds'%(timeNow-timeStart) 

# New zero for timing
timeStart = time.time()

outputFile = open(statusDir+'/'+filename, "w")
for datasetName in phedexDatasets :
    phedexSet = phedexDatasets[datasetName]
    allSites = phedexSet.locatedOnSites()
    if(len(allSites) < 1):
        continue

    for site in allSites:
    
        size = phedexSet.size(site)
        creationTime = phedexSet.creationTime(site)
        group = phedexSet.group(site)
        
        line = datasetName + " " + group + " " + str(creationTime)
        line = line + " "  + str(size) + " " +site
        outputFile.write(line)
        outputFile.write("\n")
outputFile.close()

timeNow = time.time()
print ' - Creating phedex cache took: %d seconds'%(timeNow-timeStart) 
