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
import subprocess, sys, os, re, glob, time
import datetime
from   datetime import datetime, timedelta

if not os.environ.get('DETOX_DB'):
	print '\n ERROR - DETOX environment not defined: source setup.sh\n'
	sys.exit(0)

if len(sys.argv) < 2:
    print 'not enough arguments\n'
    sys.exit()
else:
    federations = sys.argv[1:]

allDatasets = {}
datasetsAtSites = {}
skipDataset = []
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
    # Launch the shell command
    strout, error = process.communicate()

    #child = pexpect.spawn(cmd,timeout=1200)
    #child.expect(pexpect.EOF)
    #strout = child.before

    timeNow = time.time()
    print '   - Phedex query took: %d seconds'%(timeNow-timeStart) 

    timeStart = time.time()
    records = re.split("block\ bytes",strout)
    for line in records:

        if "name" not in line:
            continue

        group = None
        size = None
        creationTime = None
        datasetName = (re.findall(r"name='(\S+)'", line))[0]

        if "#" in datasetName:
            datasetName = (re.split("#",datasetName))[0]

	complete = (re.findall(r"complete='(\w+)'",line))[0]
        if complete == 'n':
            skipDataset.append(datasetName)
	    continue

        res = re.findall(r"group='(\S+)'",line)

        if len(res) > 0:
            group = res[0]
        else:
            group = "undef"

        creationTime = int((re.findall(r"time_update='(\d+)",line))[0])
        size = float((re.findall(r"bytes='(\d+)'",line))[0])
        size = size/1024/1024/1024

	t2Site = re.findall(r"node='(\w+)'",line)
	custodial = (re.findall(r"custodial='(\w+)'",line))[0]
        if custodial == 'y':
	    continue
        
        if datasetName in allDatasets.keys():
            size = size + (allDatasets[datasetName])[2]
            if creationTime < (allDatasets[datasetName])[1]:
                creationTime = (allDatasets[datasetName])[1]
        allDatasets[datasetName] = [group,creationTime,size]

        if datasetName in datasetsAtSites.keys():
            for esite in t2Site:
                if esite not in (datasetsAtSites[datasetName]):
                    datasetsAtSites[datasetName].append(esite)
        else:
            datasetsAtSites[datasetName] = t2Site

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
for datasetName in allDatasets :
    if datasetName in skipDataset:
        continue

    group = (allDatasets[datasetName])[0]
    creationTime = (allDatasets[datasetName])[1]
    size = (allDatasets[datasetName])[2]
    line = datasetName + " " + group + " " + str(creationTime) + " " + str(size) + " "
    tmpar = datasetsAtSites[datasetName]
    for i in range(len(tmpar)) :
        line = line + str(tmpar[i]) + " "
    outputFile.write(line)
    outputFile.write("\n")
outputFile.close()

timeNow = time.time()
print ' - Creating phedex cache took: %d seconds'%(timeNow-timeStart) 
