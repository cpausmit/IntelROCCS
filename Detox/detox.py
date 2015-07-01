#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This is the master script that user should run It runs all other auxilary scripts. At the end you
# will have full set of deletion suggestions.
#
# For now we get the list of all sites from the file with quotas because we need to know the
# capacity of each site (and right now we use only one group as proxy).
#
#---------------------------------------------------------------------------------------------------
import sys, os, subprocess, re, glob, time, MySQLdb
import siteStatus
import centralManager

# setup definitions
if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# make sure we start in the right directory
os.chdir(os.environ.get('DETOX_BASE'))
topWorkArea = os.environ['DETOX_DB']

phedexGroups = (os.environ['DETOX_GROUP']).split(',')

# set this to turn off actual deletions for debugging
requestDeletions = True
requestTransfers = False

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def createCacheAreas():
    # create directory structure where to cache our input and analysis output
    statusArea = topWorkArea + '/' + os.environ['DETOX_STATUS']
    if not os.path.exists(statusArea):
        os.system('mkdir -p ' + statusArea)

    resultArea = topWorkArea + '/' + os.environ['DETOX_RESULT']
    if not os.path.exists(resultArea):
        os.system('mkdir -p ' + resultArea)

#====================================================================================================
#  M A I N
#====================================================================================================

timeStart = time.time()

centralManager = centralManager.CentralManager()
centralManager.checkProxyValid()

#if you need to approve requests by hand
#centralManager.submitUpdateRequest('T2_PT_NCG_Lisbon',433579)
#dataset='/SMHiggsToWWTo2Tau2Nu_M-125_7TeV-jhu-pythia6/Fall11-PU_S6_START42_V14B-v1/AODSIM'
#centralManager.changeGroup('T2_US_MIT', dataset, 'AnalysisOps')

# Make directories to hold cache data
createCacheAreas()

# Get a list of phedex datasets (goes to cache)
timeStart = time.time()
print ' Extracting PhEDEx Information'
centralManager.extractPhedexData("node=T2*&node=T1_*_Disk")
timeNow = time.time()
print ' - Extracting PhEDEx information took: %d seconds'%(timeNow-timeStart)
timePre = timeNow

print ' Extracting Deprecated Datasets'
timeStart = time.time()
centralManager.extractDeprecatedData()
timeNow = time.time()
print ' - Extracting daprecated datasets took: %d seconds'%(timeNow-timeStart)
timePre = timeNow

# extract usage data from popularity service
print ' Collect site information information.'
centralManager.extractPopularityData()
timeNow = time.time()
print ' - Collecting site information took: %d seconds'%(timeNow-timePre)
timePre = timeNow

centralManager.rankDatasetsLocally()
centralManager.extractCacheRequests()
centralManager.findExtraUsage() 

timeNow = time.time()
timePre = timeNow

for iii in range(0, len(phedexGroups)):
    phedGroup = phedexGroups[iii]
    mode = 'w'
    if iii > 0:
        mode = 'a'

    print "\n--- Processing group " + phedGroup + " ---\n"

    centralManager.rankDatasetsGlobally(phedGroup)
    centralManager.makeDeletionLists(phedGroup)

    timeNow = time.time()
    print ' - Making deletion lists took: %d seconds'%(timeNow-timePre)
    timePre = timeNow
    
    if requestDeletions and phedGroup == 'AnalysisOps':
        centralManager.requestDeletions()

    timeNow = time.time()
    print ' - Requesting deletions took: %d seconds'%(timeNow-timePre)
    timePre = timeNow

    centralManager.extractCacheRequests()
    centralManager.showCacheRequests()
    timeNow = time.time()
    print ' - Extracting requests from database took: %d seconds'%(timeNow-timePre)
    timePre = timeNow

    if phedGroup == 'AnalysisOps':
        centralManager.updateSiteStatus()

    centralManager.printResults(phedGroup,mode)
    timeNow = time.time()
    print ' - Printing results took: %d seconds'%(timeNow-timePre)
    timePre = timeNow

    if requestTransfers:
        print ' Subscribe datasets to T1s'
        centralManager.assignToT1s()
        timeNow = time.time()
        print ' - Subscribing to T1s took: %d seconds'%(timeNow-timePre)
        timePre = timeNow

# Final summary of timing
timeNow = time.time()
print ' Total Cycle took: %d seconds'%(timeNow-timeStart)
