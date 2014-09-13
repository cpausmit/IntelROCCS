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

# set this to turn off actual deletions for debugging
requestDeletions = True

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def createCacheAreas():
    # create directory structure where to cache our input and analysis output
    if not os.path.exists(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']):
        os.system('mkdir -p ' + os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'])
    if not os.path.exists(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']):
        os.system('mkdir -p ' + os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT'])

#====================================================================================================
#  M A I N
#====================================================================================================

timeStart = time.time()

centralManager = centralManager.CentralManager()
centralManager.checkProxyValid()

#if you need to approve requests by hand
#centralManager.submitUpdateRequest('T2_RU_IHEP',421768)

# Make directories to hold cache data

createCacheAreas()

# Get a list of phedex datasets (goes to cache)
timeStart = time.time()
print ' Extracting PhEDEx Information'
centralManager.extractPhedexData("node=T2*&node=T1*")
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
centralManager.rankDatasetsGlobally()
centralManager.makeDeletionLists()

timeNow = time.time()
print ' - Making deletion lists took: %d seconds'%(timeNow-timePre)
timePre = timeNow

centralManager.extractCacheRequests()

if requestDeletions:
    centralManager.requestDeletions()

timeNow = time.time()
print ' - Requesting deletions took: %d seconds'%(timeNow-timePre)
timePre = timeNow

centralManager.showCacheRequests()

# Final summary of timing
timeNow = time.time()
print ' Total Cycle took: %d seconds'%(timeNow-timeStart)
