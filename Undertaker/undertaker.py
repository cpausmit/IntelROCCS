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
if not os.environ.get('UNDERTAKER_BASE'):
    print '\n ERROR - environment not defined: source setup.sh\n'
    sys.exit(0)

# make sure we start in the right directory
os.chdir(os.environ.get('UNDERTAKER_BASE'))

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def makeArea():
    area = os.environ['UNDERTAKER_DB'] + '/' + os.environ['UNDERTAKER_TRDIR']
    if not os.path.exists(area):
        os.system('mkdir -p ' + area)

#====================================================================================================
#  M A I N
#====================================================================================================

timeStart = time.time()

makeArea()

centralManager = centralManager.CentralManager()
centralManager.checkProxyValid()

# Get a list of phedex datasets (goes to cache)
timeStart = time.time()
print ' Extracting SiteReadiness Information'
centralManager.extractReadinessData()
timeNow = time.time()
print ' - Extracting SiteReadiness information took: %d seconds'%(timeNow-timeStart)
timePre = timeNow

centralManager.printResults()

centralManager.findStateChanges()

centralManager.enableSites()

centralManager.processPending()
if not centralManager.canProceed():
    sys.exit(1)

centralManager.resignDatasets()

# Final summary of timing
timeNow = time.time()
print ' Total Cycle took: %d seconds'%(timeNow-timeStart)
