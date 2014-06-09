#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This is the master script that user should run It runs all other auxilary scripts. At the end you
# will have full set of deletion suggestions.
#
# For now we get the list of all sites from the file with quotas because we need to know the
# capacity of each site (and right now we use only one group as proxy).
#
#---------------------------------------------------------------------------------------------------
import sys, os, subprocess, re, glob, time, pickle, MySQLdb
import siteStatus

# setup definitions
if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# make sure we start in the right directory
os.chdir(os.environ.get('DETOX_BASE'))

# allow to switch each piece of the code on and off for faster debugging
getPhedexCache = True
getPopularityCache = True
extractUsedDatasets = True
rankDatasets = True
makeDeletionLists = True
requestDeletions = True
# requestDeletions = False

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
# Retrieve all sites from the quota file
timeInitial = time.time()

allSites = siteStatus.getAllSites()

# Make directories to hold cache data

createCacheAreas()

# Get a list of phedex datasets (goes to cache)

timeStart = time.time()
print ' Cache phedex information.'
if getPhedexCache:
    retValue = subprocess.call(["./cacheDatasetsInPhedexAtSites.py", "T2"])
    if retValue != 0:
	print " ERROR - Call to cacheDatasetsInPhedexAtSites.py failed, exit code %d"%(retValue)
        sys.exit(1)
timeNow = time.time()
print ' - Renewing phedex cache took: %d seconds'%(timeNow-timeStart) 

# For each site update popularity, rank datasets, perform the necessary release list

timeStart = time.time()
print ' Collect site information information.'
for site in sorted(allSites):
    if allSites[site].getStatus() == 0:
        continue

    # extract usage data from popularity service
    if getPopularityCache:
        retValue = subprocess.call(["./cacheDatasetsPopularity.py",site])
        if retValue != 0:
            allSites[site].setValid(0)
            sys.exit(1)

    # unify datasets as given by the popularity service and phedex
    if extractUsedDatasets:
        subprocess.call(["./extractUsedDatasets.py",site])

    # rank datasets for each site
    if rankDatasets:
        subprocess.call(["./rankDatasets.py",site])
timeNow = time.time()
print ' - Collecting site information took: %d seconds'%(timeNow-timeStart) 

strAllSites = pickle.dumps(allSites)

# For global ranking we will read all local rankings, calculate global rank for each dataset, and
# update the files.
subprocess.call(["./rankDatasetsGlobally.py",strAllSites])

# Run the script that unifies all of sites and creates deletion suggestions
timeStart = time.time()
print ' Create deletion lists.'
if makeDeletionLists:
    subprocess.call(["./makeDeletionLists.py",strAllSites])
timeNow = time.time()
print ' - Creation of deletion lists took: %d seconds'%(timeNow-timeStart)

# Run the script that makes the deletion request to phedex
timeStart = time.time()
print ' Make deletion request.'
if requestDeletions:
    subprocess.call(["./requestDeletions.py",strAllSites])
timeNow = time.time()
print ' - Deletion requests took: %d seconds'%(timeNow-timeStart) 

# Run the script that lists the last x deletion requests to phedex
print ' Show cache release requests.'
for site in sorted(allSites):
    if allSites[site].getStatus() == 0:
        print ' Site not active, status=%d  - %s'%(allSites[site].getStatus(),site)
    else:
        print ' Site --- active, status=%d  - %s'%(allSites[site].getStatus(),site)
        subprocess.call(["./showCacheRequests.py",site])

# Final summary of timing
print ' Total Cycle took: %d seconds'%(timeNow-timeInitial) 
