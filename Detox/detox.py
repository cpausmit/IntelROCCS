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
import sys, os, re, glob, time, MySQLdb
import datetime
from   datetime import date, timedelta
from subprocess import call

# setup definitions
if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# make sure we start in the right directory
os.chdir(os.environ.get('DETOX_BASE'))

# debug flag
debug = 0

# allow to switch each piece of the code on and off for faster debugging
getPhedexCache = True
getPopularityCache = True
extractUsedDatasets = True
rankDatasets = True
makeDeletionLists = True
requestDeletions = True

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def getAllSites():
    # we are looking for all relevant sites
    sites = []
    db = os.environ.get('DETOX_SITESTORAGE_DB')
    server = os.environ.get('DETOX_SITESTORAGE_SERVER')
    user = os.environ.get('DETOX_SITESTORAGE_USER')
    pw = os.environ.get('DETOX_SITESTORAGE_PW')
    table = os.environ.get('DETOX_QUOTAS')
    # open database connection
    print ' Access quota table (%s) in site storage database (%s) to find all sites.'%(table,db)
    db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # define sql
    sql = "select * from " + table
    # go ahead and try
    try:
        # Execute the SQL command
        if debug>0:
            print '\n Mysql> ' + sql
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            siteName = row[0]
            groupName = row[1]
            if not siteName in sites:
                if debug>0:
                    print ' Appending: ' + siteName
                sites.append(siteName)
    except:
        print ' Error(%s) -- could not retrieve sites'%(sql)
        print sys.exc_info()

    return sites

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

allSites = getAllSites()

# Make directories to hold cache data

createCacheAreas()

# Get a list of phedex datasets (goes to cache)

timeStart = time.time()
print ' Cache phedex information.'
if getPhedexCache:
    call(["./cacheDatasetsInPhedexAtSites.py", "T2"])
timeNow = time.time()
print ' - Renewing phedex cache took: %d seconds'%(timeNow-timeStart) 
    
# For each site update popularity, rank datasets, perform the necessary release list

timeStart = time.time()
print ' Collect site information information.'
for site in allSites:

    # extract usage data from popularity service
    if getPopularityCache:
        call(["./cacheDatasetsPopularity.py",site])

    # unify datasets as given by the popularity service and phedex
    if extractUsedDatasets:
        call(["./extractUsedDatasets.py",site])

    # rank datasets for each site
    if rankDatasets:
        call(["./rankDatasets.py",site])
timeNow = time.time()
print ' - Collecting site information took: %d seconds'%(timeNow-timeStart) 

# Run the script that unifies all of sites and creates deletion suggestions
timeStart = time.time()
print ' Create deletion lists.'
if makeDeletionLists:
    call(["./makeDeletionLists.py"])
timeNow = time.time()
print ' - Creation of deletion lists took: %d seconds'%(timeNow-timeStart) 

# Run the script that makes the deletion request to phedex
timeStart = time.time()
print ' Make deletion request.'
if requestDeletions:
    call(["./requestDeletions.py"])
timeNow = time.time()
print ' - Deletion requests took: %d seconds'%(timeNow-timeStart) 

# Run the script that makes the deletion request to phedex
print ' Show cache release requests.'
call(["./showCacheRequests.py"])

# Final summary of timing
print ' Total Cycle took: %d seconds'%(timeNow-timeInitial) 
