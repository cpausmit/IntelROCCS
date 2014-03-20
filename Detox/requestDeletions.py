#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
# This script looks at the results of deletion suggestions for a specified site and actually asks
# PhEDEx for deletions.  The PhEDEx communications are handled inside PhEDExAPI class.
#
#  The deletion request would have to approved by sysadmins At the moment deletion requests will go
# to T2_US_MIT, T2_US_Nebraska, and T2_DE_RWTH.
#
# ToDo: If needed, we will add a mechanism to maintain a local list of submitted subscriptions so
# that we will NOT ask again if it looks like there is orginal request submitted earlier still
# pending for approval.
#
#----------------------------------------------------------------------------------------------------
import sys, os, re, glob, time, glob, shutil
import datetime
from   datetime import date, timedelta
from phedexApi import phedexApi

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def submitRequest(site, datasets=[]):
    
    if len(datasets) < 1:
        print " ERROR - Trying to submit empty request for " + site
        return

    phedex = phedexApi(logPath='./')

    # compose data for deletion request
    check,data = phedex.xmlData(datasets=datasets,instance='prod')

    if check: 
        print " ERROR - phedexApi.xmlData failed"
        sys.exit(1)

    # here the request is really sent
    check,response = phedex.delete(node=site,data=data,\
                       comments='IntelROOCS -- Automatic Cache Release Request'+\
                       '(if not acted upon will repeat in about 6 hours)',
                       instance='prod')
    if check:
        print " ERROR - phedexApi.delete failed"
        print response
        sys.exit(1)

#====================================================================================================
#  M A I N
#====================================================================================================
if __name__ == '__main__':
    """
__main__

"""

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# directories we work in
statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']

# define three test sites
testSites = [ 'T2_US_MIT', 'T2_US_Nebraska', 'T2_DE_RWTH' ]

deletionFile = "DeleteDatasets.txt"

# hash of sites and corresponding dataset deletion list
siteDeletionList = {}

# look at the results, and for each site get a list to be deleted
allSubDirs = glob.glob(resultDirectory + "/T*")
for member in allSubDirs:
    site = member.split('/')[-1]
    inputFile = resultDirectory + '/' + site + '/' + deletionFile

    print " Looking at file: " + inputFile
    fileHandle = open(inputFile,"r")
    for line in fileHandle.xreadlines():
        items = line.split()

	# CP-CP this is bad decoding prone to failure
        if len(items) != 5 : 
            continue
        print ' Found dataset: ' + dataset
        dataset = items[4]
        if not dataset.startswith('/'):
            continue
        if site in siteDeletionList.keys():
            siteDeletionList[site].append(dataset)
        else:
            siteDeletionList[site] = [dataset]
    fileHandle.close()

# now submit actual requests
for site in siteDeletionList.keys():
    if site not in testSites:
        continue

    print "Deletion request for site " + site
    print siteDeletionList[site]
    submitRequest(site,siteDeletionList[site])
