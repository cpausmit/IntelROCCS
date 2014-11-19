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
import sys, os, re
import phedexApi

# setup definitions
if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# make sure we start in the right directory
os.chdir(os.environ.get('DETOX_BASE'))

deprecated   = {}
siteDsets    = {}
siteSize2Del = {}

siteLCPerc = {}
#===================================================================================================
#  H E L P E R S
#===================================================================================================
def readDeprecated():
    filename = 'DeprecatedSets.txt'
    inputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                     + filename, 'r')
    for line in inputFile.xreadlines():
        name,status = line.split()
        deprecated[name] = 1
    inputFile.close()    

    if '/METFwd/Run2010B-Nov4ReReco_v1/RECO' in deprecated.keys():
        print "found it"

def readMatchPhedex():
    filename = os.environ['DETOX_PHEDEX_CACHE']
    inputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                     + filename, "r")

    for line in inputFile.xreadlines():
        line = line.rstrip()
        items = line.split()
        datasetName = items[0]
        group = items[1]

        if datasetName not in deprecated:
            continue
        if group != 'AnalysisOps':
            continue

        size = float(items[3])
        site = items[7]

        if site not in siteSize2Del:
            siteSize2Del[site] = 0
        siteSize2Del[site] = siteSize2Del[site] + size
        
        if site not in siteDsets:
            siteDsets[site] = [datasetName]
        else:
            siteDsets[site].append(datasetName)
        
    inputFile.close()

def requestDeletions(site,dsetNames):
    phedex = phedexApi.phedexApi(logPath='./')
    # compose data for deletion request
    check,data = phedex.xmlData(datasets=dsetNames,instance='prod')
    if check:
        print " ERROR - phedexApi.xmlData failed"
        sys.exit(1)

    # here the request is really sent
    message = 'IntelROCCS -- Cache Release Request -- Deprecated Datasets'
    check,response = phedex.delete(node=site,data=data,comments=message,instance='prod')
    if check:
        print " ERROR - phedexApi.delete failed"
        print response
        sys.exit(1)
    respo = response.read()
    matchObj = re.search(r'"id":"(\d+)"',respo)
    reqid = int(matchObj.group(1))
    del phedex

     # here we brute force deletion to be approved
    phedex = phedexApi.phedexApi(logPath='./')
    check,response = phedex.updateRequest(decision='approve',request=reqid,node=site,instance='prod')
    if check:
        print " ERROR - phedexApi.updateRequest failed - reqid="+ str(reqid)
        print response
    del phedex

#====================================================================================================
#  M A I N
#====================================================================================================

# find deprecated datasets
readDeprecated()

# match deprecated datasets against phedex data
readMatchPhedex()

total = 0
for site in sorted(siteSize2Del):
    print ("%-7.1f %-18s" %(siteSize2Del[site],site))
    total = total + siteSize2Del[site]
print ("Total size to be deleted = %8.1f GB" %total)


#submit deletion requests for deprecated datasets
print "\n You want do continue with those deleteions? [Y/N]"
line  = sys.stdin.readline()
line = line.rstrip()
if line == 'Y':
    for site in sorted(siteDsets):
        #if not site == 'T2_US_MIT': continue
        setsToDelete = siteDsets[site]
        print site
        print siteSize2Del[site]
        for dset in setsToDelete:
            print dset
        #print setsToDelete
        requestDeletions(site,setsToDelete)
