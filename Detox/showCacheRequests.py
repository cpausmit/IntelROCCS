#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script looks at the results for deletions at the given site and prints the history stored in
# the database for the last 20 (pick your favorite number here) deletion requests in the form that
# makes it easy for the site to see why the datasets were deleted
#
#---------------------------------------------------------------------------------------------------
import sys, os, re, glob, time, shutil, MySQLdb

if len(sys.argv) < 2:
    print '\n not enough arguments\n\n'               # please add a more useful output with example
    sys.exit()
else:
    site = str(sys.argv[1])

#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == '__main__':
    """
__main__

"""
print " - show requests for site: %s"%(site)
    
siteRequests = {}
requestDetails = {}
requestSizes = {}
requestTime = {}
dranks = {}

myCnf = os.environ['DETOX_MYSQL_CONFIG']

db = MySQLdb.connect(read_default_file=myCnf,read_default_group="mysql")
cursor = db.cursor()
sql = "select  RequestId,SiteName,Dataset,Size,Rank,GroupName,TimeStamp from Requests " + \
      " where SiteName='" + site + "' order by RequestId DESC LIMIT 1000"

try:
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        reqid   = row[0]
        site    = row[1]
        dataset = row[2]
        size    = row[3]
        rank    = row[4]
        tstamp  = row[6]

        dranks[dataset] = rank 
        
        if site in siteRequests.keys():
            if reqid not in siteRequests[site]:
                (siteRequests[site]).append(reqid)
        else:
            siteRequests[site] = [reqid]

        if reqid in requestDetails.keys():
            if dataset not in requestDetails[reqid]:
                (requestDetails[reqid]).append(dataset)
                requestSizes[reqid] = requestSizes[reqid] + size
        else:
            requestDetails[reqid] = [dataset]
            requestSizes[reqid] = size
            requestTime[reqid] = tstamp
except:
    print " -- FAILED extract mysql info: %s"%(sql)
    db.rollback()
    
db.close()

resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']

# the site is fixed
#for site in siteRequests.keys():

outputFile = open(resultDirectory + '/' + site + '/DeletionHistory.txt','w')
counter = 0

outputFile.write("# ======================================================================\n")
outputFile.write("# This is the list of the 20 last phedex deletion requests at this site.\n")
outputFile.write("#  ---- note: presently those requests are not necessarily approved.    \n")
outputFile.write("# ======================================================================\n")

if site in siteRequests.keys():
    requests = siteRequests[site]
    for reqid in sorted(requests,reverse=True):
        outputFile.write("#\n# PhEDEx Request: %s (%10s, %.1f GB)\n"%\
                         (reqid,requestTime[reqid],requestSizes[reqid]))
    
        outputFile.write("#\n# Rank   DatasetName\n")
        outputFile.write("# ---------------------------------------\n")
        for dataset in requestDetails[reqid]:
            outputFile.write("  %-6d %s\n" %(dranks[dataset],dataset))
        outputFile.write("\n")
        counter = counter + 1
        if counter > 20:
            break
else:
    outputFile.write("# no requests found")

outputFile.close()
