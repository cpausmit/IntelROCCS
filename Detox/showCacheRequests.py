#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script looks at the results for deletion and prints the history stored in the database in the
# form that makes it easy for sites to track rquests that were made by the system only 20 most
# recent requests wil be shown
#
#---------------------------------------------------------------------------------------------------
import sys, os, re, glob, time, shutil
import MySQLdb

#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == '__main__':
    """
__main__

"""

siteRequests = {}
requestDetails = {}
requestSizes = {}
requestTime = {}

myCnf = os.environ['DETOX_MYSQL_CONFIG']

db = MySQLdb.connect(read_default_file=myCnf,read_default_group="mysql")
cursor = db.cursor()
sql = "select  RequestId,SiteName,Dataset,Size,Rank,GroupName,TimeStamp from Requests order" + \
      " by RequestId DESC LIMIT 10000"

# ! this could be done in one line but it is just nice to see what is deleted !
try:
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        reqid   = row[0]
        site    = row[1]
        dataset = row[2]
        size    = row[3]
        tstamp  = row[6]
        #tstamp  = (str(row[6]).split())[0]
        
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
    print "caught an exception"
    db.rollback()
    
db.close()

resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']

for site in siteRequests.keys():
    outputFile = open(resultDirectory + '/' + site + '/DeletionHistory.txt','w')

    counter = 0
    requests = siteRequests[site]
    for reqid in sorted(requests,reverse=True):
        outputFile.write("PhEDEx Request: %s (%10s, %.1f GB)\n"%\
                         (reqid,requestTime[reqid],requestSizes[reqid]))

        for dataset in requestDetails[reqid]:
            outputFile.write("%s\n" %dataset)
        outputFile.write("\n")
        counter = counter + 1
        if counter > 20:
            break
    outputFile.close()
