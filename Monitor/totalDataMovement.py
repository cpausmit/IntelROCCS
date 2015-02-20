#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script accesses the Requests, Datasets and DatasetsProperties tables and calculates the total
# data movements including data deletions and data replication.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def getDbCursor():
    # configuration
    db = os.environ.get('DETOX_SITESTORAGE_DB')
    server = os.environ.get('DETOX_SITESTORAGE_SERVER')
    user = os.environ.get('DETOX_SITESTORAGE_USER')
    pw = os.environ.get('DETOX_SITESTORAGE_PW')
    # open database connection
    db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    return db.cursor()


#===================================================================================================
#  M A I N
#===================================================================================================
# get access to the database
cursor = getDbCursor()
sql = "select DatasetProperties.Size, Requests.RequestType, Datasets.DatasetName from " + \
      "Requests, Datasets, DatasetProperties where " + \
      "Datasets.DatasetId=Requests.DatasetId and Datasets.DatasetId=DatasetProperties.DatasetId;"

print "\nAccess IntelROCCS database. This might take a while.\n"

try:
    # Execute the SQL command
    #print '        ' + sql
    cursor.execute(sql)
    results = cursor.fetchall()
    # print results
    delSize = 0
    repSize = 0
    for row in results:
	size = float(row[0])
	type = int(row[1])
	name = row[2]
	if type == 0:
	    repSize += size
	elif type == 1:
	    delSize += size
except:
    sys.stderr.write("Error (%s): unable to insert record into table.\n"%(sql))

print "\n ==== SUMMARY DATA DELETION/REPLICATION\n"
print " Deleted    [TB]: %.2f"%(delSize/1000.)
print " Replicated [TB]: %.2f\n"%(repSize/1000.)
	    
sys.exit(0)
