#====================================================================================================
#  C L A S S E S  concerning the site status
#====================================================================================================
import os, sys, MySQLdb

def getAllSites():
    # we are looking for all relevant sites
    sites = {}
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

    group = "AnalysisOps"
    sql = "select * from " + table + ' where GroupName=\'' + group +'\''
    # go ahead and try
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            siteName = row[0]
            willBeUsed = row[4]
            sizeTb = row[2]
            siteSizeGb = sizeTb * 1024
            sites[siteName] = SiteStatus(siteName)
            sites[siteName].setStatus(willBeUsed)
            sites[siteName].setSize(siteSizeGb)
    except:
        print ' Error(%s) -- could not retrieve sites'%(sql)
        print sys.exc_info()
        sys.exit(1)

    return sites

#---------------------------------------------------------------------------------------------------
"""
Class:  SiteStatus(siteName='')
Stores flags for each site.
"""
#---------------------------------------------------------------------------------------------------
class SiteStatus:
    "A SiteStatus defines the ststua of the site."

    def __init__(self, siteName):
        self.name = siteName
        # do we want to use this site and is retrieved from Quotas table (SiteStorage)?
        self.status = 0
        # will be set to 0 if information for site is corrupted, do not issue delete requests
        self.valid = 1
        self.size = 0

    def setStatus(self,status):
        self.status = status
    def setValid(self,valid):
        self.valid = valid
    def setSize(self,size):
        self.size = size

    def getStatus(self):
        return self.status
    def getValid(self):
        return self.valid
    def getSize(self):
        return self.size
