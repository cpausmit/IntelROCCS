#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Queries database to get all sites on which AnalysisOps have quota. Can also return only those that
# are currently not blacklisted or those that are currently blacklisted.
#
#---------------------------------------------------------------------------------------------------
import sys, os
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import IntelROCCS.Api.db.dbApi as dbApi

class getSites():
    def __init__(self):
        self.dbApi = dbApi.dbApi()

    def getAllSites(self):
        # Change query when tables are updated
        #query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId=Groups.GroupId WHERE Groups.GroupName=%s"
        query = "SELECT SiteName FROM Quotas WHERE GroupName=%s"
        values = ["analysisOps"]
        data = self.dbaccess.dbQuery(query, values=values)
        return [site[0] for site in data]

    def getBlacklistedSites(self):
        # Change query when tables are updated
        #query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId=Groups.GroupId WHERE Groups.GroupName=%s AND Quotas.Status=%s"
        query = "SELECT SiteName FROM Quotas WHERE GroupName=%s AND Status=%s"
        values = ['AnalysisOps', '0']
        data = self.dbaccess.dbQuery(query, values=values)
        return [site[0] for site in data]

    def getAvailableSites(self):
        # Change query when tables are updated
        #query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Quotas.GroupId=Groups.GroupId WHERE Groups.GroupName=%s AND Quotas.Status=%s"
        query = "SELECT SiteName FROM Quotas WHERE GroupName=%s AND Status=%s"
        values = ['AnalysisOps', '1']
        data = self.dbaccess.dbQuery(query, values=values)
        return [site[0] for site in data]

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./getSites.py <function>
if __name__ == '__main__':
    if not (len(sys.argv) == 2):
        print "Usage: python ./getSites.py <function>"
        sys.exit(2)
    site_req = getSites()
    func = getattr(site_req, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./getSites.py <function>"
        sys.exit(3)
    data = func()
    print len(data)
    print data
    sys.exit(0)
