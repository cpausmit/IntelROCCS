#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import os, datetime, ConfigParser
import sites, dbApi, phedexData, popDbData

class systemVis():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
        self.visualizationPath = config.get('data_dealer', 'visualization_path')
        self.sites = sites.sites()
        self.dbApi = dbApi.dbApi()
        self.phedexData = phedexData.phedexData()
        self.popDbData = popDbData.popDbData()

    def collectData(self):
        utcNow = datetime.datetime.utcnow()
        today = datetime.date(utcNow.year, utcNow.month, utcNow.day)
        fs = open('%s/%s.csv' % (self.visualizationPath, 'system'), 'w')
        fs.write("site,quota,used,accesses,subscribed\n")
        fs.close()
        availableSites = self.sites.getAvailableSites()
        for siteName in availableSites:
            query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
            values = [siteName, "AnalysisOps"]
            data = self.dbApi.dbQuery(query, values=values)
            quotaGb = data[0][0]*10**3
            usedGb = self.phedexData.getSiteStorage(siteName)
            accesses = 0
            for i in range(1, 8):
                date = (today - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                accesses += self.popDbData.getSiteAccesses(date, siteName)
            subscribedGb = 0
            query = "SELECT DatasetProperties.Size, Requests.Progress FROM Requests INNER JOIN DatasetProperties ON DatasetProperties.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Sites.SiteName=%s AND Requests.RequestType=%s AND Requests.Progress < %s"
            values = [siteName, 0, 100]
            data = self.dbApi.dbQuery(query, values=values)
            for row in data:
                subscribedGb += int(row[0])*(1 - float(row[1])/10**2)
            fs = open('%s/%s.csv' % (self.visualizationPath, 'system'), 'a')
            fs.write("%s,%d,%d,%d,%d\n" % (siteName, quotaGb, usedGb, accesses, subscribedGb))
            fs.close()
