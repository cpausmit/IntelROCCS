#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import datetime
import sites, dbApi

fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.csv', 'w')
fs.write("site,quota,in_transit\n")
fs.close()
dbApi_ = dbApi.dbApi()
sites_ = sites.sites()
allSites = sites_.getAllSites()
blacklistedSites = sites_.getBlacklistedSites()
today = datetime.date.today()
today = datetime.datetime.combine(today, datetime.time(0,0,0,0))
availableSites = [site for site in allSites if site not in blacklistedSites]
for siteName in availableSites:
    query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
    values = [siteName, "AnalysisOps"]
    data = dbApi_.dbQuery(query, values=values)
    quota = data[0][0]*10**3
    inTransit = 0
    query = "SELECT DatasetProperties.SizeTb, Requests.Progress FROM Requests INNER JOIN DatasetProperties ON DatasetProperties.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Sites.SiteName=%s AND Requests.RequestType=%s AND Requests.Progress < %s"
    values = [siteName, 0, 1]
    data = dbApi_.dbQuery(query, values=values)
    for row in data:
        inTransit += float(row[0])*(1 - float(row[1])/10**2)
    fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.csv', 'a')
    fs.write("%s,%s,%s,%s,%s\n" % (siteName, str(quota), str(inTransit)))
    fs.close()
