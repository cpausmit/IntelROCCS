#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
import sites, dbApi, phedexData, popDbData

fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.tsv', 'w')
fs.write("site\tquota\tused\tcpu\n")
fs.close()
dbApi_ = dbApi.dbApi()
phedexData_ = phedexData.phedexData()
popDbData_ = popDbData.popDbData()
sites_ = sites.sites()
availableSites = sites_.getAvailableSites()
today = datetime.date.today()
date = today - datetime.timedelta(days=1)
for siteName in availableSites:
    query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
    values = [siteName, "AnalysisOps"]
    data = dbApi_.dbQuery(query, values=values)
    quota = data[0][0]*10**3
    used = phedexData_.getSiteStorage(siteName)
    cpu = popDbData_.getSiteCpu(siteName, date.strftime('%Y-%m-%d'))
    fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.tsv', 'a')
    fs.write("%s\t%s\t%s\t%s\n" % (siteName, quota, used, cpu))
    fs.close()
