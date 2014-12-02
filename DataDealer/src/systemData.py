#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
import sites, dbApi, phedexData, popDbData

fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.tsv', 'w')
fs.write("site\tquota\tused\tcpu\tsubscribed\n")
fs.close()
dbApi_ = dbApi.dbApi()
phedexData_ = phedexData.phedexData()
popDbData_ = popDbData.popDbData()
sites_ = sites.sites()
availableSites = sites_.getAvailableSites()
today = datetime.date.today()
date = today - datetime.timedelta(days=1)
for siteName in availableSites:
    subscriptions = []
    query = "SELECT Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Datasets.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Requests.Date>%s AND Sites.SiteName=%s AND Requests.RequestType=%s"
    values = [date.strftime('%Y-%m-%d %H:%M:%S'), siteName, 0]
    data = dbApi_.dbQuery(query, values=values)
    for sub in data:
        subscriptions.append(sub[0])
    subscribed = 0
    for datasetName in subscriptions:
        subscribed += phedexData_.getDatasetSize(datasetName)
    query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
    values = [siteName, "AnalysisOps"]
    data = dbApi_.dbQuery(query, values=values)
    quota = data[0][0]*10**3
    used = phedexData_.getSiteStorage(siteName)
    cpu = 0
    for i in range(1, 8):
        date = today - datetime.timedelta(days=i)
        cpu += popDbData_.getSiteCpu(siteName, date.strftime('%Y-%m-%d'))
    fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/system.tsv', 'a')
    fs.write("%s\t%s\t%s\t%s\t%s\n" % (siteName, quota, used, cpu, subscribed))
    fs.close()
