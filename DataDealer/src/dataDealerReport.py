#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Daily Report
# Print: Total data owned
#        Percentage used storage
#        Data owned per site
#        Percentage used per site
#        Total data subscribed
#        Data subscribed per site
#---------------------------------------------------------------------------------------------------
import sys, os, datetime, sqlite3, ConfigParser
from operator import itemgetter
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE
import makeTable, sites
import dbApi, phedexData, popDbData

class dataDealerReport():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('intelroccs.cfg')
        self.rankingsCachePath = config.get('DataDealer', 'cache')
        self.reportPath = config.get('DataDealer', 'report_path')
        self.phedexData = phedexData.phedexData()
        self.popDbData = popDbData.popDbData()
        self.dbApi = dbApi.dbApi()
        self.sites = sites.sites()

    def _toStr(self, toList):
        names = [formataddr(i) for i in zip(*toList)]
        return ', '.join(names)

    def sendReport(self, title, text):
        # Send email
        fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
        #toList = (["Bjorn Barrefors"], ["bjorn.peter.barrefors@cern.ch"])
        toList = (["Bjorn Barrefors"], ["barrefors@gmail.com"])
        #toList = (["Data Management Group"], ["hn-cms-dmDevelopment@cern.ch"])
        #toList = (["Bjorn Barrefors", "Brian Bockelman", "Maxim Goncharov", "Christoph Paus"],
        #          ["bjorn.peter.barrefors@cern.ch", "bbockelm@cse.unl.edu", "maxi@mit.edu", "paus@mit.edu"])

        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(fromEmail)
        msg['To'] = self._toStr(toList)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)

    def createReport(self):
        # Initialize
        date = datetime.date.today()
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        rankingsCache = sqlite3.connect(cacheFile)

        # Get all sites with data usage, quota, and rank
        allSites = self.sites.getAllSites()
        blacklistedSites = self.sites.getBlacklistedSites()
        siteQuota = dict()
        for site in allSites:
            query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
            values = [site, "AnalysisOps"]
            data = self.dbApi.dbQuery(query, values=values)
            quota= data[0][0]
            usedStorage = self.phedexData.getSiteStorage(site)
            with rankingsCache:
                cur = rankingsCache.cursor()
                cur.execute('SELECT Rank FROM Sites WHERE SiteName=?', (site,))
                row = cur.fetchone()
                if not row:
                    rank = 0
                else:
                    rank = row[0]
            siteQuota[site] = (quota, usedStorage, rank)

        # Get all subscriptions
        subscriptions = []
        query = "SELECT Datasets.DatasetName, Sites.SiteName, Requests.Rank FROM Requests INNER JOIN Datasets ON Datasets.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Requests.Date>%s AND Requests.RequestType=%s"
        values = [date.strftime('%Y-%m-%d %H:%M:%S'), 0]
        data = self.dbApi.dbQuery(query, values=values)
        for sub in data:
            subscriptions.append([info for info in sub])
        subscriptions.sort(reverse=True, key=itemgetter(2))

        # Get top 10 datasets not subscribed
        cacheFile = "%s/%s.db" % (self.rankingsCachePath, "rankingsCache")
        nSubbed = len(subscriptions)
        topTen = []
        with rankingsCache:
            cur = rankingsCache.cursor()
            cur.execute('SELECT * FROM Datasets ORDER BY Rank DESC LIMIT ? OFFSET ?', (nSubbed+10, nSubbed))
            for row in cur:
                topTen.append(row)

        # Make title variables
        quota = 0.0
        dataOwned = 0.0
        for site, value in siteQuota.items():
            if not (site in blacklistedSites):
                quota += value[0]
                dataOwned += value[1]
        quotaUsed = int(100*(float(dataOwned)/float(quota*10**3)))
        dataSubscribed = 0.0
        siteSubscriptions = dict()
        for site in allSites:
            siteSubscriptions[site] = 0.0
        for subscription in subscriptions:
            subscriptionSize = self.phedexData.getDatasetSize(subscription[0])
            dataSubscribed += subscriptionSize
            site = subscription[1]
            siteSubscriptions[site] += subscriptionSize

        # Create title
        title = 'AnalysisOps %s | %d TB | %d%% | %.2f TB Subscribed' % (date.strftime('%Y-%m-%d'), int(dataOwned/10**3), int(quotaUsed), int(dataSubscribed/10**3))
        text = '%s\n  %s\n%s\n\n' % ('='*68, title, '='*68)

        # Create site table
        # get status of sites
        siteTable = makeTable.Table(add_numbers=False)
        siteTable.setHeaders(['Site', 'Subscribed TB', 'Space Used TB', 'Space Used %', 'Quota TB', 'Rank', 'Status'])
        for site in allSites:
            subscribed = float(siteSubscriptions[site])/(10**3)
            quota = siteQuota[site][0]
            usedTb = int(siteQuota[site][1]/10**3)
            usedP = "%d%%" % (int(100*(float(usedTb)/float(quota))))
            status = "up"
            rank = float(siteQuota[site][2])
            if site in blacklistedSites:
                status = "down"
            siteTable.addRow([site, subscribed, usedTb, usedP, quota, rank, status])

        text += siteTable.plainText()

        # create subscription table
        text += "\n\nNew Subscriptions\n\n"

        subscriptionTable = makeTable.Table(add_numbers=False)
        subscriptionTable.setHeaders(['Rank', 'Size GB', 'Replicas', 'CPU Hours', 'Dataset'])
        for subscription in subscriptions:
            dataset = subscription[0]
            rank = float(subscription[2])
            size = self.phedexData.getDatasetSize(dataset)
            replicas = int(self.phedexData.getNumberReplicas(dataset))
            cpuH = int(self.popDbData.getDatasetCpus(dataset, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')))
            subscriptionTable.addRow([rank, size, replicas, cpuH, dataset])

        text += subscriptionTable.plainText()

        # create top ten datasets not subscribed table
        text += "\n\nTop Ten Datasets Not Subscribed\n\n"

        topTenTable = makeTable.Table(add_numbers=False)
        topTenTable.setHeaders(['Rank', 'Size GB', 'Replicas', 'CPU Hours', 'Dataset'])
        for dataset in topTen:
            datasetName = dataset[0]
            rank = float(dataset[1])
            size = self.phedexData.getDatasetSize(datasetName)
            replicas = int(self.phedexData.getNumberReplicas(datasetName))
            cpuH = int(self.popDbData.getDatasetCpus(datasetName, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')))
            topTenTable.addRow([rank, size, replicas, cpuH, datasetName])

        text += topTenTable.plainText()

        fs = open('%s/data_dealer-%s.report' % (self.reportPath, date.strftime('%Y%m%d')), 'w')
        fs.write(text)
        fs.close()

        text = "http://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/Logs/data_dealer-latest.log\n\nhttp://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/Reports/data_dealer-latest.report"

        self.sendReport(title, text)

if __name__ == '__main__':
    dataDealerReport()
    sys.exit(0)
