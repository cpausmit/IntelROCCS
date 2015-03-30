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
import os, datetime, sqlite3, ConfigParser
from operator import itemgetter
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE
import makeTable, sites
import dbApi, phedexData, popDbData

class report():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
        self.rankingsCachePath = config.get('data_dealer', 'rankings_cache')
        self.crabCachePath = config.get('data_dealer', 'crab_cache')
        self.reportPath = config.get('data_dealer', 'report_path')
        self.fromEmail = config.items('from_email')[0]
        self.toEmails = config.items('report_emails')
        self.phedexData = phedexData.phedexData()
        self.popDbData = popDbData.popDbData()
        self.dbApi = dbApi.dbApi()
        self.sites = sites.sites()

    def _toStr(self, toEmails):
        names = [formataddr(email) for email in toEmails]
        return ', '.join(names)

    def sendReport(self, title, text):
        # Send email
        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(self.fromEmail)
        msg['To'] = self._toStr(self.toEmails)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)

    def createDailyReport(self):
        # Initialize
        today = datetime.date.today()
        date = today
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
            quota = data[0][0]
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

        # Get all deletions
        deletions = []
        query = "SELECT Datasets.DatasetName, Sites.SiteName, Requests.Rank FROM Requests INNER JOIN Datasets ON Datasets.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Requests.Date>%s AND Requests.RequestType=%s"
        values = [date.strftime('%Y-%m-%d %H:%M:%S'), 1]
        data = self.dbApi.dbQuery(query, values=values)
        for sub in data:
            deletions.append([info for info in sub])
        deletions.sort(reverse=True, key=itemgetter(2))

        # get all jobs in crab queue
        cacheFile = "%s/%s.db" % (self.crabCachePath, "crabCache")
        crabCache = sqlite3.connect(cacheFile)
        jobs = []
        crabSites = []
        with crabCache:
            cur = crabCache.cursor()
            cur.execute('SELECT * FROM Jobs')
            for row in cur:
                jobs.append([info for info in row])
            cur.execute('SELECT SiteName FROM Sites')
            for row in cur:
                if row[0] in allSites:
                    crabSites.append(row[0])
        jobs.sort(key=itemgetter(1))

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
        title = 'Daily AnalysisOps Report %s | %d TB | %d%% | %.3f TB Subscribed' % (date.strftime('%Y-%m-%d'), int(dataOwned/10**3), int(quotaUsed), float(dataSubscribed/10**3))
        text = '%s\n  %s\n%s\n\n' % ('='*78, title, '='*78)

        # Create site table
        # get status of sites
        siteTable = makeTable.Table(add_numbers=False)
        siteTable.setHeaders(['Site', 'Subscribed TB', 'Space Used TB', 'Space Used %', 'Quota TB', 'Rank', 'Status', 'CRAB Q'])
        for site in allSites:
            subscribed = float(siteSubscriptions[site])/(10**3)
            quota = siteQuota[site][0]
            usedTb = int(siteQuota[site][1]/10**3)
            usedP = "%d%%" % (int(100*(float(usedTb)/float(quota))))
            status = "up"
            rank = float(siteQuota[site][2])
            crabStatus = "no"
            if site in blacklistedSites:
                status = "down"
            if site in crabSites:
                crabStatus = "yes"
            siteTable.addRow([site, subscribed, usedTb, usedP, quota, rank, status, crabStatus])

        text += siteTable.plainText()

        # create subscription table
        text += "\n\nNew Subscriptions\n\n"

        subscriptionTable = makeTable.Table(add_numbers=False)
        subscriptionTable.setHeaders(['Rank', 'Size GB', 'Replicas', 'Dataset'])
        for subscription in subscriptions:
            dataset = subscription[0]
            rank = float(subscription[2])
            size = int(self.phedexData.getDatasetSize(dataset))
            replicas = int(self.phedexData.getNumberReplicas(dataset))
            subscriptionTable.addRow([rank, size, replicas, dataset])

        text += subscriptionTable.plainText()

        # create deletion table
        text += "\n\nNew Deletions\n\n"

        deletionTable = makeTable.Table(add_numbers=False)
        deletionTable.setHeaders(['Rank', 'Size GB', 'Replicas', 'Dataset'])
        for deletion in deletions:
            dataset = deletion[0]
            rank = float(deletion[2])
            size = int(self.phedexData.getDatasetSize(dataset))
            replicas = int(self.phedexData.getNumberReplicas(dataset))
            deletionTable.addRow([rank, size, replicas, dataset])

        text += deletionTable.plainText()

        # create crab table
        text += "\n\nJobs in CRAB older than 1 day\n\n"

        crabTable = makeTable.Table(add_numbers=False)
        crabTable.setHeaders(['Queue Date', 'Replicas', 'Dataset', 'User', 'Num Jobs', 'Jobs Left'])
        for job in jobs:
            datasetName = job[0]
            timestamp = job[1]
            userName = job[2]
            numJobs = job[3]
            jobsLeft = job[4]
            replicas = int(self.phedexData.getNumberReplicas(datasetName))
            crabTable.addRow([datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'), replicas, datasetName, userName, int(numJobs), int(jobsLeft)])

        text += crabTable.plainText()

        fs = open('%s/data_dealer-%s.report' % (self.reportPath, today.strftime('%Y%m%d')), 'w')
        fs.write(text)
        fs.close()

        text = "Todays Log: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/Logs/data_dealer-%s.log\n\nTodays Report: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/Reports/data_dealer-%s.report\n\nCurrent Subscription Progress: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/DataDealer/Progress/data_dealer-%s.progress" % (today.strftime('%Y%m%d'), today.strftime('%Y%m%d'), today.strftime('%Y%m%d'))

        self.sendReport(title, text)

    def createWeeklyReport(self):
        # Initialize
        today = datetime.date.today()
        date = today
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
            quota = data[0][0]
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

        # Get all deletions
        deletions = []
        query = "SELECT Datasets.DatasetName, Sites.SiteName, Requests.Rank FROM Requests INNER JOIN Datasets ON Datasets.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Requests.Date>%s AND Requests.RequestType=%s"
        values = [date.strftime('%Y-%m-%d %H:%M:%S'), 1]
        data = self.dbApi.dbQuery(query, values=values)
        for sub in data:
            deletions.append([info for info in sub])
        deletions.sort(reverse=True, key=itemgetter(2))

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
        title = 'Weekly AnalysisOps Report %s | %d TB | %d%% | %.3f TB Subscribed' % (date.strftime('%Y-%m-%d'), int(dataOwned/10**3), int(quotaUsed), float(dataSubscribed/10**3))
        text = '%s\n  %s\n%s\n\n' % ('='*78, title, '='*78)

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
            size = int(self.phedexData.getDatasetSize(dataset))
            replicas = int(self.phedexData.getNumberReplicas(dataset))
            cpuH = int(self.popDbData.getDatasetCpus(dataset, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')))
            subscriptionTable.addRow([rank, size, replicas, cpuH, dataset])

        text += subscriptionTable.plainText()

        # create deletion table
        text += "\n\nNew Deletions\n\n"

        deletionTable = makeTable.Table(add_numbers=False)
        deletionTable.setHeaders(['Rank', 'Size GB', 'Replicas', 'CPU Hours', 'Dataset'])
        for deletion in deletions:
            dataset = deletion[0]
            rank = float(deletion[2])
            size = int(self.phedexData.getDatasetSize(dataset))
            replicas = int(self.phedexData.getNumberReplicas(dataset))
            cpuH = int(self.popDbData.getDatasetCpus(dataset, (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')))
            deletionTable.addRow([rank, size, replicas, cpuH, dataset])

        text += deletionTable.plainText()

        fs = open('%s/data_dealer-%s.report' % (self.reportPath, today.strftime('%Y%m%d')), 'a')
        fs.write(text)
        fs.close()
