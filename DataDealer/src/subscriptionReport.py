#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Daily Report
# Print: Total data owned
#		 Percentage used storage
#		 Data owned per site
#		 Percentage used per site
#		 Total data subscribed
#		 Data subscribed per site
#---------------------------------------------------------------------------------------------------
import sys, os, datetime, calendar
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE
import makeTable, sites, siteRanker
import dbApi, phedexApi, phedexData, popDbData

class subscriptionReport():
	def __init__(self):
		phedexCache = os.environ['PHEDEX_CACHE']
		popDbCache = os.environ['POP_DB_CACHE']
		cacheDeadline = int(os.environ['CACHE_DEADLINE'])
		self.phedexData = phedexData.phedexData(phedexCache, cacheDeadline)
		self.popDbData = popDbData.popDbData(popDbCache, cacheDeadline)
		self.phedexApi = phedexApi.phedexApi()
		self.dbApi = dbApi.dbApi()
		self.sites = sites.sites()
		self.siteRanker = siteRanker.siteRanker()

	def _toStr(self, toList):
	    names = [formataddr(i) for i in zip(*toList)]
	    return ', '.join(names)

	def sendReport(self, title, text):
		# Send email
		fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
		toList = (["Bjorn Barrefors"], ["bjorn.peter.barrefors@cern.ch"])
		#toList = (["Data Management Group"], ["hn-cms-dmDevelopment@cern.ch"])

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
		date = datetime.date.today() - datetime.timedelta(days=20)

		# Get all currently valid sites with data usage and quota
		allSites = self.sites.getAllSites()
		siteQuota = dict()
		for site in availableSites:
			quota = self.siteRanker.getMaxStorage(site)
			usedStorage = self.phedexData.getSiteStorage(site)
			siteQuota[site] = (quota, usedStorage)

		# Get all subscriptions
		subscriptions = []
		query = "SELECT Datasets.DatasetName, Sites.SiteName, Requests.Rank FROM Requests INNER JOIN Datasets ON Datasets.DatasetId=Requests.DatasetId INNER JOIN Sites ON Sites.SiteId=Requests.SiteId WHERE Requests.Date>%s AND Requests.RequestType=%s"
		values = [date.strftime('%Y-%m-%d %H:%M:%S'), 0]
		data = self.dbApi.dbQuery(query, values=values)
		for subscription in data:
			subscriptions.append(subscription)

		# Make title variables
		quota = 0.0
		dataOwned = 0.0
		for value in siteQuota.itervalues():
			quota += value[0]
			dataOwned += value[1]
		quotaUsed = 100*(dataOwned/(quota*10**3))
		dataSubscribed = 0.0
		for subsciption in subscriptions:
			dataSubscribed += self.phedexData.getDatasetSize(subsciption[0])

		# Create title
		title = 'AnalysisOps %s | %d TB | %d%% | %.2f TB Subscribed' % (date.strftime('%Y-%m-%d'), int(dataOwned/10**3), int(quotaUsed), dataSubscribed/10**3)
		text = '%s\n  %s\n%s\n\n' % ('='*68, title, '='*68)

		# Create site table
		siteSubscriptions = dict()
		for site in allSites:
			siteSubscriptions[site] = 0
		for subscription in subscriptions:
			site = subscription[1]
			siteSubscriptions[site] += self.phedexData.getDatasetSize(subsciption[0])

		# get status of sites
		availableSites = self.sites.getAvailableSites()

		siteTable = makeTable.Table(add_numbers=False)
		siteTable.setHeaders(['Site', 'Subscribed TB', 'Space Used TB', 'Space Used %', 'Quota TB', 'Status'])
		for site in allSites:
			subscribed = float(siteSubscriptions[site])/(10**3)
			usedTb = int(siteQuota[site][1]/10**3)
			quota = int(siteQuota[site][0])
			usedP = "%d%%" % (int(100*(float(usedTb)/quota)))
			status = "up"
			if site in availableSites:
				status = "down"
			siteTable.addRow([site, subscribed, usedTb, usedP, quota, status])

		text += siteTable.plainText()

		text += "\n\nNew Subscriptions\n\n"

		subscriptionTable = makeTable.Table(add_numbers=False)
		subscriptionTable.setHeaders(['Site', 'Rank', 'Size TB', 'Replicas', 'Accesses', 'Dataset'])
		for subscription in subscriptions:
			dataset = subscription[0]
			site = subscription[1]
			rank = float(subscription[2])
			size = float(self.phedexData.getDatasetSize(dataset))/(10**3)
			replicas = int(self.phedexData.getNumberReplicas(dataset))
			accesses = int(self.popDbData.getDatasetAccesses(dataset, (datetime.date.today() - datetime.timedelta(days=1))))
			subscriptionTable.addRow([site, rank, size, replicas, accesses, dataset])

		text += subscriptionTable.plainText()

		self.sendReport(title, text)

if __name__ == '__main__':
	subscriptionReport()
	sys.exit(0)
