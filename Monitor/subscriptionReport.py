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
import sys, os, sqlite3, datetime, calendar
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.Utils import formataddr
from email.quopriMIME import encode
from subprocess import Popen, PIPE
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import makeTable
import IntelROCCS.DataDealer.sites as sites
import IntelROCCS.DataDealer.siteRanker as siteRanker
import IntelROCCS.DataDealer.phedexDb as phedexDb

def subscriptionReport():
	# Initialize
	phedexDb_ = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
	sites_ = sites.sites()
	siteRanker_ = siteRanker.siteRanker()
	requestsDbPath = "%s/Cache" % (os.environ['INTELROCCS_BASE'])
	requestsDbFile = "%s/requests.db" % (requestsDbPath)
	requestsDbCon = sqlite3.connect(requestsDbFile)
	date = datetime.date.today()

	# Get all currently valid sites with data usage and quota
	availableSites = sites_.getAvailableSites()
	siteQuota = dict()
	for site in availableSites:
		quota = siteRanker_.getMaxStorage(site)
		used = phedexDb_.getSiteStorage(site)
		siteQuota[site] = (quota, used)

	# Get all subscriptions
	subscriptions = []
	with requestsDbCon:
		cur = requestsDbCon.cursor()
		cur.execute('SELECT * FROM Requests WHERE Timestamp>?', (calendar.timegm(date.timetuple()),))
		for row in cur:
			subscriptions.append(row)

	# Make title variables
	quota = 0.0
	dataOwned = 0.0
	for value in siteQuota.itervalues():
		quota += value[0]
		dataOwned += value[1]
	quotaUsed = 100*(dataOwned/(quota*10**3))
	dataSubscribed = 0.0
	for subsciption in subscriptions:
		dataSubscribed += subsciption[4]

	# Create title
	title = 'AnalysisOps %s | %d TB | %d%% | %.2f TB Subscribed' % (date.strftime('%Y-%m-%d'), int(dataOwned/10**3), int(quotaUsed), dataSubscribed/10**3)
	text = '%s\n  %s\n%s\n\n' % ('='*68, title, '='*68)

	# Create site table
	siteSubscriptions = dict()
	for site in availableSites:
		siteSubscriptions[site] = 0
	for subscription in subscriptions:
		site = subscription[3]
		siteSubscriptions[site] += subscription[4]

	siteTable = makeTable.Table(add_numbers=False)
	siteTable.setHeaders(['Site', 'Subscribed TB', 'Used TB', 'Used %', 'Quota TB'])
	for site in availableSites:
		subscribed = float(siteSubscriptions[site])/(10**3)
		usedTb = int(siteQuota[site][1]/10**3)
		quota = int(siteQuota[site][0])
		usedP = "%d%%" % (int(100*(float(usedTb)/quota)))
		siteTable.addRow([site, subscribed, usedTb, usedP, quota])

	text += siteTable.plainText()

	text += "\n\nNew Subscriptions\n\n"

	subscriptionTable = makeTable.Table(add_numbers=False)
	subscriptionTable.setHeaders(['Site', 'Rank', 'Size TB', 'Replicas', 'Accesses', 'Dataset'])
	for subscription in subscriptions:
		site = subscription[3]
		rank = float(subscription[7])
		size = float(subscription[4])/(10**3)
		replicas = int(subscription[5])
		accesses = int(subscription[6])
		dataset = subscription[2]
		subscriptionTable.addRow([site, rank, size, replicas, accesses, dataset])

	text += subscriptionTable.plainText()

	def _toStr(toList):
	    names = [formataddr(i) for i in zip(*toList)]
	    return ', '.join(names)

	# Send email
	fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
	toList = (["Data Management Group",
			   "Bjorn Barrefors",
			   "Brian Bockelman",
			   "Maxim Goncharov",
			   "Christoph Paus",
			   "Tony Wildish",
			   "Nicolo Magini",
			   "Thomas Kress",
			   "Frank Wuerthwein"],
			  ["hn-cms-dmDevelopment@cern.ch",
			   "bjorn.peter.barrefors@cern.ch",
			   "bbockelm@cse.unl.edu",
			   "maxi@mit.edu",
			   "paus@mit.edu",
			   "tony.wildish@cern.ch",
			   "nicolo.magini@cern.ch",
			   "thomas.kress@cern.ch",
			   "fkw@fnal.gov"])
	#toList = (["Bjorn Barrefors"], ["bbarrefo@cse.unl.edu"])
	#toList = (["Data Management Group"], ["hn-cms-dmDevelopment@cern.ch"])

	msg = MIMEMultipart()
	msg['Subject'] = title
	msg['From'] = formataddr(fromEmail)
	msg['To'] = _toStr(toList)
	msg1 = MIMEMultipart("alternative")
	msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
	msgText2 = MIMEText(text)
	msg1.attach(msgText2)
	msg1.attach(msgText1)
	msg.attach(msg1)
	msg = msg.as_string()
	p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
	p.communicate(msg)

if __name__ == '__main__':
	subscriptionReport()
	sys.exit(0)
