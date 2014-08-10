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
	phedexDb = phedexDb.phedexDb("%s/Cache/PhedexCache" % (os.environ['INTELROCCS_BASE']), 12)
	sites = sites.sites()
	siteRanker = siteRanker.siteRanker()
	requestsDbPath = "%s/Cache" % (os.environ['INTELROCCS_BASE'])
	requestsDbFile = "%s/requests.db" % (requestsDbPath)
	requestsDbCon = sqlite3.connect(requestsDbFile)
	date = datetime.date.today()

	# Get all currently valid sites with data usage and quota
	availableSites = sites.getAvailableSites()
	siteQuota = dict()
	for site in availableSites:
		quota = siteRanker.getMaxStorage(site)
		used = phedexDb.getSiteStorage(site)
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
	title = 'AnalysisOps %s | %.2f PB | %d%% | %.2f TB Subscribed' % (date.strftime('%Y-%m-%d'), dataOwned/10**3, int(quotaUsed), dataSubscribed/10**3)
	text = '%s\n  %s\n%s\n\n' % ('='*68, title, '='*68)

	# Create site table
	siteSubscriptions = dict()
	for site in availableSites:
		siteSubscriptions[site] = 0
	for subscription in subscriptions:
		site = subscription[3]
		siteSubscriptions[site] += subscription[4]

	siteTable = makeTable.Table(add_numbers=False)
	siteTable.setHeaders(['Site', 'Subscribed GB', 'Used TB', 'Used %', 'Quota TB'])
	for site in availableSites:
		subscribed = int(siteSubscriptions[site])
		usedTb = int(siteQuota[site][1]/10**3)
		quota = int(siteQuota[site][0])
		usedP = "%d%%" % (int(100*(float(usedTb)/quota)))
		siteTable.addRow([site, subscribed, usedTb, usedP, quota])

	text += siteTable.plainText()

	text += "\n\nNew Subscriptions\n\n"

	subscriptionTable = makeTable.Table(add_numbers=False)
	subscriptionTable.setHeaders(['Site', 'Rank', 'Size GB', 'Replicas', 'Accesses', 'Dataset'])
	for subscription in subscriptions:
		site = subscription[3]
		rank = float(subscription[7])
		size = int(subscription[4])
		replicas = int(subscription[5])
		accesses = int(subscription[6])
		dataset = subscription[2]
		subscriptionTable.addRow([site, rank, size, replicas, accesses, dataset])

	text += subscriptionTable.plainText()

	def _toStr(toList):
	    names = [formataddr(i) for i in zip(*toList)]
	    return ', '.join(names)

	# Send email
	fromEmail = ("Bjorn Barrefors", "barrefors@cern.ch")
	toList = (["Bjorn Barrefors", "Brian Bockelman", "Maxim Goncharov", "Christoph Paus"], ["bbarrefo@cse.unl.edu", "bbockelm@cse.unl.edu", "maxi@mit.edu", "paus@mit.edu"])
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
	# server = smtplib.SMTP(smtpServerHost)
	# server.sendmail(fromEmail[1], toList[1], msg)
	# server.quit()

if __name__ == '__main__':
	subscriptionReport()
	sys.exit(0)