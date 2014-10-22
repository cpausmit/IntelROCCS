#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Simple script to email log file
#---------------------------------------------------------------------------------------------------
import sys, datetime
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

def _toStr(toList):
    names = [formataddr(i) for i in zip(*toList)]
    return ', '.join(names)

# create email
date = datetime.date.today()

# title
title = 'Data Dealer Log %s' % (date.strftime('%Y-%m-%d'))

# message
f = open('/tmp/IntelROCCS/Logs/%s-data_dealer.log' % (date.strftime('%Y%m%d')), 'r')
text = f.read()
f.close()

# Send email
fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
toList = (["Bjorn Barrefors"], ["bjorn.peter.barrefors@cern.ch"])
#toList = (["Bjorn Barrefors"], ["barrefors@gmail.com"])
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
sys.exit(0)
