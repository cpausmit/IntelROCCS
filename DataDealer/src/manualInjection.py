#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Collects all the necessary data to generate rankings for all datasets in the AnalysisOps space.
#---------------------------------------------------------------------------------------------------
import os, sys, ConfigParser
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

def manualInjection():
    config = ConfigParser.RawConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'manual_injection.cfg'))
    values = config.get('datasets', 'datasets')
    datasets = list(filter(None, (value.strip() for value in values.splitlines())))
    requests = [420102, 424015]
    sendEmail(datasets, requests)

def _toStr(toEmails):
    names = [formataddr(email) for email in toEmails]
    return ', '.join(names)

def sendEmail(datasets, requests):
    config = ConfigParser.RawConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'manual_injection.cfg'))
    fromEmail = config.items('from_email')[0]
    toEmails = config.items('to_emails')
    title = "Your datasets have been injected into AnalysisOps space"
    text = "As requested, we have injected the following datasets into AnalysisOps\n\n"
    for dataset in datasets:
        text += "%s\n" % (dataset)
    text += "\nYou can track the transfers at the following subscription pages\n\n"
    for request in requests:
        text += "https://cmsweb.cern.ch/phedex/prod/Request::View?request=%d&.submit=Submit\n" % (request)
    text += "\nFor any questions feel free to contact Bjorn Barrefors at bjorn.peter.barrefors@cern.ch\n\n"
    text += "Bjorn Barrefors - Dynamic Data Management"
    msg = MIMEMultipart()
    msg['Subject'] = title
    msg['From'] = formataddr(fromEmail)
    msg['To'] = _toStr(toEmails)
    msg1 = MIMEMultipart("alternative")
    msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
    msgText2 = MIMEText(text)
    msg1.attach(msgText2)
    msg1.attach(msgText1)
    msg.attach(msg1)
    msg = msg.as_string()
    p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
    p.communicate(msg)

#===================================================================================================
#  H E L P E R S
#===================================================================================================


#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == "__main__":
    manualInjection()
    sys.exit(0)
