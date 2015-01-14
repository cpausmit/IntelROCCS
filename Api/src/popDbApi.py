#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Python interface to access Popularity Database. See website for API documentation
# (https://cms-popularity.cern.ch/popdb/popularity/apidoc)
#
# Use SSO cookie to avoid password
# (http://linux.web.cern.ch/linux/docs/cernssocookie.shtml)
# It is up to the caller to make sure a valid SSO cookie is obtained before any calls are made. A
# SSO cookie is valid for 24h. Requires usercert.pem and userkey.pem in ~/.globus/
#
# The API doesn't check to make sure correct values are passed or that rquired parameters are
# passed. All such checks needs to be done by the caller. All data is returned as JSON.
#
# In case of error an error message is printed to the log, currently specified by environemental
# variable INTELROCCS_LOG, and '0' is returned. User will have to check that something is returned.
# If a valid call is made but no data was found a JSON structure is still returned, it is up to
# the caller to check for actual data.
#---------------------------------------------------------------------------------------------------
import sys, os, re, json, urllib, urllib2, datetime, subprocess, ConfigParser
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

class popDbApi():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.cfg')
        self.popDbBase = config.get('PopDB', 'base')
        self.cert = config.get('PopDB', 'certificate')
        self.key = config.get('PopDB', 'key')
        self.cookie = config.get('PopDB', 'sso_cookie')
        self.deadline = config.getint('Phedex', 'expiration_timer')
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(self.deadline))
        modTime = datetime.datetime.fromtimestamp(0)
        if (not os.path.isfile(self.cookie)) or (os.path.getsize(self.cookie) == 0):
            self.renewSsoCookie()
        else:
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(self.cookie))
            if (timeNow-deltaNhours) > modTime:
                self.renewSsoCookie()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def renewSsoCookie(self):
        strout = ""
        for attempt in range(3):
            process = subprocess.Popen(["cern-get-sso-cookie", "--cert", self.cert, "--key", self.key, "-u", self.popDbBase, "-o", self.cookie], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
            strout = process.communicate()[0]
            if process.returncode != 0:
                continue
            else:
                break
        else:
            self.error("Could not generate SSO cookie")
        return 0

    def call(self, url, values):
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        fullUrl = request.get_full_url() + request.get_data()
        strout = ""
        for attempt in range(3):
            process = subprocess.Popen(["curl", "-k", "-s", "-L", "--cookie", self.cookie, "--cookie-jar", self.cookie, fullUrl], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
            strout = process.communicate()[0]
            if process.returncode != 0:
                continue
            else:
                try:
                    jsonData = json.loads(strout)
                except ValueError, e:
                    continue
                break
        else:
            self.error("Pop DB call failed for url: %s" % (str(url)))
        return jsonData

    def error(self, e):
        title = "FATAL IntelROCCS Error -- Pop DB"
        text = "FATAL -- %s" % (str(e),)
        fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
        toList = (["Bjorn Barrefors"], ["barrefors@gmail.com"])
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
        raise Exception("FATAL -- %s" % (str(e),))

    def _toStr(self, toList):
        names = [formataddr(i) for i in zip(*toList)]
        return ', '.join(names)

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def DataTierStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DataTierStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            logFile.write("ERROR -- DataTierStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (tstart, tstop, sitename))
        return jsonData

    def DSNameStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSNameStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR -- DSNameStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tstart, tstop, sitename))
        return jsonData

    def DSStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR -- DSStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (tstart, tstop, sitename))
        return jsonData

    def getCorruptedFiles(self, sitename='summary', orderby=''):
        values = {'sitename':sitename, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getCorruptedFiles"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR -- getCorruptedFiles call failed for values: sitename=%s, orderby=%s\n" % (sitename, orderby))
        return jsonData

    def getDSdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR -- getDSdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDSNdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSNdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR -- getDSNdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDTdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDTdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR: getDTdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getSingleDNstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDNstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR: getSingleDNstat call failed for values: sitename=%s, aggr=%s, orderby=%s\n" % (sitename, aggr, orderby))
        return jsonData

    def getSingleDSstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDSstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR: getSingleDSstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s\n" % (name, sitename, aggr, orderby))
        return jsonData

    def getSingleDTstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDTstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR: getSingleDTstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s\n" % (name, sitename, aggr, orderby))
        return jsonData

    def getUserStat(self, tstart='', tstop='', collname='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'collname':collname, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getUserStat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print("ERROR: getUserStat call failed for values: tstart=%s, tstop=%s, collname=%s, orderby=%s\n" % (tstart, tstop, collname, orderby))
        return jsonData
