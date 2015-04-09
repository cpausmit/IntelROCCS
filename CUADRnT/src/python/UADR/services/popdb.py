#!/usr/local/bin/python
"""
File       : popdb.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Popularity DB service module
"""

# system modules
import json
import urllib
import urllib2
import datetime

# package modules
from UADR.services.generic import GenericService

class PopDBService(GenericService):
    """
    Helper class to access Popularity DB API
    """
    def __init__(self, config=None, verbose=0):
        GenericService.__init__(self, config, verbose)
        self.name = 'popdb'
        self.url = config['services'][self.name]
        self.cert = config.get('pop_db', 'certificate')
        self.key = config.get('pop_db', 'key')
        self.cookie = config.get('pop_db', 'sso_cookie')
        self.renewSsoCookie()

    def renewSsoCookie(self):
        # Will try to generate cookie 3 times before reporting an error
        for attempt in range(3):
            process = subprocess.Popen(["cern-get-sso-cookie", "--cert", self.cert, "--key", self.key, "-u", self.popDbBase, "-o", self.cookie], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
            process.communicate()[0]
            if process.returncode != 0:
                continue
            else:
                break
        else:
            self.error("Could not generate SSO cookie")
        return 0

    def fetch(self, url, values):
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
                except ValueError:
                    continue
                break
        else:
            self.error("Pop DB call failed for url: %s %s" % (str(url), str(values)))
        return jsonData


#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def DataTierStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DataTierStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- DataTierStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (tstart, tstop, sitename))
        return jsonData

    def DSNameStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSNameStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- DSNameStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tstart, tstop, sitename))
        return jsonData

    def DSStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- DSStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s\n" % (tstart, tstop, sitename))
        return jsonData

    def getCorruptedFiles(self, sitename='summary', orderby=''):
        values = {'sitename':sitename, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getCorruptedFiles"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- getCorruptedFiles call failed for values: sitename=%s, orderby=%s\n" % (sitename, orderby))
        return jsonData

    def getDSdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- getDSdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDSNdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSNdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR -- getDSNdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDTdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDTdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR: getDTdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s\n" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getSingleDNstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDNstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR: getSingleDNstat call failed for values: sitename=%s, aggr=%s, orderby=%s\n" % (sitename, aggr, orderby))
        return jsonData

    def getSingleDSstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDSstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR: getSingleDSstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s\n" % (name, sitename, aggr, orderby))
        return jsonData

    def getSingleDTstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDTstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR: getSingleDTstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s\n" % (name, sitename, aggr, orderby))
        return jsonData

    def getUserStat(self, tstart='', tstop='', collname='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'collname':collname, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getUserStat"))
        jsonData = self.call(url, values)
        if not jsonData:
            self.error("ERROR: getUserStat call failed for values: tstart=%s, tstop=%s, collname=%s, orderby=%s\n" % (tstart, tstop, collname, orderby))
        return jsonData
