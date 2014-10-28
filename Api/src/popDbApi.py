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
import sys, os, re, json, urllib, urllib2, datetime, subprocess
import initPopDb

class popDbApi():
    def __init__(self, userCert, userKey, ssoCookie):
        self.popDbBase = os.environ['POP_DB_BASE']
        self.cert = userCert
        self.key = userKey
        self.cookie = ssoCookie

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def renewSsoCookie(self):
        process = subprocess.Popen(["cern-get-sso-cookie", "--cert", self.cert, "--key", self.key, "-u", self.popDbBase, "-o", self.cookie], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        strout = process.communicate()[0]
        if process.returncode != 0:
            raise Exception(" FATAL -- could not generate SSO cookie\nError msg: %s" % (str(strout)))
        return 0

    def call(self, url, values):
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        fullUrl = request.get_full_url() + request.get_data()
        process = subprocess.Popen(["curl", "-k", "-s", "-L", "--cookie", self.cookie, "--cookie-jar", self.cookie, fullUrl], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        strout = process.communicate()[0]
        if process.returncode != 0:
            print(" ERROR -- Access to popularity database failed\nError msg: %s" % (str(strout)))
            return 0
        try:
            jsonData = json.loads(strout)
        except ValueError, e:
            print(" ERROR -- no JSON data returned from popularity database failed\nError msg: %s" % (str(strout)))
            return 0
        return jsonData

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def DataTierStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DataTierStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            logFile.write(" ERROR -- DataTierStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s" % (tstart, tstop, sitename))
        return jsonData

    def DSNameStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSNameStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- DSNameStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tstart, tstop, sitename))
        return jsonData

    def DSStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("DSStatInTimeWindow"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- DSStatInTimeWindow call failed for values: tstart=%s, tstop=%s, sitename=%s" % (tstart, tstop, sitename))
        return jsonData

    def getCorruptedFiles(self, sitename='summary', orderby=''):
        values = {'sitename':sitename, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getCorruptedFiles"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- getCorruptedFiles call failed for values: sitename=%s, orderby=%s" % (sitename, orderby))
        return jsonData

    def getDSdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- getDSdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDSNdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDSNdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- getDSNdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getDTdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getDTdata"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR: getDTdata call failed for values: tstart=%s, tstop=%s, sitename=%s, aggr=%s, n=%s, orderby=%s" % (tstart, tstop, sitename, aggr, n, orderby))
        return jsonData

    def getSingleDNstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDNstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR: getSingleDNstat call failed for values: sitename=%s, aggr=%s, orderby=%s" % (sitename, aggr, orderby))
        return jsonData

    def getSingleDSstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDSstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR: getSingleDSstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s" % (name, sitename, aggr, orderby))
        return jsonData

    def getSingleDTstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getSingleDTstat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR: getSingleDTstat call failed for values: name=%s, sitename=%s, aggr=%s, orderby=%s" % (name, sitename, aggr, orderby))
        return jsonData

    def getUserStat(self, tstart='', tstop='', collname='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'collname':collname, 'orderby':orderby}
        url = urllib.basejoin(self.popDbBase, "%s/?&" % ("getUserStat"))
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR: getUserStat call failed for values: tstart=%s, tstop=%s, collname=%s, orderby=%s" % (tstart, tstop, collname, orderby))
        return jsonData

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./popDbApi.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]
if __name__ == '__main__':
    popDbApi_ = popDbApi()
    error = popDbApi_.renewSsoCookie()
    if error:
        print "Failed to renew SSO cookie"
        sys.exit(1)
    print "Renewed SSO cookie"
    if len(sys.argv) < 2:
        print "Usage: python ./popDbApi.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(2)
    func = getattr(popDbApi_, sys.argv[1], None)
    if not func:
        print "%s is not a valid popularity db api call" % (sys.argv[1])
        print "Usage: python ./popDbApi.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    args = dict()
    for arg in sys.argv[2:]:
        try:
            a, v = arg.split('=')
        except ValueError, e:
            print "Passed argument %s does not follow the correct usage" % (arg)
            print "Usage: python ./popDbApi.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
            sys.exit(2)
        args[a] = v
    jsonData = 0
    try:
        jsonData = func(**args)
    except TypeError, e:
        print e
        print "Usage: python ./popDbApi.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    if not jsonData:
        print " ERROR -- PopDB call failed"
        sys.exit(1)
    print jsonData
    sys.exit(0)
