#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Python interface to access Popularity Database. See website for API documentation
# (https://cms-popularity.cern.ch/popdb/popularity/apidoc)
#
# Use SSO cookie to avoid password 
# (http://linux.web.cern.ch/linux/docs/cernssocookie.shtml)
# It is up to the caller to make sure a valid SSO cookie is obtained before any calls are made. A
# SSO cookie is valid for 24h. Requires myCert.key and myCert.pem stored in BASEDIR/certs/
#
# The API doesn't check to make sure correct values are passed or that rquired parameters are 
# passed. All such checks needs to be done by the caller.
#
# In case of error an exception is thrown. This needs to be dealt with by the caller.
#---------------------------------------------------------------------------------------------------
import sys, os, re, urllib, urllib2, subprocess
import json

class popDbApi():
    def __init__(self):
        self.BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
        self.POPDB_BASE = "https://cms-popularity.cern.ch/popdb/popularity/"
        self.CERT = "%s/.globus/usercert.pem" % (os.environ['HOME'])
        self.KEY = "%s/.globus/userkey.pem" % (os.environ['HOME'])
        self.COOKIE = "%s/.globus/ssocookie.txt" % (os.environ['HOME'])

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def renewSSOCookie(self):
        # TODO : what happens if this fails? deal with it
        subprocess.call(["cern-get-sso-cookie", "--cert", self.CERT, "--key", self.KEY, "-u", self.POPDB_BASE, "-o", self.COOKIE])

    def call(self, url, values):
        data = urllib.urlencode(values)
        request = urllib2.Request(url, data)
        full_url = request.get_full_url() + request.get_data()
        process = subprocess.Popen(["curl", "-k", "-s", "-L", "--cookie", self.COOKIE, "--cookie-jar", self.COOKIE, full_url], stdout=subprocess.PIPE)
        strout, error = process.communicate()
        if process.returncode != 0:
            raise Exception("FATAL - popularity failure, exit status %s\n URL - %s" % (str(process.returncode), str(full_url)))
        try:
            json_data = json.loads(strout)
        except ValueError, e:
            raise Exception("FATAL - popularity failure, reason: %s\n URL - %s" % (str(strout), str(full_url)))
        return json_data

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def getDSdata(self, tstart='', tstop='', sitename='summary', aggr='', n='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getDSdata"))
        return self.call(url, values)

    def getDTdata(self, tstart='', tstop='', sitename='summary', aggr='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getDTdata"))
        return self.call(url, values)

    def getDSNdata(self, tstart='', tstop='', sitename='summary', aggr='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename, 'aggr':aggr, 'n':n, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getDSNdata"))
        return self.call(url, values)

    def getSingleDSstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getSingleDSstat"))
        return self.call(url, values)

    def getSingleDTstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getSingleDTstat"))
        return self.call(url, values)

    def getSingleDNstat(self, name='', sitename='summary', aggr='', orderby=''):
        values = {'name':name, 'sitename':sitename, 'aggr':aggr, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getSingleDNstat"))
        return self.call(url, values)

    def DSStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("DSStatInTimeWindow"))
        return self.call(url, values)

    def DataTierStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("DataTierStatInTimeWindow"))
        return self.call(url, values)

    def DSNameStatInTimeWindow(self, tstart='', tstop='', sitename='summary'):
        values = {'tstart':tstart, 'tstop':tstop, 'sitename':sitename}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("DSNameStatInTimeWindow"))
        return self.call(url, values)

    def getUserStat(self, tstart='', tstop='', collname='', orderby=''):
        values = {'tstart':tstart, 'tstop':tstop, 'collname':collname, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getUserStat"))
        return self.call(url, values)

    def getCorruptedFiles(self, sitename='summary', orderby=''):
        values = {'sitename':sitename, 'orderby':orderby}
        url = urllib.basejoin(self.POPDB_BASE, "%s/?&" % ("getCorruptedFiles"))
        return self.call(url, values)

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./popDB.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]
if __name__ == '__main__':
    popdb = popDB()
    popdb.renewSSOCookie()
    print "Updated SSO cookie"
    if len(sys.argv) < 2:
        print "Usage: python ./popDB.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(2)
    func = getattr(popdb, sys.argv[1], None)
    if not func:
        print "%s is not a valid popularity db api call" % (sys.argv[1])
        print "Usage: python ./popDB.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    args = dict()
    for arg in sys.argv[2:]:
        try:
            a, v = arg.split('=')
        except ValueError, e:
            print "Passed argument %s does not follow the correct usage" % (arg)
            print "Usage: python ./popDB.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
            sys.exit(2)
        args[a] = v
    try:
        data = func(**args)
    except TypeError, e:
        print e
        print "Usage: python ./popDB.py <APICall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    print data
    sys.exit(0)
