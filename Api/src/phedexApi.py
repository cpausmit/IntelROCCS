#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Python interface to access PhEDEx online API. See website for API documentation
# (https://cmsweb.cern.ch/phedex/datasvc/doc)
#
# Use grid-proxy-init to aquire a valid CERN proxy, proxy's are only valid for a limited time.
# grid-proxy-init requires usercert.pem and userkey.pem in ~/.globus/
# It is up to the caller to make sure a valid CERN proxy is available.
# Example: grid-proxy-init -valid 24:00 (Generates a proxy valid for 24h)
#
# The API doesn't check to make sure correct values are passed or that rquired parameters are
# passed. All such checks needs to be done by the caller.
#
# Functions only return data in form of JSON, never XML.
# Instance of phedex can be selected using the instance parameter [prod/dev], default is prod.
#
# In case of error an error message is printed to the log, currently specified by environemental
# variable INTELROCCS_LOG, and '0' is returned. User will have to check that something is returned.
# If a valid call is made but no data was found a JSON structure is still returned, it is up to
# the caller to check for actual data.
#---------------------------------------------------------------------------------------------------
import sys, re, os, urllib, urllib2, httplib, json, datetime, subprocess
import initPhedex

class phedexApi:
    def __init__(self, userCert, userKey):
        self.phedexBase = os.environ['PHEDEX_BASE']
        self.cert = userCert
        self.key = userKey

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def renewProxy(self):
        process = subprocess.Popen(["grid-proxy-init", "-valid", "24:00", "-cert", self.cert, "-key", self.key], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        strout = process.communicate()[0]
        if process.returncode != 0:
            # email
            raise Exception(" FATAL -- Could not generate proxy\nError msg: %s" % (str(strout)))
        return 0

    def call(self, url, values):
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        strout = ""
        try:
            strout = opener.open(request)
        except urllib2.HTTPError, e:
            errorMsg = str(e)
            phedexMsg = e.read()
            print(" FATAL -- %s\PHEDEX msg: %s" % (errorMsg, phedexMsg))
            return 0
        except urllib2.URLError, e:
            errorMsg = str(e)
            print(" FATAL -- %s\nMost likely due to invalid proxy or error in phedex base: %s" % (errorMsg, self.phedexBase))
            return 0
        response = strout.read()
        try:
            jsonData = json.loads(response)
        except ValueError, e:
            print(" FATAL -- No JSON data returned\nMost likely due to error in phedex url base: %s" % (self.phedexBase))
            return 0
        return jsonData

    # Information about the correct xml structure can be found here: https://cmsweb.cern.ch/phedex/datasvc/doc/inject
    def createXml(self, datasets=[], instance='prod'):
        xml = '<data version="2.0">'
        xml = xml + '<dbs name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader" dls="dbs">' % (instance)
        successDatasets = []
        for dataset in datasets:
            jsonData = self.data(dataset=dataset, level='file')
            if not jsonData:
                print(" FATAL -- Couldn't create xml data for dataset %s" % (dataset))
                # email
                continue
            try:
                data = jsonData.get('phedex').get('dbs')[0].get('dataset')[0]
            except IndexError, e:
                print(" FATAL -- Couldn't create xml data for dataset %s" % (dataset))
                # email
                continue
            successDatasets.append(dataset)
            xml = xml + '<dataset name="%s" is-open="%s">' % (data.get('name'), data.get('is_open'))
            blocks = data.get('block')
            for block in blocks:
                xml = xml + '<block name="%s" is-open="%s">' % (block.get('name'), block.get('is_open'))
                files = block.get('file')
                for file_ in files:
                    xml = xml + '<file name="%s" bytes="%s" checksum="%s"/>' % (file_.get('lfn'), file_.get('size'), file_.get('checksum'))
                xml = xml + "</block>"
            xml = xml + "</dataset>"
        xml = xml + "</dbs>"
        xmlData = xml + "</data>"
        return successDatasets, xmlData

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def blockReplicas(self, block='', dataset='', node='', se='', update_since='', create_since='', complete='', dist_complete='', subscribed='', custodial='', group='', show_dataset='', instance='prod'):
        values = {'block':block, 'dataset':dataset, 'node':node, 'se':se, 'update_since':update_since, 'create_since':create_since, 'complete':complete, 'dist_complete':dist_complete, 'subscribed':subscribed, 'custodial':custodial, 'group':group, 'show_dataset':show_dataset}
        url = "%s/json/%s/blockreplicas" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- blockReplicas call failed for values: block=%s, dataset=%s, node=%s, se=%s, update_since=%s, create_since=%s, complete=%s, dist_complete=%s, subscribed=%s, custodial=%s, group=%s, show_dataset=%s, instance=%s" % (block, dataset, node, se, update_since, create_since, complete, dist_complete, subscribed, custodial, group, show_dataset, instance))
        return jsonData

    def data(self, dataset='', block='', file_='', level='', create_since='', instance='prod'):
        values = {'dataset':dataset, 'block':block, 'file':file_, 'level':level, 'create_since':create_since}
        url = "%s/json/%s/data" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- data call failed for values: dataset=%s, block=%s, file=%s, level=%s, create_since=%s, instance=%s" % (dataset, block, file_, level, create_since, instance))
        return jsonData

    def delete(self, node='', data='', level='', rm_subscriptions='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'rm_subscriptions':rm_subscriptions, 'comments':comments}
        url = "%s/json/%s/delete" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            logFile.write(" ERROR -- delete call failed for values: node=%s, level=%s, rm_subscriptions=%s, comments=%s, instance=%s" % (node, level, rm_subscriptions, comments, instance))
        return jsonData

    def deleteRequests(self, request='', node='', create_since='', limit='', approval='', requested_by='', instance='prod'):
        values = {'request':request, 'node':node, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = "%s/json/%s/deleterequests" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- deleteRequests call failed for values: request=%s, node=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s" % (request, node, create_since, limit, approval, requested_by, instance))
        return jsonData

    def deletions(self, node='', se='', block='', dataset='', id_='', request='', request_since='', complete='', complete_since='', instance='prod'):
        values = {'node':node, 'se':se, 'block':block, 'dataset':dataset, 'id':id_, 'request':request, 'request_since':request_since, 'complete':complete, 'complete_since':complete_since}
        url = "%s/json/%s/deletions" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- deletions call failed for values: node=%s, se=%s, block=%s, dataset=%s, id=%s, request=%s, request_since=%s, complete=%s, complete_since=%s, instance=%s" % (node, se, block, dataset, id_, request, request_since, complete, complete_since, instance))
        return jsonData

    def requestList(self, request='', type_='', approval='', requested_by='', node='', decision='', group='', create_since='', create_until='', decide_since='', decide_until='', dataset='', block='', decided_by='', instance='prod'):
        values = {'request':request, 'type':type_, 'approval':approval, 'requested_by':requested_by, 'node':node, 'decision':decision, 'group':group, 'create_since':create_since, 'create_until':create_until, 'decide_since':decide_since, 'decide_until':decide_until, 'dataset':dataset, 'block':block, 'decided_by':decided_by}
        url = "%s/json/%s/requestlist" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- requestList call failed for values: request=%s, type=%s, approval=%s, requested_by=%s, node=%s, decision=%s, group=%s, create_since=%s, create_until=%s, decide_since=%s, decide_until=%s, dataset=%s, block=%s, decided_by=%s, instance=%s" % (request, type_, approval, requested_by, node, decision, group, create_since, create_until, decide_since, decide_until, dataset, block, decided_by, instance))
        return jsonData

    def subscribe(self, node='', data='', level='', priority='', move='', static='', custodial='', group='', time_start='', request_only='', no_mail='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'priority':priority, 'move':move, 'static':static, 'custodial':custodial, 'group':group, 'time_start':time_start, 'request_only':request_only, 'no_mail':no_mail, 'comments':comments}
        url = "%s/json/%s/subscribe" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- subscribe call failed for values: node=%s, level=%s, priority=%s, move=%s, static=%s, custodial=%s, group=%s, time_start=%s, request_only=%s, no_mail=%s, comments=%s, instance=%s" % (node, level, priority, move, static, custodial, group, time_start, request_only, no_mail, comments, instance))
        return jsonData

    def transferRequests(self, request='', node='', group='', create_since='', limit='', approval='', requested_by='', instance='prod'):
        values = {'request':request, 'node':node, 'group':group, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = "%s/json/%s/transferrequests" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- transferRequests call failed for values: request=%s, node=%s, group=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s" % (request, node, group, create_since, limit, approval, requested_by, instance))
        return jsonData

    def updateRequest(self, decision='', request='', node='', comments='', instance='prod'):
        values = {'decision':decision, 'request':request, 'node':node, 'comments':comments}
        url = "%s/json/%s/updaterequests" % (self.phedexBase, instance)
        jsonData = self.call(url, values)
        if not jsonData:
            print(" ERROR -- updateRequest call failed for values: decision=%s, request=%s, node=%s, comments=%s, instance=%s" % (decision, request, node, comments, instance))
        return jsonData

#===================================================================================================
#  H E L P E R   C L A S S
#===================================================================================================
class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#===================================================================================================
#  S C R I P T
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./phedexApi.py <apiCall> <instance> ['arg1_name=arg1' 'arg2_name=arg2' ...]
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(2)
    phedexApi_ = phedexApi()
    error = phedexApi_.renewProxy()
    if error:
        print "Failed to renew proxy"
        sys.exit(1)
    print "Renewed proxy"
    func = getattr(phedexApi_, sys.argv[1], None)
    if not func:
        print "%s is not a valid phedex api call" % (sys.argv[1])
        print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    args = dict()
    for arg in sys.argv[2:]:
        try:
            a, v = arg.split('=')
        except ValueError, e:
            print "Passed argument %s does not follow the correct usage" % (arg)
            print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
            sys.exit(2)
        args[a] = v
    jsonData = 0
    try:
        jsonData = func(**args)
    except TypeError, e:
        print e
        print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(1)
    if not jsonData:
        print " ERROR -- Phedex call failed."
        sys.exit(1)
    stored = 0
    nDatasets = 0
    datasets = jsonData.get('phedex').get('dataset')
    for dataset in datasets:
        nDatasets += 1
        sizeGb = 0
        sizeByte = 0
        datasetName = dataset.get('name')
        print datasetName
        if re.match('.+/USER', datasetName):
            continue
        for block in dataset.get('block'):
            size = int(block.get('bytes'))
            for replica in dataset.get('block')[0].get('replica'):
                siteName = replica.get('node')
                groupName = replica.get('group')
                if siteName == 'T2_BE_IIHE' and groupName == 'AnalysisOps':
                    sizeByte += size
        sizeGb = float(sizeByte)/10**9
        stored += sizeGb
    print "%.2f" % (stored/10**3)
    sys.exit(0)
