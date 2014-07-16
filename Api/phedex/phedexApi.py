#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Python interface to access PhEDEx online API. See website for API documentation
# (https://cmsweb.cern.ch/phedex/datasvc/doc)
#
# Use grid-proxy-init to aquire a valid CERN proxy, proxy's are only valid for a limited time. grid-proxy-init requires usercert.pem and userkey.key in ~/.globus/
# It is up to the caller to make sure a valid CERN proxy is available.
#
# The API doesn't check to make sure correct values are passed or that rquired parameters are 
# passed. All such checks needs to be done by the caller.
#
# Functions only return data in form of JSON, never XML.
# Instance of phedex can be selected using the instance parameter [prod/dev], default is prod.
#
# In case of error an exception is thrown. This needs to be dealt with by the caller.
#---------------------------------------------------------------------------------------------------
import sys, os, urllib, urllib2, httplib, json

class phedexApi:
    def __init__(self):
        self.PHEDEX_BASE = "https://cmsweb.cern.ch/phedex/datasvc/"

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def call(self, url, values):
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        fullUrl = request.get_full_url() + request.get_data()
        strout = ""
        try:
            strout = opener.open(request)
        except urllib2.HTTPError, e:
            raise Exception("FATAL - phedex failure: %s\n- for url: %s" % (str(e), str(fullUrl)))
        except urllib2.URLError, e:
            raise Exception("FATAL - phedex failure: %s\n - for url: %s" % (str(e), str(fullUrl)))
        try:
            response = strout.read()
            jsonData = json.loads(response)
        except ValueError, e:
            raise Exception("FATAL - phedex failure: %s\n - for url: %s" % (str(strout), str(fullUrl)))
        return jsonData

    # the parser (parse and xmlData) is used to create an xml data structure which can be passed to 
    # phedex calls subscribe and delete. currently it's based on source diving investigation of how
    # phedex structures this data object, no documentation exists.
    # TODO : Write example structure for better understanding
    def parse(self, data, xml):
        for k, v in data.iteritems():
            k = k.replace("_", "-")
            if type(v) is list:
                xml = "%s>" % (xml,)
                for v1 in v:
                    xml = "%s<%s" % (xml, k)
                    xml = self.parse(v1, xml)
                    if (k == "file"):
                        xml = "%s/>" % (xml,)
                    else:
                        xml = "%s</%s>" % (xml, k)
            else:
                if k == "lfn":
                    k = "name"
                elif k == "size":
                    k = "bytes"
                if (k == "name" or k == "is-open" or k == "is-transient" or k == "bytes" or k== "checksum"):
                    xml = '%s %s="%s"' % (xml, k, v)
        return xml

    def xmlData(self, datasets=[], instance='prod'):
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">' % (xml, 'dbs', instance)
        for dataset in datasets:
            jsonData = self.data(dataset=dataset, level='file', create_since='0', instance=instance)
            data = jsonData.get('phedex').get('dbs')
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xmlData = "%s</data>" % (xml,)
        return xmlData

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def data(self, dataset='', block='', file_='', level='', create_since='', instance='prod'):
        values = {'dataset':dataset, 'block':block, 'file':file_, 'level':level, 'create_since':create_since}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/data" % (instance))
        return self.call(url, values)

    def subscribe(self, node='', data='', level='', priority='', move='', static='', custodial='', group='', time_start='', request_only='', no_mail='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'priority':priority, 'move':move, 'static':static, 'custodial':custodial, 'group':group, 'time_start':time_start, 'request_only':request_only, 'no_mail':no_mail, 'comments':comments}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/subscribe" % (instance))
        return self.call(url, values)

    def delete(self, node='', data='', level='', rm_subscriptions='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'rm_subscriptions':rm_subscriptions, 'comments':comments}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/delete" % (instance))
        return self.call(url, values)

    def updateRequest(self, decision='', request='', node='', comments='', instance='prod'):
        values = {'decision':decision, 'request':request, 'node':node, 'comments':comments}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/updaterequest" % (instance))
        return self.call(url, values)

    def blockReplicas(self, block='', dataset='', node='', se='', update_since='', create_since='', complete='', dist_complete='', subscribed='', custodial='', group='', show_dataset='', instance='prod'):
        values = {'block':block, 'dataset':dataset, 'node':node, 'se':se, 'update_since':update_since, 'create_since':create_since, 'complete':complete, 'dist_complete':dist_complete, 'subscribed':subscribed, 'custodial':custodial, 'group':group, 'show_dataset':show_dataset}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/blockreplicas" % (instance))
        return self.call(url, values)

    def deletions(self, node='', se='', block='', dataset='', id_='', request='', request_since='', complete='', complete_since='', instance='prod'):
        values = {'node':node, 'se':se, 'block':block, 'dataset':dataset, 'id':id_, 'request':request, 'request_since':request_since, 'complete':complete, 'complete_since':complete_since}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/deletions" % (instance))
        return self.call(url, values)

    def deleteRequests(self, request='', node='', create_since='', limit='', approval='', requested_by=''):
        values = {'request':request, 'node':node, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/deleterequests" % (instance))
        return self.call(url, values)

    def transferRequests(self, request='', node='', group='', create_since='', limit='', approval='', requested_by=''):
        values = {'request':request, 'node':node, 'group':group, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/transferrequests" % (instance))
        return self.call(url, values)

    def requestList(self, request='', type_='', approval='', requested_by='', node='', decision='', group='', create_since='', create_until='', decide_since='', decide_until='', dataset='', block='', decided_by='', instance='prod'):
        values = {'request':request, 'type':type_, 'approval':approval, 'requested_by':requested_by, 'node':node, 'decision':decision, 'group':group, 'create_since':create_since, 'create_until':create_until, 'decide_since':decide_since, 'decide_until':decide_until, 'dataset':dataset, 'block':block, 'decided_by':decided_by}
        url = urllib.basejoin(self.PHEDEX_BASE, "json/%s/requestlist" % (instance))
        return self.call(url, values)

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
        proxy = os.environ.get("X509_USER_PROXY")
        if not proxy:
            proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./phedex.py <apiCall> <instance> ['arg1_name:arg1' 'arg2_name:arg2' ...]
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./phedex.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(2)
    phdx = phedex()
    func = getattr(phdx, sys.argv[1], None)
    if not func:
        print "%s is not a valid phedex api call" % (sys.argv[1])
        print "Usage: python ./phedex.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    args = dict()
    for arg in sys.argv[2:]:
        try:
            a, v = arg.split('=')
        except ValueError, e:
            print "Passed argument %s does not follow the correct usage" % (arg)
            print "Usage: python ./phedex.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
            sys.exit(2)
        args[a] = v
    try:
        data = func(**args)
    except TypeError, e:
        print e
        print "Usage: python ./phedex.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
        sys.exit(3)
    print data
    sys.exit(0)
