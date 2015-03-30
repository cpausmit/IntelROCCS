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
# In case of error an error message is printed to the log.
# If a valid call is made but no data was found a JSON structure is still returned, it is up to
# the caller to check for actual data.
#---------------------------------------------------------------------------------------------------
import os, urllib, urllib2, httplib, json, ConfigParser
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

class phedexApi:
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'api.cfg'))
        self.fromEmail = config.items('from_email')[0]
        self.toEmails = config.items('error_emails')
        self.phedexBase = config.get('phedex', 'base')

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def call(self, url, values):
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        strout = ""
        e = ""
        msg = ""
        # Will try to call 3 times before reporting an error
        for attempt in range(3):
            try:
                strout = opener.open(request)
                response = strout.read()
                jsonData = json.loads(response)
            except urllib2.HTTPError, err:
                e = err
                msg = e.read()
                continue
            except urllib2.URLError, err:
                e = err
                msg = e.read()
                continue
            except ValueError, err:
                e = err
                continue
            else:
                break
        else:
            self.error(e, msg)
        return jsonData

    def error(self, e, errMsg):
        title = "FATAL IntelROCCS Error -- PhEDEx"
        text = "FATAL -- %s --- MSG: %s" % (str(e), str(errMsg))
        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(self.fromEmail)
        msg['To'] = self._toStr(self.toEmails)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)
        print "FATAL -- %s --- MSG: %s" % (str(e), str(errMsg))
        raise Exception("FATAL -- %s --- MSG: %s" % (str(e), str(errMsg)))

    def _toStr(self, toList):
        names = [formataddr(i) for i in zip(*toList)]
        return ', '.join(names)

    def createXml(self, datasets=[], instance='prod'):
        xml = '<data version="2.0">'
        xml = xml + '<dbs name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">' % (instance)
        successDatasets = []
        for dataset in datasets:
            for attempt in range(3):
                jsonData = self.data(dataset=dataset, level='block')
                if not jsonData:
                    continue
                try:
                    data = jsonData.get('phedex').get('dbs')[0].get('dataset')[0]
                except IndexError:
                    continue
                else:
                    break
            else:
                print("ERROR -- Couldn't create xml data for dataset %s\n" % (dataset))
                continue
            successDatasets.append(dataset)
            xml = xml + '<dataset name="%s" is-open="%s">' % (data.get('name'), data.get('is_open'))
            xml = xml + "</dataset>"
        xml = xml + "</dbs>"
        xmlData = xml + "</data>"
        return successDatasets, xmlData

#===================================================================================================
#  A P I   C A L L S
#===================================================================================================
    def blockArrive(self, id_='', block='', dataset='', to_node='', priority='', update_since='', basis='', arrive_before='', arrive_after='', instance='prod'):
        values = {'id':id_, 'block':block, 'dataset':dataset, 'to_node':to_node, 'priority':priority, 'update_since':update_since, 'basis':basis, 'arrive_before':arrive_before, 'arrive_after':arrive_after}
        url = "%s/json/%s/blockarrive" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- blockArrive call failed", "values: id=%s, block=%s, dataset=%s, to_node=%s, priority=%s, update_since=%s, basis=%s, arrive_before=%s, arrive_after=%s, instance=%s\n" % (id_, block, dataset, to_node, priority, update_since, basis, arrive_before, arrive_after, instance))
        return jsonData

    def blockReplicas(self, block='', dataset='', node='', se='', update_since='', create_since='', complete='', dist_complete='', subscribed='', custodial='', group='', show_dataset='', instance='prod'):
        values = {'block':block, 'dataset':dataset, 'node':node, 'se':se, 'update_since':update_since, 'create_since':create_since, 'complete':complete, 'dist_complete':dist_complete, 'subscribed':subscribed, 'custodial':custodial, 'group':group, 'show_dataset':show_dataset}
        url = "%s/json/%s/blockreplicas" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- blockReplicas call failed", "values: block=%s, dataset=%s, node=%s, se=%s, update_since=%s, create_since=%s, complete=%s, dist_complete=%s, subscribed=%s, custodial=%s, group=%s, show_dataset=%s, instance=%s\n" % (block, dataset, node, se, update_since, create_since, complete, dist_complete, subscribed, custodial, group, show_dataset, instance))
        return jsonData

    def data(self, dataset='', block='', file_='', level='', create_since='', instance='prod'):
        values = {'dataset':dataset, 'block':block, 'file':file_, 'level':level, 'create_since':create_since}
        url = "%s/json/%s/data" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- data call failed", "values: dataset=%s, block=%s, file=%s, level=%s, create_since=%s, instance=%s\n" % (dataset, block, file_, level, create_since, instance))
        return jsonData

    def delete(self, node='', data='', level='', rm_subscriptions='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'rm_subscriptions':rm_subscriptions, 'comments':comments}
        url = "%s/json/%s/delete" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- delete call failed", "values: node=%s, level=%s, rm_subscriptions=%s, comments=%s, instance=%s\n" % (node, level, rm_subscriptions, comments, instance))
        return jsonData

    def deleteRequests(self, request='', node='', create_since='', limit='', approval='', requested_by='', instance='prod'):
        values = {'request':request, 'node':node, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = "%s/json/%s/deleterequests" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- deleteRequests call failed", "values: request=%s, node=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s\n" % (request, node, create_since, limit, approval, requested_by, instance))
        return jsonData

    def deletions(self, node='', se='', block='', dataset='', id_='', request='', request_since='', complete='', complete_since='', instance='prod'):
        values = {'node':node, 'se':se, 'block':block, 'dataset':dataset, 'id':id_, 'request':request, 'request_since':request_since, 'complete':complete, 'complete_since':complete_since}
        url = "%s/json/%s/deletions" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- deletions call failed for values: node=%s, se=%s, block=%s, dataset=%s, id=%s, request=%s, request_since=%s, complete=%s, complete_since=%s, instance=%s\n" % (node, se, block, dataset, id_, request, request_since, complete, complete_since, instance))
        return jsonData

    def requestList(self, request='', type_='', approval='', requested_by='', node='', decision='', group='', create_since='', create_until='', decide_since='', decide_until='', dataset='', block='', decided_by='', instance='prod'):
        values = {'request':request, 'type':type_, 'approval':approval, 'requested_by':requested_by, 'node':node, 'decision':decision, 'group':group, 'create_since':create_since, 'create_until':create_until, 'decide_since':decide_since, 'decide_until':decide_until, 'dataset':dataset, 'block':block, 'decided_by':decided_by}
        url = "%s/json/%s/requestlist" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- requestList call failed", "values: request=%s, type=%s, approval=%s, requested_by=%s, node=%s, decision=%s, group=%s, create_since=%s, create_until=%s, decide_since=%s, decide_until=%s, dataset=%s, block=%s, decided_by=%s, instance=%s\n" % (request, type_, approval, requested_by, node, decision, group, create_since, create_until, decide_since, decide_until, dataset, block, decided_by, instance))
        return jsonData

    def subscribe(self, node='', data='', level='', priority='', move='', static='', custodial='', group='', time_start='', request_only='', no_mail='', comments='', instance='prod'):
        values = {'node':node, 'data':data, 'level':level, 'priority':priority, 'move':move, 'static':static, 'custodial':custodial, 'group':group, 'time_start':time_start, 'request_only':request_only, 'no_mail':no_mail, 'comments':comments}
        url = "%s/json/%s/subscribe" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- subscribe call failed", "values: node=%s, data=%s, level=%s, priority=%s, move=%s, static=%s, custodial=%s, group=%s, time_start=%s, request_only=%s, no_mail=%s, comments=%s, instance=%s\n" % (node, data, level, priority, move, static, custodial, group, time_start, request_only, no_mail, comments, instance))
        return jsonData

    def subscriptions(self, dataset='', block='', node='', se='', create_since='', request='', custodial='', group='', priority='', move='', suspended='', collapse='', percent_min='', percent_max='', instance='prod'):
        values = {'dataset':dataset, 'block':block, 'node':node, 'se':se, 'create_since':create_since, 'request':request, 'custodial':custodial, 'group':group, 'priority':priority, 'move':move, 'suspended':suspended, 'collapse':collapse, 'percent_min':percent_min, 'percent_max':percent_max}
        url = "%s/json/%s/subscriptions" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- subscriptions call failed", "values: dataset=%s, block=%s, node=%s, se=%s, create_since=%s, request=%s, custodial=%s, group=%s, priority=%s, move=%s, suspended=%s, collapse=%s, percent_min=%s, percent_max=%s, instance=%s\n" % (dataset, block, node, se, create_since, request, custodial, group, priority, move, suspended, collapse, percent_min, percent_max, instance))
        return jsonData

    def transferRequests(self, request='', node='', group='', create_since='', limit='', approval='', requested_by='', instance='prod'):
        values = {'request':request, 'node':node, 'group':group, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
        url = "%s/json/%s/transferrequests" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- transferRequests call failed", "values: request=%s, node=%s, group=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s\n" % (request, node, group, create_since, limit, approval, requested_by, instance))
        return jsonData

    def updateRequest(self, decision='', request='', node='', comments='', instance='prod'):
        values = {'decision':decision, 'request':request, 'node':node, 'comments':comments}
        url = "%s/json/%s/updaterequests" % (self.phedexBase, instance)
        try:
            jsonData = self.call(url, values)
        except Exception:
            self.error("ERROR -- updateRequest call failed", "values: decision=%s, request=%s, node=%s, comments=%s, instance=%s\n" % (decision, request, node, comments, instance))
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
