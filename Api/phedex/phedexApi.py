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
import sys, os, urllib, urllib2, httplib, json, datetime, subprocess

class phedexApi:
	def __init__(self):
		self.logFile = os.environ['INTELROCCS_LOG']
		self.phedexBase = "https://cmsweb.cern.ch/phedex/datasvc/"

#===================================================================================================
#  H E L P E R S
#===================================================================================================
	def renewProxy(self):
		process = subprocess.Popen(["grid-proxy-init", "-valid", "24:00"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
		strout = process.communicate()[0]
		if process.returncode != 0:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: Cannot generate proxy\nError msg: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(strout)))
			return 1
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
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: %s\nPhEDEx msg: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), errorMsg, phedexMsg))
			return 0
		except urllib2.URLError, e:
			errorMsg = str(e)
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx URL ERROR: %s\nMost likely due to invalid proxy or error in phedex base: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), errorMsg, self.phedexBase))
			return 0
		response = strout.read()
		try:
			jsonData = json.loads(response)
		except ValueError, e:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: No JSON data returned\nMost likely due to error in phedex url base: %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.phedexBase))
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
				with open(self.logFile, 'a') as logFile:
					logFile.write("%s PhEDEx ERROR: Couldn't create xml data for dataset %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), dataset))
				continue
			try:
				data = jsonData.get('phedex').get('dbs')[0].get('dataset')[0]
			except IndexError, e:
				with open(self.logFile, 'a') as logFile:
					logFile.write("%s PhEDEx ERROR: Couldn't create xml data for dataset %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), dataset))
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
		url = urllib.basejoin(self.phedexBase, "json/%s/blockreplicas" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: blockReplicas call failed for values: block=%s, dataset=%s, node=%s, se=%s, update_since=%s, create_since=%s, complete=%s, dist_complete=%s, subscribed=%s, custodial=%s, group=%s, show_dataset=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), block, dataset, node, se, update_since, create_since, complete, dist_complete, subscribed, custodial, group, show_dataset, instance))
		return jsonData

	def data(self, dataset='', block='', file_='', level='', create_since='', instance='prod'):
		values = {'dataset':dataset, 'block':block, 'file':file_, 'level':level, 'create_since':create_since}
		url = urllib.basejoin(self.phedexBase, "json/%s/data" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: data call failed for values: dataset=%s, block=%s, file=%s, level=%s, create_since=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), dataset, block, file_, level, create_since, instance))
		return jsonData

	def delete(self, node='', data='', level='', rm_subscriptions='', comments='', instance='prod'):
		values = {'node':node, 'data':data, 'level':level, 'rm_subscriptions':rm_subscriptions, 'comments':comments}
		url = urllib.basejoin(self.phedexBase, "json/%s/delete" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: delete call failed for values: node=%s, level=%s, rm_subscriptions=%s, comments=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), node, level, rm_subscriptions, comments, instance))
		return jsonData

	def deleteRequests(self, request='', node='', create_since='', limit='', approval='', requested_by='', instance='prod'):
		values = {'request':request, 'node':node, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
		url = urllib.basejoin(self.phedexBase, "json/%s/deleterequests" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: deleteRequests call failed for values: request=%s, node=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), request, node, create_since, limit, approval, requested_by, instance))
		return jsonData

	def deletions(self, node='', se='', block='', dataset='', id_='', request='', request_since='', complete='', complete_since='', instance='prod'):
		values = {'node':node, 'se':se, 'block':block, 'dataset':dataset, 'id':id_, 'request':request, 'request_since':request_since, 'complete':complete, 'complete_since':complete_since}
		url = urllib.basejoin(self.phedexBase, "json/%s/deletions" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: deletions call failed for values: node=%s, se=%s, block=%s, dataset=%s, id=%s, request=%s, request_since=%s, complete=%s, complete_since=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), node, se, block, dataset, id_, request, request_since, complete, complete_since, instance))
		return jsonData

	def requestList(self, request='', type_='', approval='', requested_by='', node='', decision='', group='', create_since='', create_until='', decide_since='', decide_until='', dataset='', block='', decided_by='', instance='prod'):
		values = {'request':request, 'type':type_, 'approval':approval, 'requested_by':requested_by, 'node':node, 'decision':decision, 'group':group, 'create_since':create_since, 'create_until':create_until, 'decide_since':decide_since, 'decide_until':decide_until, 'dataset':dataset, 'block':block, 'decided_by':decided_by}
		url = urllib.basejoin(self.phedexBase, "json/%s/requestlist" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: requestList call failed for values: request=%s, type=%s, approval=%s, requested_by=%s, node=%s, decision=%s, group=%s, create_since=%s, create_until=%s, decide_since=%s, decide_until=%s, dataset=%s, block=%s, decided_by=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), request, type_, approval, requested_by, node, decision, group, create_since, create_until, decide_since, decide_until, dataset, block, decided_by, instance))
		return jsonData

	def subscribe(self, node='', data='', level='', priority='', move='', static='', custodial='', group='', time_start='', request_only='', no_mail='', comments='', instance='prod'):
		values = {'node':node, 'data':data, 'level':level, 'priority':priority, 'move':move, 'static':static, 'custodial':custodial, 'group':group, 'time_start':time_start, 'request_only':request_only, 'no_mail':no_mail, 'comments':comments}
		url = urllib.basejoin(self.phedexBase, "json/%s/subscribe" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: subscribe call failed for values: node=%s, level=%s, priority=%s, move=%s, static=%s, custodial=%s, group=%s, time_start=%s, request_only=%s, no_mail=%s, comments=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), node, level, priority, move, static, custodial, group, time_start, request_only, no_mail, comments, instance))
		return jsonData

	def transferRequests(self, request='', node='', group='', create_since='', limit='', approval='', requested_by='', instance='prod'):
		values = {'request':request, 'node':node, 'group':group, 'create_since':create_since, 'limit':limit, 'approval':approval, 'requested_by':requested_by}
		url = urllib.basejoin(self.phedexBase, "json/%s/transferrequests" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: transferRequests call failed for values: request=%s, node=%s, group=%s, create_since=%s, limit=%s, approval=%s, requested_by=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), request, node, group, create_since, limit, approval, requested_by, instance))
		return jsonData

	def updateRequest(self, decision='', request='', node='', comments='', instance='prod'):
		values = {'decision':decision, 'request':request, 'node':node, 'comments':comments}
		url = urllib.basejoin(self.phedexBase, "json/%s/updaterequest" % (instance))
		jsonData = self.call(url, values)
		if not jsonData:
			with open(self.logFile, 'a') as logFile:
				logFile.write("%s PhEDEx ERROR: updateRequest call failed for values: decision=%s, request=%s, node=%s, comments=%s, instance=%s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), decision, request, node, comments, instance))
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
		proxy = os.environ.get("X509_USER_PROXY")
		if not proxy:
			proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
		return proxy

	def getConnection(self, host, timeout=300):
		return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#===================================================================================================
#  S C R I P T
#===================================================================================================
# Use this for testing purposes or as a script.
# Usage: python ./phedexApi.py <apiCall> <instance> ['arg1_name:arg1' 'arg2_name:arg2' ...]
if __name__ == '__main__':
	if len(sys.argv) < 2:
		print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
		sys.exit(2)
	phedexApi = phedexApi()
	error = phedexApi.renewProxy()
	if error:
		print "Failed to renew proxy, see log (%s) for more details" % (phedexApi.logFile)
		sys.exit(1)
	print "Renewed proxy"
	func = getattr(phedexApi, sys.argv[1], None)
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
	data = 0
	try:
		data = func(**args)
	except TypeError, e:
		print e
		print "Usage: python ./phedexApi.py <apiCall> ['arg1_name=arg1' 'arg2_name=arg2' ...]"
		sys.exit(1)
	if not data:
		print "PhEDEx call failed, see log (%s) for more details" % (phedexApi.logFile)
		sys.exit(1)
	print data
	sys.exit(0)
