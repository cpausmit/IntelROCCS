#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
#
# This script will find a Tier-2 appropriate to serve as the initial location for the specifified
# dataset.
#
#---------------------------------------------------------------------------------------------------
import os, sys, subprocess, getopt, re, random, urllib, urllib2, httplib, json

#===================================================================================================
#  C L A S S E S
#===================================================================================================

#---------------------------------------------------------------------------------------------------
class phedexApi:
#---------------------------------------------------------------------------------------------------
    """
    _phedexApi_

    Interface to submit queries to the PhEDEx API For specifications of calls see
    https://cmsweb.cern.ch/phedex/datasvc/doc

    Class variables:

    phedexBase -- Base URL to the PhEDEx web API (https://cmsweb.cern.ch/phedex/datasvc/)
    """
    # Useful variables
    #-----------------
    # phedexInstance = "prod" or "dev"
    # dataType = "json" or "xml"
    # site = "T2_US_Nebraska"
    # dataset = "/Muplus_Pt1_PositiveEta-gun/Muon2023Upg14-DES23_62_V1-v1/GEN-SIM"
    # group = 'AnalysisOps' (or 'local')

    def __init__(self):
        """
        __init__
        Set up class constants
        """
        self.phedexBase = "https://cmsweb.cern.ch/phedex/datasvc/"

    def phedexCall(self, url, values):
        """
        _phedexCall_

        Make http post call to PhEDEx API. Function only gaurantees that something is returned, the
        caller need to check the response for correctness.

        url    - URL to make API call
        values - arguments to pass to the call

        Return values:
        1 -- Status, 0 = everything went well, 1 = something went wrong
        2 -- IF status == 0 : HTTP response ELSE : Error message
        """
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        try:
            response = opener.open(request)
        except urllib2.HTTPError, e:
            return 1, " Error - urllib2.HTTPError %s \n  URL: %s\n  VALUES: %s"%\
                   (e.read,str(url),str(values))
        except urllib2.URLError, e:
            return 1, " Error - urllib2.URLError %s \n  URL: %s\n  VALUES: %s"%\
                   (e.args,str(url),str(values))
        return 0, response

    def data(self, dataset='', block='', fileName='', level='block',
             createSince='', format='json', instance='prod'):
        """
        _data_

        PhEDEx data call. At least one of the arguments dataset, block, file have to be passed.  No
        checking is made for xml data. Even if JSON data is returned no gaurantees are made for the
        structure of it

        Keyword arguments:
        dataset     -- Name of dataset to look up
        block       -- Name of block to look up
        file        -- Name of file to look up
        block       -- Only return data for this block
        fileName    -- Data for file fileName returned
        level       -- Which granularity of dataset information to show
        createSince -- Files/blocks/datasets created since this date/time
        format      -- Which format to return data as, XML or JSON
        instance    -- Which instance of PhEDEx to query, dev or prod

        Return values:
        check -- 0 if all went well, 1 if error occured
        data  -- json structure if json format, xml structure if xml format
        """
        if not (dataset or block or fileName):
            return 1, " Error - Need to pass at least one of dataset/block/fileName"
        values = { 'dataset' : dataset, 'block' : block, 'file' : fileName,
                   'level' : level, 'create_since' : createSince }
        dataURL = urllib.basejoin(self.phedexBase, "%s/%s/data"%(format, instance))
        check, response = self.phedexCall(dataURL, values)
        if check:
            return 1, " Error - Data call failed"
        if format == "json":
            try:
                data = json.load(response)
            except ValueError, e:
                # This usually means that PhEDEx didn't like the URL
                return 1, " ERROR - ValueError in call to url %s : %s"%(dataURL, str(e))
            if not data:
                return 1, " Error - no json data available"
        else:
            data = response.read()
        return 0, data

    def parse(self, data, xml):
        """
        _parse_

        Take data output from PhEDEx and parse it into xml syntax corresponding to subscribe and
        delete calls.
        """
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
                if (k == "name" or k == "is-open" or k == "is-transient" or \
                    k == "bytes" or k== "checksum"):
                    xml = '%s %s="%s"' % (xml, k, v)
        return xml

    def xmlData(self, datasets=[], instance='prod'):
        """
        _xmlData_

        Get json data from PhEDEx for all datasets and convert it to a xml structure complient with
        the PhEDEx delete/subscribe call.

        datasets - list of dataset names
        instance - the instance on which the datasets resides, prod/dev

        Return values:
        error -- 1 if an error occurred, 0 if everything went as expected
        xml   -- the converted data now represented as an xml structure
        """
        if not datasets:
            return 1, " Error - need to pass at least one of dataset."
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">'\
              % (xml, 'dbs', instance)
        for dataset in datasets:
            check, response = self.data(dataset=dataset, level='file', instance=instance)
            if check:
                return 1, " Error"
            data = response.get('phedex').get('dbs')
            if not data:
                return 1, " Error"
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xml_data = "%s</data>" % (xml,)

        return 0, xml_data

    def subscribe(self, node='', data='', level='dataset', priority='low', move='n', static='n',
                  custodial='n', group='AnalysisOps', timeStart='', requestOnly='n', noMail='n',
                  comments='', format='json', instance='prod'):
        """
        _subscribe_

        Set up subscription call to PhEDEx API.
        """
        if not (node and data):
            return 1, "Error - subscription: node and data needed."
        values = { 'node' : node, 'data' : data, 'level' : level, 'priority' : priority,
                   'move' : move, 'static' : static, 'custodial' : custodial, 'group' : group,
                   'time_start' : timeStart, 'request_only' : requestOnly, 'no_mail' : noMail,
                   'comments' : comments }
        subscriptionURL = urllib.basejoin(self.phedexBase, "%s/%s/subscribe" % (format, instance))
        check, response = self.phedexCall(subscriptionURL, values)
        if check:
            return 1, "Error - subscription: check not zero"
        return 0, response

    def delete(self, node='', data='', level='dataset', rmSubscriptions='y',
               comments='', format='json', instance='prod'):
        """
        _delete_

        Set up subscription call to PhEDEx API.
        """
        if not (node and data):
            return 1, " Error - need to pass both node and data"
        values = { 'node' : node, 'data' : data, 'level' : level,
                   'rm_subscriptions' : rmSubscriptions, 'comments' : comments }
        deleteURL = urllib.basejoin(self.phedexBase, "%s/%s/delete" % (format, instance))
        check, response = self.phedexCall(deleteURL, values)
        if check:
            return 1, " Error - self.phedexCall with response: " + response
        return 0, response

#---------------------------------------------------------------------------------------------------
class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_

    Get proxy to acces PhEDEx API. Needed for subscribe and delete calls.

    Class variables:
      key  -- user key to CERN with access to PhEDEx
      cert -- user certificate connected to key
    """

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        proxy = os.environ['X509_USER_PROXY']
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def testLocalSetup(dataset,debug=0):
    # check the input parameters
    if dataset == '':
        print ' Error - no dataset specified. EXIT!\n'
        print usage
        sys.exit(1)
    
    # check the user proxy
    if os.environ.get('X509_USER_PROXY'):
        if debug > 0:
            print ' Using user proxy: ' + os.environ.get('X509_USER_PROXY')
    else:
        print ' Error - no X509_USER_PROXY, please define the variable correctly. EXIT!'
        sys.exit(1)
        
    # check das_client.py tool
    cmd = 'which das_client.py'
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        line    = line[:-1]
    if line != "":
        if debug > 0:
            print ' Using das_client.py from: ' + line
    else:
        print ' Error - das_client.py in your path, please find it and add it to PATH. EXIT!'
        sys.exit(1)

def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    # this is the text including the size units, that need to be converted
    sizeGb  = float(sizeTxt[0:-2])
    units   = sizeTxt[-2:]
    # decide what to do for the given unit
    if   units == 'MB':
        sizeGb = sizeGb/1024.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1024.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(0)

    # return the size in GB as a float
    return sizeGb
    
def findExistingSubscriptions(dataset,debug=0):

    webServer = 'https://cmsweb.cern.ch/'
    phedexBlocks = 'phedex/datasvc/xml/prod/blockreplicas?subscribed=y&node=T2*&dataset=' + dataset
    cert = os.environ.get('X509_USER_PROXY')
    url = '"'+webServer+phedexBlocks + '"'
    cmd = 'curl --cert ' + cert + ' -k -H "Accept: text/xml" ' + url + ' 2> /dev/null'
    if debug > 1:
        print ' Access phedexDb: ' + cmd 
    
    # setup the shell command
    siteNames = ''
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        if debug > 1:
            print ' LINE: ' + line
        # find the potential T2s
        try:
            sublines = re.split("<replica\ ",line)
            for subline in sublines[1:]:
                siteName = (re.findall(r"node='(\S+)'",subline))[0]
                siteNames += ' ' + siteName
        except:
            siteName = ''

    return siteNames
    
def getActiveSites(debug=0):
    # hardcoded fallback
    tier2Base = [ 'T2_AT_Vienna','T2_BR_SPRACE','T2_CH_CSCS','T2_DE_DESY','T2_DE_RWTH',
                  'T2_ES_CIEMAT','T2_ES_IFCA',
                  'T2_FR_IPHC','T2_FR_GRIF_LLR',
                  'T2_IT_Pisa','T2_IT_Bari','T2_IT_Rome',
                  'T2_RU_JINR',
                  'T2_UK_London_IC',
                  'T2_US_Caltech','T2_US_Florida','T2_US_MIT','T2_US_Nebraska','T2_US_Purdue',
                  'T2_US_Wisconsin'
                  ]

    # download list of active sites
    sites = []

    # get the active site list
    cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/ActiveSites.txt'
    cmd += ' -O - 2> /dev/null'
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        site = line[:-1]
        if debug > 1:
            print ' Adding site: ' + site
        sites.append(site)

    # something went wrong
    if len(sites) < 10:
        print ' WARNINIG - too few sites found, reverting to hardcoded list'
        sites = tier2Base

    # return the size in GB as a float
    return sites

def chooseMatchingSite(tier2Sites,sizeGb,debug):

    iRan = -1
    quota = 0
    nTrials = 0

    while sizeGb > 0.1*quota:
        iRan = random.randint(0,len(tier2Sites)-1)
        site = tier2Sites[iRan]
        # not elegant or reliable (should use database directly)
        cmd  = 'wget http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'+site+'/Summary.txt'
        cmd += ' -O - 2> /dev/null | grep ^Total'
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
            line = line[:-1]
            f = line.split(' ')
            quota = float(f[-1]) * 1024.  # make sure it is GB
        if debug > 0:
            print ' Trying to fit %.1f GB into Tier-2 [%d]: %s with quota of %.1f GB (use 0.1 max)'%\
                  (sizeGb,iRan,site,quota)
    
        if nTrials > 20:
            print ' Error - no matching site could be found. Dataset too big? EXIT!'
            sys.exit(1)
    
        nTrials += 1

    return iRan,site,quota
    
def submitSubscriptionRequest(site, datasets=[]):
    
    # make sure we have datasets to subscribe
    if len(datasets) < 1:
        print " ERROR - Trying to submit empty request for " + site
        return

    phedex = phedexApi()

    # compose data for subscription request
    check,data = phedex.xmlData(datasets=datasets,instance='prod')

    if check: 
        print " ERROR - phedexApi.xmlData failed"
        return
    
    # here the request is really sent
    message = 'IntelROCCS -- Automatic Dataset Subscription by Computing Operations.'
    if not exe:
        print " Message: " + message
        print " --> check,response \ "
        print " = phedex.subscribe(node=site,data=data,comments=message,group='AnalysisOps', \ "
        print "                    instance='prod')"
    check,response = phedex.subscribe(node=site,data=data,comments=message,group='AnalysisOps',
                                      instance='prod')
    if check:
        print " ERROR - phedexApi.subscribe failed"
        print response
        return

#===================================================================================================
#  M A I N
#===================================================================================================
# Define string to explain usage of the script
usage =  " Usage: assignDatasetToSite.py   --dataset=<name of a CMS dataset>\n"
usage += "                               [ --debug=0 ]\n"
usage += "                               [ --exec ]\n"
usage += "                               [ --help ]\n\n"

# Define the valid options which can be specified and check out the command line
valid = ['dataset=','debug=','exec','help']
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

# --------------------------------------------------------------------------------------------------
# Get all parameters for the production
# --------------------------------------------------------------------------------------------------
# Set defaults for each command line parameter/option
debug = 0
dataset = ''
exe = False

# Read new values from the command line
for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(0)
    if opt == "--dataset":
        dataset = arg
    if opt == "--debug":
        debug = arg
    if opt == "--exec":
        exe = True

# inspecting the local setup
#---------------------------

testLocalSetup(dataset,debug)

# size of provided dataset
#-------------------------

# use das client to find the present size of the dataset
cmd = 'das_client.py --format=plain --limit=0 --query="file dataset=' + \
      dataset + ' | sum(file.size)" |tail -1 | cut -d= -f2'
for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        line    = line[:-1]

# this is the text including the size units, that needs to be converted)
if line != '':
    sizeGb = convertSizeToGb(line)
else:
    print ' Error - no reasonable size found with das_client.py.'
    sys.exit(1)

if not exe:
    print '\n DAS information:  %.1f GB  %s'%(sizeGb,dataset)

# has the dataset already been subscribed?
#-----------------------------------------
# - no test that the complete dataset has been subscribed (could be just one block?)
# - we test all Tier2s and check there is at least one block subscribed no completed bit required
#
# --> need to verify this is sufficient

siteNames = findExistingSubscriptions(dataset,debug)

if siteNames != '':
    print ' Already subscribed on Tier-2:' + siteNames
    print '\n The job is done already: EXIT!\n'
    sys.exit(0)

# find a matching site
#---------------------

# find all dynamically managed sites
tier2Sites = getActiveSites(debug)

# choose a site randomly and exclude sites that are too small
iRan,site,quota = chooseMatchingSite(tier2Sites,sizeGb,debug)

if not exe:
    print ''
    print ' SUCCESS - Assigned to Tier-2 [%d]: %s (quota: %.1f GB)'%(iRan,site,quota)

# make phedex subscription
#-------------------------

# prepare subscription list
datasets = []
datasets.append(dataset)

# subscribe them
if exe:
    print ' -> initial subscribtion of  %s  to %s'%(dataset,site)
    submitSubscriptionRequest(site,datasets)
else:
    print ' -> not subscribing .... please use  --exec  option.'
