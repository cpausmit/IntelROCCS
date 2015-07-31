#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script provide an API to PhEDEX communications one can delete or subscribe datasets using
# methods defined in this class.
#
# This script uses auxilary tool for logging purposes in case the request runs into the error.
#
#---------------------------------------------------------------------------------------------------
__author__       = '*Bjorn Barrefors, $Maxim Goncharov, $Christoph Paus'
__organization__ = '*Holland Computing Center - University of Nebraska-Lincoln, $MIT'
__email__        = 'bbarrefo@cse.unl.edu, maxi@mit.edu, paus@mit.edu'

import sys
import os
import re
import urllib
import urllib2
import httplib
import time
import datetime
try:
    import json
except ImportError:
    import simplejson as json
from cmsDataLogger import cmsDataLogger

####################################################################################################
#
#                             P h E D E x   A P I
#
####################################################################################################
class phedexApi:
    """
    _phedexApi_

    Interface to submit queries to the PhEDEx API For specifications of calls see
    https://cmsweb.cern.ch/phedex/datasvc/doc

    Class variables:

    phedexBase -- Base URL to the PhEDEx web API
    logger     -- Used to print log and error messages to log file
    """
    # Useful variables
    # phedexBase = "https://cmsweb.cern.ch/phedex/datasvc/"
    # phedexInstance = "prod" or "dev
    # dataType = "json" or "xml"
    # site = "T2_US_Nebraska"
    # dataset = "/BTau/GowdyTest10-Run2010Av3/RAW"
    # group = 'local' or 'AnalysisOps'
    def __init__(self, logPath=''):
        """
        __init__

        Set up class constants
        """
        statusDirectory = os.environ['UNDERTAKER_DB']
        self.logger     = cmsDataLogger(statusDirectory+'/')
        self.phedexBase = "https://cmsweb.cern.ch/phedex/datasvc/"

    ############################################################################
    #                                                                          #
    #                           P h E D E x   C A L L                          #
    #                                                                          #
    ############################################################################
    def phedexCall(self, url, values):
        """
        _phedexCall_

        Make http post call to PhEDEx API.

        Function only gaurantees that something is returned,
        the caller need to check the response for correctness.

        Keyword arguments:
        url    -- URL to make API call
        values -- Arguments to pass to the call

        Return values:
        1 -- Status, 0 = everything went well, 1 = something went wrong
        2 -- IF status == 0 : HTTP response ELSE : Error message
        """
        name = "phedexCall"
        data = urllib.urlencode(values)
        opener = urllib2.build_opener(HTTPSGridAuthHandler())
        request = urllib2.Request(url, data)
        try:
            response = opener.open(request)
        except urllib2.HTTPError, e:
            self.logger.error(name, e.read())
            self.logger.error(name, "URL: %s" % (str(url),))
            self.logger.error(name, "VALUES: %s" % (str(values),))
            return 1, " ERROR - urllib2.HTTPError"
        except urllib2.URLError, e:
            self.logger.error(name, e.args)
            self.logger.error(name, "URL: %s" % (str(url),))
            self.logger.error(name, "VALUES: %s" % (str(values),))
            return 1, " ERROR - urllib2.URLError"
        return 0, response
    ############################################################################
    #                                                                          #
    #                                  D A T A                                 #
    #                                                                          #
    ############################################################################
    def data(self, dataset='', block='', fileName='', level='block',
             createSince='', format='json', instance='prod'):
        """
        _data_

        PhEDEx data call

        At least one of the arguments dataset, block, file have to be passed

        No checking is made for xml data

        Even if JSON data is returned no gaurantees are made for the structure
        of it

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
        name = "data"
        if not (dataset or block or fileName):
            self.logger.error(name, "Need to pass at least one of dataset/block/fileName")
            return 1, "Error"
        values = { 'dataset' : dataset, 'block' : block, 'file' : fileName,
                   'level' : level, 'create_since' : createSince }
        dataURL = urllib.basejoin(self.phedexBase, "%s/%s/data" % (format, instance))
        check, response = self.phedexCall(dataURL, values)
        if check:
            # An error occurred
            print "failed here"
            print response
            self.logger.error(name, "Data call failed")
            return 1, "Error"
        if format == "json":
            try:
                data = json.load(response)
            except ValueError, e:
                # This usually means that PhEDEx didn't like the URL
                self.logger.error(name, "In call to url %s : %s" % (dataURL, str(e)))
                return 1, " ERROR - ValueError"
            if not data:
                self.logger.error(name, "No json data available")
                return 1, " ERROR - no data"
        else:
            data = response.read()
        return 0, data
    ############################################################################
    #                                                                          #
    #                                 P A R S E                                #
    #                                                                          #
    ############################################################################
    def parse(self, data, xml):
        """
        _parse_

        Take data output from PhEDEx and parse it into  xml syntax
        corresponding to subscribe and delete calls.
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
    ############################################################################
    #                                                                          #
    #                             X M L   D A T A                              #
    #                                                                          #
    ############################################################################
    def xmlData(self, datasets=[], instance='prod'):
        """
        _xmlData_

        Get json data from PhEDEx for all datasets and convert it to a xml
        structure complient with the PhEDEx delete/subscribe call.

        Keyword arguments:
        datasets -- List of dataset names
        instance -- The instance on which the datasets resides, prod/dev

        Return values:
        error -- 1 if an error occurred, 0 if everything went as expected
        xml   -- The converted data now represented as an xml structure
        """
        name = "xmlData"
        # @CHANGED: Function now takes a list of datasets instead of only one
        if not datasets:
            self.logger.error(name, "Need to pass at least one of dataset")
            return 1, "Error"
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">'\
              % (xml, 'dbs', instance)
        for dataset in datasets:
            check, response = self.data(dataset=dataset, level='file', instance=instance)
            if check:
                return 1, "Error"
            data = response.get('phedex').get('dbs')
            if not data:
                return 1, "Error"
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xml_data = "%s</data>" % (xml,)
        return 0, xml_data
    ############################################################################
    #                                                                          #
    #                             S U B S C R I B E                            #
    #                                                                          #
    ############################################################################
    def subscribe(self, node='', data='', level='dataset', priority='low',
                  move='n', static='n', custodial='n', group='AnalysisOps',
                  timeStart='', requestOnly='n', noMail='n', comments='',
                  format='json', instance='prod'):
        """
        _subscribe_

        Set up subscription call to PhEDEx API.
        """
        name = "subscribe"
        if not (node and data):
            self.logger.error(name, "Need to pass both node and data")
            return 1, "Error"
        values = { 'node' : node, 'level' : level, 'priority' : priority,
                   'move' : move, 'static' : static, 'custodial' : custodial, 'group' : group,
                   'time_start' : timeStart, 'request_only' : requestOnly, 'no_mail' : noMail,
                   'comments' : comments, 'data' : data}
        subscriptionURL = urllib.basejoin(self.phedexBase, "%s/%s/subscribe" % (format, instance))
        check, response = self.phedexCall(subscriptionURL, values)
        if check:
            # An error occurred
            self.logger.error(name, "Subscription call failed")
            #print values
            return 1, "Error"
        return 0, response
    ############################################################################
    #                                                                          #
    #                                D E L E T E                               #
    #                                                                          #
    ############################################################################
    def delete(self, node='', data='', level='dataset', rmSubscriptions='y',
               comments='', format='json', instance='prod'):
        name = "delete"
        if not (node and data):
            self.logger.error(name, "Need to pass both node and data")
            return 1, "Error"
        values = { 'node' : node, 'data' : data, 'level' : level,
                   'rm_subscriptions' : rmSubscriptions, 'comments' : comments }
        deleteURL = urllib.basejoin(self.phedexBase, "%s/%s/delete" % (format, instance))
        check, response = self.phedexCall(deleteURL, values)
        if check:
            self.logger.error(name, "Delete call failed")
            return 1, "ERROR - self.phedexCall with response: " + response
        return 0, response

    def updateRequest(self, decision, request, node, comments='',format='json', instance='prod'):
        name = "update"
        values = {'decision':decision, 'request':request, 'node':node, 'comments':comments}
        url = urllib.basejoin(self.phedexBase, "%s/%s/updaterequest" % (format, instance))
        check, response = self.phedexCall(url, values)
        if check:
            self.logger.error(name, "Update call failed")
            return 1, "ERROR - self.phedexCall with response: " + response
        return 0, response

####################################################################################################
#
#                       H T T P S   G R I D   A U T H   H A N D L E R
#
####################################################################################################
class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_

    Get  proxy to acces PhEDEx API

    Needed for subscribe and delete calls

    Class variables:
    key  -- User key to CERN with access to PhEDEx
    cert -- User certificate connected to key
    """
    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.getProxy()
        self.cert = self.key
    def https_open(self, req):
        return self.do_open(self.getConnection, req)
    def getProxy(self):
        proxy = os.environ['UNDERTAKER_X509UP']
        return proxy
    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

####################################################################################################
#
#                                            M A I N
#
####################################################################################################
if __name__ == '__main__':
    """
    __main__

    For testing purpose only
    """

    # Example for deletion

    #phedexApi = phedexApi(logPath='./')
    #check, data = phedexApi.xmlData(datasets=['/MET/Run2012A-22Jan2013-v1/AOD'], instance='prod')
    #if check:
    #    sys.exit(1)
    #print data
    #
    #check, response = \
    #       phedexApi.delete(node='T2_US_MIT', data=data, instance='prod',
    #                        comments='Just a test by Christoph Paus for Maxim Goncharov.')
    #if check:
    #    print "This is the response from phedexApi.delete: " + response
    #    sys.exit(1)
    #
    #print response.read()

    # Example for subscription

    #phedexApi = phedexApi(logPath='./')
    #check, data = phedexApi.xmlData(
    #    datasets=['/Muplus_Pt1_PositiveEta-gun/Muon2023Upg14-DES23_62_V1-v1/GEN-SIM'], instance='prod')
    #if check:
    #    sys.exit(1)
    #print data
    #
    #check, response = \
    #       phedexApi.subscribe(node='T2_US_MIT', data=data, instance='prod', group='AnalysisOps',
    #                           comments='Just a test by Christoph Paus for Maxim Goncharov.')
    #if check:
    #    print "This is the response from phedexApi.delete: " + response
    #    sys.exit(1)
    #
    #print response.read()

    #sys.exit(0)
