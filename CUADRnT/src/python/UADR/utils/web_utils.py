#!/usr/bin/env python2.7
"""
File       : web_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions to deal with internet services
"""

# system modules
import logging
import os
import json
import urllib
import httplib
import urllib2

# Get module specific logger
logger = logging.getLogger(__name__)

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class
    """
    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = self.get_proxy()
        self.cert = self.key

    def get_proxy(self):
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
        return proxy

    def https_open(self, request):
        """Open request method"""
        return self.do_open(self.get_connection, request)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if self.cert:
            return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def get_secure_data(target_url, api, params=dict(), method='get'):
    """
    Create http request for target_url, api and params of service
    Data should be json data returned as a string
    params can also be a list of tuples where the first value is the param name
    and the second is the value, this value can be a list of values which will
    then be split up
    """
    headers = {'Accept': 'application/json'}
    data = urllib.urlencode(params, doseq=True)
    url = '%s/%s' % (target_url, api)
    if method == 'post':
        request = urllib2.Request(url, data)
    else:
        full_url = '%s?%s' % (str(url), str(data))
        request = urllib2.Request(full_url)
    for key, val in headers.items():
        request.add_header(key, val)
    handler = HTTPSClientAuthHandler()
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    try:
        return_data = urllib2.urlopen(request)
        return_data = return_data.read()
        json_data = json.loads(return_data)
    except Exception as e:
        logger.warning("Couldn't fetch data for url %s?%s\n    Reason:\n    %s", str(url), str(data), str(e))
        json_data = dict()
    return json_data

def get_data(target_url, api, file_):
    """
    Extract info from online text file
    Format of file:
        tab separated fields
        comments marked using #
        headers commented out
    """
    json_data = list()
    try:
        response = urllib2.urlopen('%s/%s/%s' % (target_url, api, file_))
    except Exception as e:
        logger.warning("Couldn't fetch data for url %s/%s/%s\n    Reason:\n    %s", str(target_url), str(api), str(file_), str(e))
    else:
        data = response.read()
        for line in data.split('\n'):
            if not (line.find('DataOps') == -1):
                break
            if not line or line[0] == '#':
                continue
            row = line.split()
            json_data.append(row)
    return json_data
