#!/usr/local/bin/python
"""
File       : url_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions to deal with internet services
"""

# system modules
import logging
import os
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

def get_data(target_url, api, params=dict()):
    """
    Create http request for target_url, api and params of service
    Data is json data returned as a string
    Use json.loads(data) to generate json structure
    Check for ValueError if not valid json
    """
    headers = {'Accept': 'application/json'}
    url_data = urllib.urlencode(params, doseq=True)
    full_url = '%s/%s?%s' % (target_url, api, url_data)
    request = urllib2.Request(full_url)
    for key, val in headers.items():
        request.add_header(key, val)
    handler = HTTPSClientAuthHandler()
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    try:
        return_data = urllib2.urlopen(request)
        data = return_data.read()
    except Exception as e:
        logger.warning("Couldn't fetch data for url %s\n    Reason:\n    %s", full_url, str(e))
        data = "{}"
    return data, full_url
