#!/usr/local/bin/python
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic service module
"""

# Shared properties between services:
#   Contact a web service using a base url and some key:value parameters
#   Services require a valid cert and key
#   Want to cache results in a document-oriented database

# system modules
import time

# package modules
from UADR.utils.utils import get_data
from UADR.core.storage import StorageManager

class GenericService(object):
    """
    Generic UADR service class
    """
    def __init__(self, config=None, verbose=0):
        if not config:
            config = {}
        self.name = 'generic'
        self.verbose = verbose
        self.storage = StorageManager(config)

    def fetch(self, url, params=dict(), cache=True):
        """
        Get data from url using parameters params
        """
        debug = 0
        data = "[]"
        # first try to get data from cache
        # if data not in cache or out of date fetch from online service
        if self.verbose:
            debug = self.verbose - 1
        # try to re fetch data 3 times on error
        try:
            data = get_data(url, params, debug=debug)
        except Exception as ex:
            print str(ex)
            for attempt in xrange(3):
                time.sleep(0.1)
        return data
