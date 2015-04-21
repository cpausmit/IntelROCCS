#!/usr/local/bin/python
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic service module
"""

# TODO: (5) Implement cache

# system modules
import json

# package modules
from UADR.utils.url_utils import fetch

class GenericService(object):
    """
    Generic UADR service class
    Shared properties between services:
        Contact a web service using a base url and some key:value parameters
        Services require a valid cert and key
        Want to cache results in a document-oriented database
    """
    def __init__(self, config=dict(), debug=0):
        self.config = config
        self.name = 'generic'
        self.debug = debug

    def fetch(self, api, params=dict(), cache=True, cache_only=False):
        """
        Get data from url using parameters params
        If param cache is not true update cache on cache miss
        If param cache_only is true just update the cache, don't return any data.
            Use this parameter to spawn external thread to update cache in background
        """
        if self.debug:
            print "%s: Fetching %s data for %s" % (self.name, api, str(params))
        data = fetch(self.target_url, api, params, self.name, debug=self.debug)
        json_data = json.loads(data)
        return json_data
