#!/usr/local/bin/python
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic service module
"""

# TODO: (5) Implement cache

# system modules
import logging
import json

# package modules
from UADR.utils.url_utils import get_data

class GenericService(object):
    """
    Generic UADR service class
    Shared properties between services:
        Contact a web service using a base url and some key:value parameters
        Services require a valid cert and key
        Want to cache results in a document-oriented database
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.target_url = ''

    def fetch(self, api, params=dict(), cache=True, cache_only=False):
        """
        Get data from url using parameters params
        If param cache is not true update cache on cache miss
        If param cache_only is true just update the cache, don't return any data.
            Use this parameter to spawn external thread to update cache in background
        """
        data, full_url = get_data(target_url=self.target_url, api=api, params=params)
        try:
            json_data = json.loads(data)
        except:
            self.logger.warning("Couldn't fetch data for url %s\n    Reason:\n    %s", full_url, str(data))
        return json_data
