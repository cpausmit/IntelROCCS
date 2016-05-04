#!/usr/bin/env python2.7
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic service module
"""

# system modules
import logging

# package modules
from cuadrnt.utils.web_utils import get_secure_data
from cuadrnt.utils.web_utils import get_data
from cuadrnt.data_management.core.storage import StorageManager

class GenericService(object):
    """
    Generic cuadrnt service class
    Shared properties between services:
        Contact a web service using a base url and some key:value parameters
        Services require a valid cert and key
        Want to cache results in a document-oriented database
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.storage = StorageManager(self.config)
        self.SERVICE = 'generic'
        self.TARGET_URL = ''

    def fetch(self, api, params=dict(), method='get', secure=True, cache=True, cache_only=False, force_cache=False):
        """
        Get data from url using parameters params
        If param cache is true update cache on cache miss
        If param cache_only is true just update the cache, don't return any data.
            Use this parameter to spawn external thread to update cache in background
        """
        if cache:
            json_data = dict()
            if not force_cache:
                json_data = self.storage.get_cache(self.SERVICE, api, params)
            if not json_data:
                if secure:
                    json_data = get_secure_data(target_url=self.TARGET_URL, api=api, params=params, method=method)
                else:
                    json_data = get_data(target_url=self.TARGET_URL, api=api, file_=params)
                if type(json_data) is not dict:
                    json_data = {'data':json_data}
                self.storage.insert_cache(self.SERVICE, api, params, json_data)
            if not cache_only:
                return json_data
        else:
            if secure:
                json_data = get_secure_data(target_url=self.TARGET_URL, api=api, params=params, method=method)
            else:
                json_data = get_data(target_url=self.TARGET_URL, api=api, file_=params)
            if type(json_data) is not dict:
                json_data = {'data':json_data}
            return json_data
