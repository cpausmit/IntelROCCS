#!/usr/bin/env python
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic service module
"""

# system modules
import logging

# package modules
from UADR.core.storage import StorageManager
from UADR.utils.web_utils import get_secure_data
from UADR.utils.web_utils import get_data

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
        self.service = 'generic'
        self.config = config
        self.target_url = ''
        self.storage = StorageManager(config)

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
                json_data = self.storage.get_cache(self.service, api, params)
            if not json_data:
                if secure:
                    json_data = get_secure_data(target_url=self.target_url, api=api, params=params, method=method)
                else:
                    json_data = get_data(target_url=self.target_url, api=api, file_=params)
                if type(json_data) is not dict:
                    json_data = {'data':json_data}
                self.storage.insert_cache(self.service, api, params, json_data)
            if not cache_only:
                return json_data
        else:
            if secure:
                json_data = get_secure_data(target_url=self.target_url, api=api, params=params, method=method)
            else:
                json_data = get_data(target_url=self.target_url, api=api, file_=params)
            if type(json_data) is not dict:
                json_data = {'data':json_data}
            return json_data
