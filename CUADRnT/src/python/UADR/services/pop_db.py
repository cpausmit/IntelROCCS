#!/usr/local/bin/python
"""
File       : pop_db.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Popularity DB service module
"""

# system modules
import logging
import os
import json

# package modules
from UADR.services.generic import GenericService
from UADR.utils.sso_utils import sso_fetch

class PopDBService(GenericService):
    """
    Helper class to access Popularity DB API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.name = 'pop_db'
        self.logger = logging.getLogger(__name__)
        self.cookie_path = os.path.join(os.environ['HOME'], '.globus')
        self.target_url = config['services'][self.name]

    def fetch(self, api, params=dict(), cache=True, cache_only=False):
        """
        Fetch data from Popularity DB service
        If param cache is not true update cache on cache miss
        If param cache_only is true just update the cache, don't return any data.
            Use this parameter to spawn external thread to update cache in background
        This will be replaced by GenericService fetch function once SSO cookie identification is removed
        """
        data = sso_fetch(self.cookie_path, self.target_url, api, params)
        json_data = json.loads(data)
        return json_data
