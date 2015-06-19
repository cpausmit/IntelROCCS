#!/usr/bin/env python
"""
File       : intelroccs.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Helper service to access IntelROCCS files online
"""

# system modules
import logging

# package modules
from UADR.services.generic import GenericService
from UADR.core.storage import StorageManager

class IntelROCCSService(object):
    """
    Helper class to access MIT DB
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.service = 'intelroccs'
        self.config = config
        self.target_url = str(config['services'][self.service])
        self.storage_manager = StorageManager(self.config)
