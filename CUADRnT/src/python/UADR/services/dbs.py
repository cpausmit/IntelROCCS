#!/usr/bin/env python
"""
File       : dbs.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: DBS service module
"""

# system modules
import logging

# package modules
from UADR.services.generic import GenericService

class DBSService(GenericService):
    """
    Helper class to access DBS reader API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.service = 'dbs'
        self.target_url = str(config['services'][self.service])
