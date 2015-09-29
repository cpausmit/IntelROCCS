#!/usr/bin/env python2.7
"""
File       : dbs.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: DBS service module
"""

# system modules
import logging

# package modules
from cuadrnt.services.generic import GenericService

class DBSService(GenericService):
    """
    Helper class to access DBS reader API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'dbs'
        self.TARGET_URL = str(self.config['services'][self.SERVICE])
