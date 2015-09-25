#!/usr/bin/env python2.7
"""
File       : intelroccs.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Helper service to access IntelROCCS files online
"""

# system modules
import logging

# package modules
from cuadrnt.services.generic import GenericService

class IntelROCCSService(GenericService):
    """
    Helper class to access IntelROCCS files online
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'intelroccs'
        self.TARGET_URL = str(self.config['services'][self.SERVICE])
