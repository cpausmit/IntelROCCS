#!/usr/bin/env python
"""
File       : phedex.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: PhEDEx service module
"""

# system modules
import logging

# package modules
from UADR.services.generic import GenericService

class PhEDExService(GenericService):
    """
    Helper class to access PhEDEx API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.service = 'phedex'
        self.target_url = str(self.config['services'][self.service])
