#!/usr/bin/env python2.7
"""
File       : pop_db.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Popularity DB service module
"""

# system modules
import logging

# package modules
from cuadrnt.services.generic import GenericService

class PopDBService(GenericService):
    """
    Helper class to access Popularity DB API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'pop_db'
        self.TARGET_URL = str(self.config['services'][self.SERVICE])
