#!/usr/local/bin/python
"""
File       : pop_db.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Popularity DB service module
"""

# system modules
import logging

# package modules
from UADR.services.generic import GenericService

class PopDBService(GenericService):
    """
    Helper class to access Popularity DB API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.target_url = config['services']['pop_db']
