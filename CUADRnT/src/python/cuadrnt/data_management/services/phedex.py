#!/usr/bin/env python2.7
"""
File       : phedex.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: PhEDEx service module
"""

# system modules
import logging

# package modules
from cuadrnt.data_management.services.generic import GenericService

class PhEDExService(GenericService):
    """
    Helper class to access PhEDEx API
    Subclass of GenericService
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'phedex'
        self.TARGET_URL = str(self.config['services'][self.SERVICE])

    def generate_xml(self, dataset_names=list()):
        """
        Generate XML data for subscription call to phedex
        """
        xml = '<data version="2.0">'
        xml = xml + '<dbs name="https://cmsweb.cern.ch/dbs/prod/global/DBSReader">'
        for dataset_name in dataset_names:
            api = 'data'
            params = [('dataset', dataset_name), ('level', 'block')]
            json_data = self.fetch(api=api, params=params)
            try:
                data = json_data['phedex']['dbs'][0]['dataset'][0]
            except IndexError:
                self.logger.warning("Couldn't generate XML data for dataset %s", dataset_name)
                continue
            xml = xml + '<dataset name="%s" is-open="%s">' % (data['name'], data['is_open'])
            xml = xml + "</dataset>"
        xml = xml + "</dbs>"
        xml = xml + "</data>"
        return xml
