#!/usr/bin/env python2.7
"""
File       : crab.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: CRAB service access module
"""

# system modules
import logging
import htcondor

# package modules
from cuadrnt.data_management.services.generic import GenericService

class CRABService(GenericService):
    """
    Helper class to access CRAB
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'crab'
        collector_uri = str(self.config['services'][self.SERVICE])
        self.collector = htcondor.Collector(collector_uri)

    def fetch_cluster_ads(self, query, attributes=list()):
        """
        Get CRAB ads from HTCondor
        """
        query = str(query)
        ads = list()
        try:
            returned_ads = self.collector.query(htcondor.AdTypes.Startd, query, attributes)
            for ad in returned_ads:
                ads.append(ad)
        except Exception as e:
            self.logger.warning('CRAB query failed for\nquery: %s\nattributes: %s\n    Reason:\n    %s', str(query), str(attributes), str(e))
        return ads

    def fetch_job_ads(self, query, attributes=list()):
        """
        Get CRAB schedulers from HTCondor
        """
        query = str(query)
        ads = list()
        schedulers = self.collector.locateAll(htcondor.DaemonTypes.Schedd)
        for scheduler in schedulers:
            try:
                schedd = htcondor.Schedd(scheduler)
                returned_ads = schedd.query(query, attributes)
                for ad in returned_ads:
                    ads.append(ad)
            except Exception as e:
                self.logger.warning('CRAB query failed for\nquery: %s\nattributes: %s\n    Reason:\n    %s', str(query), str(attributes), str(e))
        return ads
