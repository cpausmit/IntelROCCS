#!/usr/bin/env python
"""
File       : sites.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect data about all AnalysisOps datasets
"""

# system modules
import logging

# package modules
from UADR.services.intelroccs import IntelROCCSService
from UADR.tools.storage import StorageManager

class SiteManager(object):
    """
    Keep track of site data
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.intelroccs = IntelROCCSService(self.config)
        self.storage = StorageManager(self.config)

    def update_sites(self):
        """
        Update all site statuses
        """
        coll = 'site_data'
        api = 'Detox'
        file_ = 'SitesInfo.txt'
        json_data = self.intelroccs.fetch(api=api, params=file_, secure=False)
        for site in json_data['data']:
            site_name = str(site[4])
            site_status = int(site[0])
            site_quota = int(site[1])*10**3
            query = {'name':site_name}
            data = {'$set':{'name':site_name, 'status':site_status, 'quota':site_quota}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)

    def update_cpu(self, site_name):
        """
        Update maximum CPU capacity for site
        """
        # TODO: Implement function

    def get_active_sites(self):
        """
        Get all sites which are active, includes sites which are not available for replication
        """
        coll = 'site_data'
        pipeline = list()
        match = {'$match':{'status':{'$in':[1, 2]}}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return [site['name'] for site in data]

    def get_available_sites(self):
        """
        Get all sites which are available for replication
        """
        coll = 'site_data'
        pipeline = list()
        match = {'$match':{'status':1}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return [site['name'] for site in data]

    def get_replicas(self, dataset_name):
        """
        Get all sites with a replica of the dataset
        """
        # TODO: Implement function
