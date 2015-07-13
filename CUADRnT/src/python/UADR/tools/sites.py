#!/usr/bin/env python
"""
File       : sites.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect data about all AnalysisOps datasets
"""

# system modules
import logging
import datetime

# package modules
from UADR.services.intelroccs import IntelROCCSService
from UADR.services.crab import CRABService
from UADR.tools.storage import StorageManager

class SiteManager(object):
    """
    Keep track of site data
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.intelroccs = IntelROCCSService(self.config)
        self.crab = CRABService(self.config)
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
            self.update_cpu(site_name)

    def update_cpu(self, site_name):
        """
        Update maximum CPU capacity for site
        """
        # remove older values
        date = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        coll = 'site_data'
        query = {'name':site_name}
        data = {'$pull':{'cpu_data':{'date':{'$lt':date}}}}
        self.storage.update_data(coll=coll, query=query, data=data)
        # get CRAB data about site
        query = 'GLIDEIN_CMSSite =?= "%s" && CPUs > 0' % (site_name)
        attributes = ["CPUs"]
        ads = self.crab.fetch_cluster_ads(query=query, attributes=attributes)
        cpus = 0
        for ad in ads:
            cpus += ad['CPUs']
        # insert new data
        date = datetime.datetime.utcnow()
        query = {'name':site_name}
        data = {'$push':{'cpu_data':{'date':date, 'cpus':cpus}}}
        self.storage.update_data(coll=coll, query=query, data=data)

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

    def get_max_cpus(self, site_name):
        """
        Get the maximum number of CPU's for site in last 30 days
        """
        coll = 'site_data'
        pipeline = list()
        match = {'$match':{'name':site_name}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'max_cpus':{'$max':'cpu_data.cpus'}}}
        pipeline.append(group)
        project = {'$project':{'max_cpus':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            max_cpus = data[0]['max_cpus']
        except:
            max_cpus = 0
        return max_cpus
