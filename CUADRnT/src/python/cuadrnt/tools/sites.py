#!/usr/bin/env python2.7
"""
File       : sites.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect data about all AnalysisOps datasets
"""

# system modules
import logging
import datetime

# package modules
from cuadrnt.utils.utils import get_json
from cuadrnt.services.intelroccs import IntelROCCSService
from cuadrnt.services.crab import CRABService
from cuadrnt.tools.storage import StorageManager

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

    def initiate_db(self):
        """
        Initiate Site database
        Does exactly the same as update_db
        """
        self.update_db()

    def update_db(self):
        """
        Initiate site data in database
        Get general data about all sites
        """
        api = 'Detox'
        file_ = 'SitesInfo.txt'
        intelroccs_data = self.intelroccs.fetch(api=api, params=file_, secure=False)
        for site_data in get_json(intelroccs_data, 'data'):
            self.insert_site_data(site_data)

    def insert_site_data(self, site_data):
        """
        Insert site into database
        """
        coll = 'site_data'
        site_name = str(site_data[4])
        site_status = int(site_data[0])
        site_quota = int(site_data[1])*10**3
        query = {'name':site_name}
        data = {'$set':{'name':site_name, 'status':site_status, 'quota_gb':site_quota}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=True)

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
        sites_data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return [site_data['name'] for site_data in sites_data]

    def update_cpu(self):
        """
        Update maximum CPU capacity for site
        """
        active_sites = self.get_active_sites()
        for site_name in active_sites:
            # remove older values
            date = datetime.datetime.utcnow() - datetime.timedelta(days=30)
            coll = 'site_data'
            query = {'name':site_name}
            data = {'$pull':{'cpu_data':{'date':{'$lt':date}}}}
            self.storage.update_data(coll=coll, query=query, data=data)
            # get CRAB data about site
            query = 'GLIDEIN_CMSSite =?= "%s" && CPUs > 0' % (site_name)
            attributes = ['GLIDEIN_CMSSite', 'CPUs']
            ads = self.crab.fetch_cluster_ads(query, attributes=attributes)
            cpus = 0
            for ad in ads:
                cpus += ad['CPUs']
            # insert new data
            date = datetime.datetime.utcnow()
            query = {'name':site_name}
            data = {'$push':{'cpu_data':{'date':date, 'cpus':cpus}}}
            self.storage.update_data(coll=coll, query=query, data=data)

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

    def get_performance(self, site_name):
        """
        Get the maximum number of CPU's for site in last 30 days
        """
        # get maximum number of CPU's and quota
        coll = 'site_data'
        pipeline = list()
        match = {'$match':{'name':site_name}}
        pipeline.append(match)
        unwind = {'$unwind':'$cpu_data'}
        pipeline.append(unwind)
        group = {'$group':{'_id':'$name', 'quota_gb':{'$max':'$quota_gb'}, 'max_cpus':{'$max':'$cpu_data.cpus'}}}
        pipeline.append(group)
        project = {'$project':{'quota_gb':1, 'max_cpus':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            max_cpus = data[0]['max_cpus']
        except:
            max_cpus = 0
        quota = float(data[0]['quota_gb'])/10**3
        try:
            performance = float(max_cpus)/float(quota)
        except:
            performance = 0.0
        if not (performance > 0):
            performance = 0.0
        return performance

    def get_available_storage(self, site_name):
        """
        Get total AnalysisOps storage available at the site
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'replicas':site_name}}
        pipeline.append(match)
        group = {'$group':{'_id':None, 'size_bytes':{'$sum':'$size_bytes'}}}
        pipeline.append(group)
        project = {'$project':{'size_bytes':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            size = data[0]['size_bytes']/10**9
        except:
            return 0
        coll = 'site_data'
        pipeline = list()
        match = {'$match':{'name':site_name}}
        pipeline.append(match)
        project = {'$project':{'quota_gb':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        quota = data[0]['quota_gb']
        available_gb = (0.95*quota) - size
        return available_gb
