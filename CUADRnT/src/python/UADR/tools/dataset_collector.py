#!/usr/bin/env python
"""
File       : dataset_collector.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect hdata about all AnalysisOps datasets
"""

# system modules
import logging
import re
import threading
import Queue

# package modules
from UADR.utils.db_utils import datetime_to_object_id
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import datetime_to_timestamp
from UADR.utils.utils import timestamp_to_datetime
from UADR.services.phedex import PhEDExService
from UADR.services.dbs import DBSService
from UADR.core.storage import StorageManager

class DatasetCollector(object):
    """
    Collects data about all analysis datasets in CMS
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.phedex = PhEDExService(self.config)
        self.dbs = DBSService(self.config)
        self.storage = StorageManager(self.config)
        self.max_threads = int(self.config['threading']['max_threads'])
        self.last_update = 86400

    def get_dataset_names(self):
        """
        Get new datasets in local cache
        """
        pipeline = list()
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll='dataset_data', pipeline=pipeline)
        return [d['name'] for d in data]

    def get_new_datasets(self):
        """
        Get all analysis datasets in CMS from phedex
        """
        # TODO: Keep track of replicas/sites
        valid_datasets_patterns = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets_patterns = ''
        invalid_file = open('/var/opt/CUADRnT/invalid_datasets_patterns', 'r')
        for pattern in invalid_file:
            invalid_datasets_patterns += pattern.strip() + '|'
        invalid_datasets_patterns = invalid_datasets_patterns[:-1]
        re_valid_datasets = re.compile(valid_datasets_patterns)
        re_invalid_datasets = re.compile(invalid_datasets_patterns)
        # Get date of latest update
        timestamp = datetime_to_timestamp(self.storage.get_last_insert_time('dataset_data'))
        self.last_update = timestamp
        # Get new replicas
        api = 'blockreplicas'
        params = {'node':'T*', 'create_since':timestamp, 'group':'AnalysisOps', 'show_dataset':'y'}
        json_data = self.phedex.fetch(api, params)
        dataset_data = list()
        for dataset in json_data['phedex']['dataset']:
            dataset_name = dataset['name']
            if (re_invalid_datasets.match(dataset_name) or not re_valid_datasets.match(dataset_name)):
                continue
            pipeline = list()
            match = {'$match':{'name':dataset_name}}
            pipeline.append(match)
            if self.storage.get_data('dataset_data', pipeline):
                continue
            dataset_data.append({'name':dataset_name})
        if dataset_data:
            self.storage.insert_data('dataset_data', dataset_data)

    def get_data(self):
        """
        Fetch data about all new AnalysisOps datasets
        Call get_new_datasets before to update list of datasets
        """
        dataset_names = self.get_new_dataset_names()
        q = Queue.Queue()
        for i in range(self.max_threads):
            worker = threading.Thread(target=self.data_worker, args=(q,))
            worker.daemon = True
            worker.start()
        for name in dataset_names:
            q.put(name)
        q.join()

    def get_new_dataset_names(self):
        """
        Get new datasets in local cache
        """
        object_id = datetime_to_object_id(timestamp_to_datetime(self.last_update))
        pipeline = list()
        match = {'$match':{'_id':{'$gte':object_id}}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll='dataset_data', pipeline=pipeline)
        return [d['name'] for d in data]

    def data_worker(self, q):
        """
        Spawn daemons to fetch online data
        """
        while True:
            dataset_name = q.get()
            self.get_phedex_data(dataset_name)
            self.get_dbs_data(dataset_name)
            self.get_replicas(dataset_name)
            q.task_done()

    def get_phedex_data(self, dataset_name):
        """
        Get data from phedex for dataset
        """
        api = 'data'
        params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
        json_data = self.phedex.fetch(api=api, params=params)
        dataset = json_data['phedex']['dbs'][0]['dataset'][0]
        size_bytes = 0
        n_files = 0
        for block in dataset['block']:
            size_bytes += block['bytes']
            n_files += block['files']
        size_gb = bytes_to_gb(size_bytes)
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'size_gb':size_gb, 'n_files':n_files}}
        data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def get_dbs_data(self, dataset_name):
        """
        Get data from DBS for dataset
        """
        api = 'datasets'
        params = {'dataset':dataset_name, 'detail':True, 'dataset_access_type':'*'}
        json_data = self.dbs.fetch(api=api, params=params)
        try:
            dataset = json_data['data'][0]
        except:
            print dataset_name
            query = {'name':dataset_name}
            self.storage.delete_data('dataset_data', query)
            return
        ds_name = dataset['primary_ds_name']
        physics_group = dataset['physics_group_name']
        data_tier = dataset['data_tier_name']
        creation_date = dataset['creation_date']
        ds_type = dataset['primary_ds_type']
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'ds_name':ds_name, 'physics_group':physics_group, 'data_tier':data_tier, 'creation_date':creation_date, 'ds_type':ds_type}}
        data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def get_replicas(self, dataset_name):
        """
        Get all sites with a replica of the dataset
        Will check for empty datasets (initial replicas may show up but have no data in them)
        """
        sites = list()
        api = 'blockreplicas'
        params = {'dataset':dataset_name, 'show_dataset':'y'}
        json_data = self.phedex.fetch(api=api, params=params)
        block = json_data['phedex']['dataset'][0]['block'][0]
        for replica in block:
            if replica['files'] == 0:
                continue
            sites.append(replica['node'])
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'replicas':sites}}
        data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)
