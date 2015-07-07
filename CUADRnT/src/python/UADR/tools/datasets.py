#!/usr/bin/env python
"""
File       : datasets.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Handle dataset data
"""

# system modules
import logging
import threading

# package modules
from UADR.utils.utils import timestamp_to_datetime
from UADR.utils.utils import datetime_day
from UADR.services.phedex import PhEDExService
from UADR.services.dbs import DBSService
from UADR.tools.sites import SiteManager
from UADR.tools.popularity import PopularityManager
from UADR.tools.storage import StorageManager

class DatasetManager(object):
    """
    Handle all dataset related data
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.phedex = PhEDExService(self.config)
        self.dbs = DBSService(self.config)
        self.storage = StorageManager(self.config)
        self.sites = SiteManager(self.config)
        self.popularity = PopularityManager(self.config)

    def update_datasets(self):
        """
        Get datasets currently in AnalysisOps and compare to database
        Deactivate removed datasets and insert new
        Update replicas
        """
        # get all datasets in database
        datasets = set(self.get_datasets())
        # get all active sites, only fetch replicas from these
        active_sites = self.sites.get_active_sites()
        api = 'blockreplicas'
        params = [('node', active_sites), ('create_since', 0.0), ('complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        json_data = self.phedex.fetch(api=api, params=params)
        current_datasets = set()
        for dataset in json_data['phedex']['dataset']:
            dataset_name = dataset['name']
            current_datasets.add(dataset_name)
            if dataset_name not in datasets:
                # this is a new dataset which need to be inserted into the database
                self.insert_dataset(dataset_name)
            # update replicas
            replicas = set()
            for block in dataset['block']:
                for replica in block['replica']:
                    replicas.add(replica['node'])
            coll = 'dataset_data'
            query = {'name':dataset_name}
            data = {'$set':{'replicas':list(replicas)}}
            data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)
        deprecated_datasets = datasets - current_datasets
        for dataset_name in deprecated_datasets:
            self.remove_dataset(dataset_name)
        dataset_names = self.get_datasets()
        self.popularity.update_popularity(dataset_names)

    def insert_dataset(self, dataset_name):
        """
        Insert a new dataset into the database
        Set static data
        """
        coll = 'dataset_data'
        data = [{'name':dataset_name}]
        self.storage.insert_data(coll=coll, data=data)
        phedex_worker = threading.Thread(target=self.insert_phedex_data, args=(dataset_name,))
        phedex_worker.daemon = True
        phedex_worker.start()
        dbs_worker = threading.Thread(target=self.insert_dbs_data, args=(dataset_name,))
        dbs_worker.daemon = True
        dbs_worker.start()
        phedex_worker.join()
        dbs_worker.join()
        self.popularity.initiate_popularity_data(dataset_name)

    def insert_phedex_data(self, dataset_name):
        """
        Fetch phedex data about dataset and insert into database
        """
        api = 'data'
        params = {'dataset':dataset_name, 'level':'block', 'create_since':0.0}
        json_data = self.phedex.fetch(api=api, params=params)
        dataset = json_data['phedex']['dbs'][0]['dataset'][0]
        size_bytes = 0
        n_files = 0
        for block in dataset['block']:
            size_bytes += block['bytes']
            n_files += block['files']
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'size_bytes':size_bytes, 'n_files':n_files}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def insert_dbs_data(self, dataset_name):
        """
        Fetch dbs data about dataset and insert into database
        """
        api = 'datasets'
        params = {'dataset':dataset_name, 'detail':True, 'dataset_access_type':'*'}
        json_data = self.dbs.fetch(api=api, params=params)
        dataset = json_data['data'][0]
        ds_name = dataset['primary_ds_name']
        physics_group = dataset['physics_group_name']
        data_tier = dataset['data_tier_name']
        creation_date = datetime_day(timestamp_to_datetime(dataset['creation_date']))
        ds_type = dataset['primary_ds_type']
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'ds_name':ds_name, 'physics_group':physics_group, 'data_tier':data_tier, 'creation_date':creation_date, 'ds_type':ds_type}}
        data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def remove_dataset(self, dataset_name):
        """
        Remove dataset from database
        """
        coll = 'dataset_data'
        query = {'name':dataset_name}
        self.storage.delete_data(coll=coll, query=query)

    def get_datasets(self):
        """
        Get all datasets currently in database
        """
        coll = 'dataset_data'
        pipeline = list()
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        dataset_names = [d['name'] for d in data]
        self.logger.info('%d datasets present in database', len(dataset_names))
        return dataset_names
