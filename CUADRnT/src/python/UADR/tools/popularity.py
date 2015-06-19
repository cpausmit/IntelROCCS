#!/usr/bin/env python
"""
File       : popularity.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate popularity metric
"""

# system modules
import logging
import threading
import Queue
import time
import datetime
import re
from math import log

# package modules
from UADR.core.storage import StorageManager
from UADR.services.pop_db import PopDBService
from UADR.tools.dataset_collector import DatasetCollector
from UADR.tools.utils import timestamp_day
from UADR.tools.utils import datetime_day
from UADR.tools.utils import datetime_to_pop_db_date
from UADR.tools.utils import timestamp_to_datetime

class Popularity(object):
    """
    Generate popularity metrics for datasets and sites
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.storage_manager = StorageManager(self.config)
        self.pop_db = PopDBService(self.config)
        self.dataset_collector = DatasetCollector(self.config)
        self.max_threads = int(self.config['threading']['max_threads'])

    def get_data(self):
        """
        Get all data needed to generate popularity data
        """
        self.dataset_collector.get_new_datasets()
        dataset_data = threading.Thread(name='dataset_data', target=self.dataset_collector.get_data)
        dataset_data.setDaemon(True)
        dataset_data.start()
        pop_data = threading.Thread(name='pop_data', target=self.get_pop_db_data)
        pop_data.setDaemon(True)
        pop_data.start()
        dataset_data.join()
        pop_data.join()
        self.generate_popularity()

    def get_pop_db_data(self):
        """
        Fetching data from popularity db
        This function will be changed as popularity metric changes
        """
        valid_datasets_patterns = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets_patterns = ''
        invalid_file = open('/var/opt/CUADRnT/invalid_datasets_patterns', 'r')
        for pattern in invalid_file:
            invalid_datasets_patterns += pattern.strip() + '|'
        invalid_datasets_patterns = invalid_datasets_patterns[:-1]
        re_valid_datasets = re.compile(valid_datasets_patterns)
        re_invalid_datasets = re.compile(invalid_datasets_patterns)
        start_date = timestamp_day(time.time()) - 86400*14
        timestamp = self.storage.get_last_insert_time('popularity_data')
        if timestamp > start_date:
            start_date = timestamp_day(timestamp)
        stop_date = timestamp_day(time.time()) - 86400
        api = 'DSStatInTimeWindow/'
        date = start_date
        coll = 'popularity_data'
        while start_date <= stop_date:
            data = list()
            db_date = timestamp_to_datetime(start_date)
            date = datetime_to_pop_db_date(db_date)
            params = {'sitename':'summary', 'tstart':date, 'tstop':date}
            json_data = self.pop_db.fetch(api=api, params=params)
            for dataset in json_data['DATA']:
                dataset_name = dataset['COLLNAME']
                if (re_invalid_datasets.match(dataset_name) or not re_valid_datasets.match(dataset_name)):
                    continue
                n_accesses = dataset['NACC']
                n_cpus = dataset['TOTCPU']
                n_users = dataset['NUSERS']
                data.append({'name':dataset_name, 'date':db_date, 'n_accesses':n_accesses, 'n_cpus':n_cpus, 'n_users':n_users})
            self.storage.insert_data(coll=coll, data=data)
            start_date += 86400

    def generate_popularity(self):
        """
        Spawn daemons to fetch all popularity data
        """
        dataset_names = self.dataset_collector.get_dataset_names()
        q = Queue.Queue()
        for i in range(self.max_threads):
            worker = threading.Thread(target=self.data_worker, args=(q,))
            worker.daemon = True
            worker.start()
        for name in dataset_names:
            q.put(name)
        q.join()

    def data_worker(self, q):
        """
        Daemon worker to fetch popularity data
        """
        while True:
            dataset_name = q.get()
            self.pop_data(dataset_name)
            q.task_done()

    def pop_metric(self, dataset_name):
        """
        Calculate popularity metric
        """
        popularity = 0.0
        # get size of dataset
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'size_gb':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll, pipeline=pipeline)
        size_gb = data['size_gb']
        today = datetime_day(datetime.datetime.utcnow())
        old_pops = list()
        coll = 'popularity_data'
        for i in range(8, 15):
            date = today - datetime.timedelta(days=i)
            pipeline = list()
            match = {'$match':{'name':dataset_name, 'date':date}}
            pipeline.append(match)
            project = {'$project':{'n_accesses':1, 'n_cpus':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            if not data:
                pop = 0
            else:
                pop = log(data['n_accesses'])*log(data['n_cpus'])
            old_pops.append(pop)
        for i in range(1, 8):
            date = today - datetime.timedelta(days=i)
            pipeline = list()
            match = {'$match':{'name':dataset_name, 'date':date}}
            pipeline.append(match)
            project = {'$project':{'n_accesses':1, 'n_cpus':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            if not data:
                new_pop = 0
            else:
                new_pop = log(data['n_accesses'])*log(data['n_cpus'])
            for old_pop in old_pops:
                popularity += new_pop - old_pop
        # update popularity
        popularity = popularity/size_gb
        coll = 'ml'
        query = {'name':dataset_name}
        data = {'name':dataset_name, 'popularity':popularity}
        self.storage.update_data(query=query, data=data, upsert=True)
