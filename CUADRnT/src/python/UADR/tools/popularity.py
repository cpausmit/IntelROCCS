#!/usr/bin/env python
"""
File       : popularity.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate popularity metric
"""

# system modules
import logging
import datetime
from math import log

# package modules
from UADR.utils.utils import pop_db_timestamp_to_datetime
from UADR.utils.utils import datetime_to_string
from UADR.utils.utils import datetime_day
from UADR.utils.utils import daterange
from UADR.services.pop_db import PopDBService
from UADR.tools.sites import SiteManager
from UADR.tools.storage import StorageManager

class PopularityManager(object):
    """
    Generate popularity metrics for datasets and sites
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.pop_db = PopDBService(self.config)
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)

    def initiate_popularity_data(self, dataset_name):
        """
        Collect popularity from creation date for dataset
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'creation_date':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        creation_date = data[0]['creation_date']
        api = 'getSingleDSstat'
        pop_data = dict()
        for metric in ['totcpu', 'naccess', 'nusers']:
            params = {'name':dataset_name, 'sitename':'summary', 'aggr':'day', 'orderby':metric}
            json_data = self.pop_db.fetch(api=api, params=params)
            try:
                data = json_data[0]['data']
            except:
                pop_data[metric] = dict()
            else:
                pop_data[metric] = dict()
                for date_data in data:
                    date = datetime_to_string(pop_db_timestamp_to_datetime(date_data[0]))
                    pop_data[metric][date] = date_data[1]
        popularity_data = list()
        end_date = datetime_day(datetime.datetime.utcnow())
        age = 0
        for date in daterange(creation_date, end_date):
            data = {'date':date, 'age':age}
            try:
                data['n_accesses'] = pop_data['naccess'][datetime_to_string(date)]
                data['n_cpus'] = pop_data['totcpu'][datetime_to_string(date)]
                data['n_users'] = pop_data['nusers'][datetime_to_string(date)]
            except:
                data['n_accesses'] = 0
                data['n_cpus'] = 0
                data['n_users'] = 0
            try:
                data['popularity'] = log(data['n_accesses'])*log(data['n_cpus'])*log(data['n_users'])
            except:
                data['popularity'] = 0
            popularity_data.append(data)
            age += 1
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$push':{'popularity_data':{'$each':popularity_data}}}
        self.storage.update_data(coll=coll, query=query, data=data)

    def update_popularity(self, dataset_names):
        """
        Add popularity data for yesterday if not existing
        """
        # fetch popularity data
        api = 'DSStatInTimeWindow/'
        date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=1)
        tstart = datetime_to_string(date)
        tstop = tstart
        params = {'sitename':'summary', 'tstart':tstart, 'tstop':tstop}
        json_data = self.pop_db.fetch(api=api, params=params)
        # sort it in dictionary for easy fetching
        pop_data = dict()
        for dataset in json_data['DATA']:
            dataset_name = dataset['COLLNAME']
            n_accesses = dataset['NACC']
            n_cpus = dataset['TOTCPU']
            n_users = dataset['NUSERS']
            pop_data[dataset_name] = {'n_accesses':n_accesses, 'n_cpus':n_cpus, 'n_users':n_users}
        # loop through all datasets and get data, if no data set to 0
        coll = 'dataset_data'
        for dataset_name in dataset_names:
            pipeline = list()
            match = {'$match':{'name':dataset_name, 'popularity_data.date':date}}
            pipeline.append(match)
            project = {'$project':{'name':1, '_id':0}}
            pipeline.append(project)
            if self.storage.get_data(coll=coll, pipeline=pipeline):
                continue
            try:
                dataset = pop_data[dataset_name]
            except:
                popularity_data = {'date':date, 'n_accesses':0, 'n_cpus':0, 'n_users':0}
            else:
                popularity_data = {'date':date, 'n_accesses':dataset['n_accesses'], 'n_cpus':dataset['n_cpus'], 'n_users':dataset['n_users']}
            try:
                popularity_data['popularity'] = log(popularity_data['n_accesses'])*log(popularity_data['n_cpus'])*log(popularity_data['n_users'])
            except:
                popularity_data['popularity'] = 0
            query = {'name':dataset_name}
            data = {'$push':{'popularity_data':popularity_data}}
            self.storage.update_data(coll=coll, query=query, data=data)
        # it doesn't matter if data is already set, will be the same
