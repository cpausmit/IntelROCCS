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

    def initiate_popularity_data(self, dataset_name, end_date=datetime_day(datetime.datetime.utcnow())):
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
        coll = 'dataset_popularity'
        for date in daterange(creation_date, end_date):
            query = {'name':dataset_name, 'date':date}
            popularity_data = {'name':dataset_name, 'date':date}
            try:
                popularity_data['n_accesses'] = pop_data['naccess'][datetime_to_string(date)]
                popularity_data['n_cpus'] = pop_data['totcpu'][datetime_to_string(date)]
                popularity_data['n_users'] = pop_data['nusers'][datetime_to_string(date)]
            except:
                popularity_data['n_accesses'] = 0
                popularity_data['n_cpus'] = 0
                popularity_data['n_users'] = 0
            try:
                popularity_data['popularity'] = log(popularity_data['n_accesses'])*log(popularity_data['n_cpus'])*log(popularity_data['n_users'])
            except:
                popularity_data['popularity'] = 0
            data = {'$set':popularity_data}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)

    def update_popularity(self, dataset_names):
        """
        Fetch lates popularity data not in database
        """
        # get dates
        coll = 'dataset_popularity'
        pipeline = list()
        sort = {'$sort':{'date':-1}}
        pipeline.append(sort)
        limit = {'$limit':1}
        pipeline.append(limit)
        project = {'$project':{'date':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            start_date = data[0]['date']
        except:
            self.logger.warning('Did not update popularity, most likely because it needs to be initiated')
            return
        end_date = datetime_day(datetime.datetime.utcnow())
        # fetch popularity data
        for date in daterange(start_date, end_date):
            api = 'DSStatInTimeWindow/'
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
            for dataset_name in dataset_names:
                popularity_data = {'name':dataset_name, 'date':date}
                coll = 'dataset_popularity'
                pipeline = list()
                match = {'$match':{'name':dataset_name, 'date':date}}
                pipeline.append(match)
                project = {'$project':{'name':1, '_id':0}}
                pipeline.append(project)
                try:
                    dataset = pop_data[dataset_name]
                except:
                    popularity_data['n_accesses'] = 0
                    popularity_data['n_cpus'] = 0
                    popularity_data['n_users'] = 0
                else:
                    popularity_data['n_accesses'] = dataset['n_accesses']
                    popularity_data['n_cpus'] = dataset['n_cpus']
                    popularity_data['n_users'] = dataset['n_users']
                try:
                    popularity_data['popularity'] = log(popularity_data['n_accesses'])*log(popularity_data['n_cpus'])*log(popularity_data['n_users'])
                except:
                    popularity_data['popularity'] = 0
                query = {'name':dataset_name, 'data':date}
                data = {'$set':popularity_data}
                self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
