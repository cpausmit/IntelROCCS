#!/usr/bin/env python2.7
"""
File       : delta.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Delta ranking algorithm
"""

# system modules
import logging
import datetime

# package modules
from cuadrnt.utils.utils import datetime_day
from cuadrnt.rankings.generic import GenericRanking

class DeltaRanking(GenericRanking):
    """
    Use delta popularity values to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)

    def dataset_rankings(self):
        """
        Generate dataset rankings
        """
        date = datetime_day(datetime.datetime.utcnow())
        dataset_names = self.datasets.get_db_datasets()
        dataset_rankings = dict()
        coll = 'dataset_popularity'
        for dataset_name in dataset_names:
            delta_popularity = self.get_dataset_popularity(dataset_name)
            # insert into database
            query = {'name':dataset_name, 'date':date}
            data = {'$set':{'delta_popularity':delta_popularity}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            # store into dict
            dataset_rankings[dataset_name] = delta_popularity
        # calculate average
        pipeline = list()
        group = {'$group':{'_id':None, 'average':{'$avg':'$delta_popularity'}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        average = data[0]['average']
        # apply to dict
        coll = 'dataset_rankings'
        for dataset_name in dataset_names:
            rank = dataset_rankings[dataset_name] - average
            dataset_rankings[dataset_name] = rank
            query = {'name':dataset_name, 'date':date}
            data = {'$set':{'name':dataset_name, 'date':date, 'delta_rank':rank}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return dataset_rankings

    def site_rankings(self):
        """
        Generate site rankings
        """
        date = datetime_day(datetime.datetime.utcnow())
        # get all sites which can be replicated to
        site_names = self.sites.get_available_sites()
        site_rankings = dict()
        for site_name in site_names:
            # get popularity
            popularity = self.get_site_popularity(site_name)
            # get cpu and storage (performance)
            performance = self.sites.get_performance(site_name)
            # get available storage
            available_storage = self.sites.get_available_storage(site_name)
            # insert into database
            coll = 'site_popularity'
            query = {'name':site_name, 'date':date}
            data = {'$set':{'name':site_name, 'date':date, 'delta_popularity':popularity, 'performance':performance, 'available_storage':available_storage}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            #calculate rank
            rank = performance*available_storage*popularity
            # store into dict
            site_rankings[site_name] = rank
            # insert into database
            coll = 'site_rankings'
            query = {'name':site_name, 'date':date}
            data = {'$set':{'name':site_name, 'date':date, 'delta_rank':rank}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return site_rankings

    def get_dataset_popularity(self, dataset_name):
        """
        Get delta popularity for dataset
        """
        coll = 'dataset_popularity'
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=14)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=8)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'old_popularity':{'$sum':{'$multiply':['$n_accesses', '$n_cpus', '$n_users']}}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            old_pop = data[0]['old_popularity']
        except:
            old_pop = 0.0
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=7)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=1)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'new_popularity':{'$sum': {'$multiply':['$n_accesses', '$n_cpus', '$n_users']}}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            new_pop = data[0]['new_popularity']
        except:
            new_pop = 0.0
        delta_popularity = new_pop - old_pop
        size_gb = self.datasets.get_size(dataset_name)
        return delta_popularity/size_gb

    def get_site_popularity(self, site_name):
        """
        Get delta popularity for site
        """
        date = datetime_day(datetime.datetime.utcnow())
        # get all datasets with a replica at the site and how many replicas it has
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'replicas':site_name}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        popularity = 0.0
        for dataset in data:
            dataset_name = dataset['name']
            # get the popularity of the dataset and dicide by number of replicas
            coll = 'dataset_popularity'
            pipeline = list()
            match = {'$match':{'name':dataset_name, 'date':date}}
            pipeline.append(match)
            project = {'$project':{'delta_popularity':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            popularity += data[0]['delta_popularity']
        return popularity
