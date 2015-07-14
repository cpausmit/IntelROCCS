#!/usr/bin/env python
"""
File       : delta.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Delta ranking algorithm
"""

# system modules
import logging
import datetime

# package modules
from UADR.utils.utils import datetime_day
from UADR.rankings.generic import GenericRanking

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
        dataset_rankings = dict()
        coll = 'popularity'
        dataset_names = self.datasets.get_datasets()
        for dataset_name in dataset_names:
            delta_popularity = self.get_dataset_popularity(dataset_name)
            # insert into database
            query = {'name':dataset_name, 'date':date}
            data = {'$set':{'name':dataset_name, 'date':date, 'delta_popularity':delta_popularity}}
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
        for dataset_name in dataset_names:
            deviance = dataset_rankings[dataset_name] - average
            dataset_rankings[dataset_name] = deviance
            query = {'name':dataset_name, 'date':date}
            data = {'$set':{'deviance':deviance}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return dataset_rankings

    # def site_rankings(self):
    #     """
    #     Generate site rankings
    #     """

    def get_dataset_popularity(self, dataset_name):
        """
        Get delta popularity for dataset
        """
        coll = 'dataset_data'
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=14)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=8)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        unwind = {'$unwind':'$popularity_data'}
        pipeline.append(unwind)
        match = {'$match':{'popularity_data.date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'delta_popularity':{'$sum':'$popularity_data.popularity'}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        old_pop = data[0]['delta_popularity']
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=7)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=1)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        unwind = {'$unwind':'$popularity_data'}
        pipeline.append(unwind)
        match = {'$match':{'popularity_data.date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'delta_popularity':{'$sum':'$popularity_data.popularity'}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        new_pop = data[0]['delta_popularity']
        delta_popularity = new_pop - old_pop
        return delta_popularity
