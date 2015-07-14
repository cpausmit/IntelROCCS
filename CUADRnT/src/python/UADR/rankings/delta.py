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
        dataset_rankings = dict()
        dataset_names = self.datasets.get_datasets()
        for dataset_name in dataset_names:
            popularity = self.get_dataset_popularity(dataset_name)
            # insert into database
            # store into dict
        # calculate average
        # apply to dict

    # def site_rankings(self):
    #     """
    #     Generate site rankings
    #     """

    def get_dataset_popularity(self, dataset_name):
        """
        Get delta popularity for dataset
        """
        old_pop = 0
        coll = 'dataset_data'
        start_date = datetime.datetime.utcnow() - datetime.timedelta(days=14)
        end_date = datetime.datetime.utcnow() - datetime.timedelta(days=8)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        unwind = {'$unwind':'$popularity_data'}
        pipeline.append(unwind)
        match = {'$match':{'popularity_data':{'date':{'$gte':start_date, '$lte':end_date}}}}
        pipeline.append(match)
        # group = {'$group':{'_id':'$name', 'delta_poppularity':{'$sum':'$popularity_data.popularity'}}}
        # pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        print data
