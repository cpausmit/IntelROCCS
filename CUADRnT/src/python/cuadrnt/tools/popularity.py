#!/usr/bin/env python2.7
"""
File       : popularity.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate popularity metric
"""

# system modules
import logging
import datetime
import Queue
import threading

# package modules
# from cuadrnt.utils.utils import pop_db_timestamp_to_datetime
from cuadrnt.utils.utils import datetime_to_string
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.utils import pop_db_timestamp_to_datetime
from cuadrnt.utils.utils import daterange
from cuadrnt.utils.utils import get_json
from cuadrnt.services.pop_db import PopDBService
from cuadrnt.tools.sites import SiteManager
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.storage import StorageManager

class PopularityManager(object):
    """
    Generate popularity metrics for datasets and sites
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.pop_db = PopDBService(self.config)
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.storage = StorageManager(self.config)
        self.MAX_THREADS = int(config['threading']['max_threads'])

    def initiate_db(self):
        """
        Collect popularity data
        """
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_popularity_data, args=(i, q))
            worker.daemon = True
            worker.start()
        start_date = datetime_day(datetime.datetime.utcnow() - datetime.timedelta(days=90))
        end_date = datetime_day(datetime.datetime.utcnow())
        # fetch popularity data
        t1 = datetime.datetime.utcnow()
        for date in daterange(start_date, end_date):
            q.put(date)
        q.join()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Inserting Pop DB data took %s', str(td))

    def insert_popularity_data(self, i, q):
        """
        Insert popularity data for one dataset into db
        """
        coll = 'dataset_popularity'
        while True:
            date = q.get()
            api = 'DSStatInTimeWindow/'
            tstart = datetime_to_string(date)
            tstop = tstart
            params = {'sitename':'summary', 'tstart':tstart, 'tstop':tstop}
            json_data = self.pop_db.fetch(api=api, params=params)
            # sort it in dictionary for easy fetching
            for dataset in json_data['DATA']:
                dataset_name = dataset['COLLNAME']
                popularity_data = {'name':dataset_name, 'date':date}
                popularity_data['n_accesses'] = dataset['NACC']
                popularity_data['n_cpus'] = dataset['TOTCPU']
                popularity_data['n_users'] = dataset['NUSERS']
                query = {'name':dataset_name, 'data':date}
                data = {'$set':popularity_data}
                self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            q.task_done()

    def update_db(self):
        """
        Fetch latest popularity data not in database
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
            self.logger.warning('Popularity needs to be initiated')
            self.initiate_db()
            return
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_popularity_data, args=(i, q))
            worker.daemon = True
            worker.start()
        end_date = datetime_day(datetime.datetime.utcnow())
        # fetch popularity data
        t1 = datetime.datetime.utcnow()
        for date in daterange(start_date, end_date):
            q.put(date)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Updating Pop DB data took %s', str(td))

    def insert_dataset(self, dataset_name):
        """
        Fetch all popularity data for dataset
        """
        api = 'getSingleDSstat'
        sitename = 'summary'
        name = dataset_name
        aggr = 'day'
        orderbys = ['totcpu', 'naccess']
        coll = 'dataset_popularity'
        for orderby in orderbys:
            params = {'sitename':sitename, 'name':name, 'aggr':aggr, 'orderby':orderby}
            json_data = self.pop_db.fetch(api=api, params=params)
            data = get_json(json_data, 'data')
            for pop_data in get_json(data, 'data'):
                date = pop_db_timestamp_to_datetime(pop_data[0])
                query = {'name':dataset_name, 'data':date}
                popularity_data = {'name':dataset_name, 'date':date}
                popularity_data[orderby] = pop_data[1]
                data = {'$set':popularity_data}
                self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
