#!/usr/bin/env python
"""
File       : storage.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Maintain a mongodb instance which is used for caching data, store calculated data and logging
"""

# system modules
import logging
import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ServerSelectionTimeoutError, DocumentTooLarge
from subprocess import call

# package modules
from UADR.utils.db_utils import get_object_id
from UADR.utils.utils import datetime_day
from UADR.utils.utils import datetime_remove_timezone

# TODO: Handle returned values

class StorageManager(object):
    """
    Helper class to access local mongodb instance
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        uri = str(config['mongodb']['uri'])
        db = str(config['mongodb']['db'])
        client = MongoClient(host=uri, serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except ServerSelectionTimeoutError:
            # server is not running, start it
            self.logger.info('Starting mongodb server %s', uri)
            call("start_mongodb")
        self.db = client[db]
        data_coll = self.db['dataset_data']
        try:
            data_coll.create_index([('name', ASCENDING)], background=True, unique=True)
        except ServerSelectionTimeoutError:
            self.logger.error('Could not connect to mongodb server %s', uri)
        else:
            for service in config['services'].keys():
                cache_coll = self.db[service]
                cache_coll.create_index('datetime', expireAfterSeconds=86400)

    def insert_cache(self, coll, api, params=dict(), data=dict()):
        """
        Insert data into collection
        Collection should be the service it is caching data for
        Use update to have the possibility to force cache update
        """
        db_coll = self.db[coll]
        object_id = get_object_id(str(api)+str(params))
        data['_id'] = object_id
        data['datetime'] = datetime_day(datetime.datetime.utcnow())
        try:
            result = db_coll.replace_one({'_id':object_id}, data, upsert=True)
        except DocumentTooLarge:
            self.logger.warning('DocumentTooLarge error for %s api %s', coll, api)
        else:
            if (result.modified_count == 0) and (not result.upserted_id):
                self.logger.warning('Failed to insert %s cache for api %s\n\tData: %s', coll, api, str(data))

    def get_cache(self, coll, api, params=dict()):
        """
        Fetch cached data for a service
        Collection should be name of service
        """
        data = dict()
        db_coll = self.db[coll]
        object_id = get_object_id(str(api)+str(params))
        if coll == 'pop_db':
            # pop db data doesn't change but is specific to a certain date so if it's being accessed, keep it
            # TODO: Can we gaurantee this?
            data = db_coll.find_one_and_update({'_id':object_id}, {'$set':{'datetime':datetime_day(datetime.datetime.utcnow())}}, {'_id':0})
        else:
            data = db_coll.find_one({'_id':object_id}, {'_id':0})
        return data

    def insert_data(self, coll, data=list(), ordered=False):
        """
        Insert data into any collection
        """
        db_coll = self.db[coll]
        result = db_coll.insert_many(data, ordered=ordered)
        if not result.inserted_ids:
            self.logger.warning('No data inserted in %s\n\tData: %s', coll, str(data))
        else:
            self.logger.info('%d documents inserted in %s', len(result.inserted_ids), coll)

    def update_data(self, coll, query=dict(), data=dict(), upsert=False):
        """
        Update data in any collection
        """
        db_coll = self.db[coll]
        result = db_coll.update_many(query, data, upsert=upsert)
        if result.modified_count == 0 and len(result.upserted_id) == 0:
            self.logger.warning('No data updated in %s\n\tQuery: %s\n\tData: %s', coll, str(query), str(data))
        else:
            self.logger.info('%d documents updated in %s', result.matched_count, coll)

    def delete_data(self, coll, query=dict()):
        """
        Remove data in any collection
        """
        db_coll = self.db[coll]
        result = db_coll.delete_many(query)
        if result.deleted_count == 0:
            self.logger.warning('No data deleted in %s\n\tQuery: %s', coll, str(query))
        else:
            self.logger.info('%d documents deleted in %s', result.deleted_count, coll)

    def get_data(self, coll, pipeline=list()):
        """
        Fetch data from any collection
        Use aggregate function for more powerful queries
        """
        data = list()
        db_coll = self.db[coll]
        return_data = db_coll.aggregate(pipeline)
        if return_data:
            data = list(return_data)
        else:
            self.logger.warning('No data returned in %s\n\tPipeline: %s', coll, str(pipeline))
        return data

    def get_last_insert_time(self, coll):
        """
        Get datetime object of last insert
        """
        pipeline = list()
        sort = {'$sort':{'_id':-1}}
        pipeline.append(sort)
        limit = {'$limit':1}
        pipeline.append(limit)
        project = {'$project':{'_id':1}}
        pipeline.append(project)
        data = self.get_data(coll, pipeline=pipeline)
        datetime_ = datetime.datetime(1970, 1, 2, 0, 0, 0)
        if data:
            datetime_ = data[0]['_id'].generation_time
            datetime_ = datetime_remove_timezone(datetime_)
        else:
            self.logger.info('Collection %s is empty, no last insert datetime', coll)
        return datetime_
