#!/usr/bin/env python2.7
"""
File       : storage.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Maintain a mongodb instance which is used for caching data, store calculated data and logging
"""

# system modules
import logging
import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ServerSelectionTimeoutError, DocumentTooLarge, AutoReconnect
from subprocess import call

# package modules
from UADR.utils.db_utils import get_object_id
from UADR.utils.utils import datetime_day
from UADR.utils.utils import datetime_remove_timezone

class StorageManager(object):
    """
    Helper class to access local mongodb instance
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.uri = str(self.config['mongodb']['uri'])
        self.db_name = str(self.config['mongodb']['db'])
        self.opt_path = str(config['paths']['opt'])
        client = MongoClient(host=self.uri, serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except ServerSelectionTimeoutError:
            # server is not running, start it
            self.logger.info('Starting mongodb server %s', self.uri)
            call(["start_mongodb", self.opt_path])
        self.db = client[self.db_name]
        data_coll = self.db['dataset_data']
        try:
            data_coll.create_index([('name', ASCENDING)], background=True, unique=True)
        except ServerSelectionTimeoutError:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
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
        result = list()
        db_coll = self.db[coll]
        object_id = get_object_id(str(api)+str(params))
        data['_id'] = object_id
        data['datetime'] = datetime_day(datetime.datetime.utcnow())
        for i in range(2):
            try:
                result = db_coll.replace_one({'_id':object_id}, data, upsert=True)
            except DocumentTooLarge:
                self.logger.warning('DocumentTooLarge error for %s api %s', coll, api)
                break
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if (result.modified_count == 0) and (not result.upserted_id):
                    self.logger.debug('Failed to insert %s cache for api %s\n\tData: %s', coll, api, str(data))
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return result

    def get_cache(self, coll, api, params=dict()):
        """
        Fetch cached data for a service
        Collection should be name of service
        """
        data = dict()
        db_coll = self.db[coll]
        object_id = get_object_id(str(api)+str(params))
        for i in range(2):
            try:
                data = db_coll.find_one({'_id':object_id}, {'_id':0})
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return data

    def insert_data(self, coll, data=list(), ordered=False):
        """
        Insert data into any collection
        """
        result = list()
        db_coll = self.db[coll]
        for i in range(2):
            try:
                result = db_coll.insert_many(data, ordered=ordered)
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if not result.inserted_ids:
                    self.logger.debug('No data inserted in %s\n\tData: %s', coll, str(data))
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return result

    def get_data(self, coll, pipeline=list()):
        """
        Fetch data from any collection
        Use aggregate function for more powerful queries
        """
        data = list()
        db_coll = self.db[coll]
        for i in range(2):
            try:
                return_data = db_coll.aggregate(pipeline)
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if return_data:
                    data = list(return_data)
                else:
                    self.logger.debug('No data returned in %s\n\tPipeline: %s', coll, str(pipeline))
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return data

    def update_data(self, coll, query=dict(), data=dict(), upsert=False):
        """
        Update data in any collection
        """
        db_coll = self.db[coll]
        for i in range(2):
            try:
                result = db_coll.update_many(query, data, upsert=upsert)
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if result.modified_count == 0 and (not result.upserted_id):
                    self.logger.debug('No data updated in %s\n\tQuery: %s\n\tData: %s', coll, str(query), str(data))
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return result

    def delete_data(self, coll, query=dict()):
        """
        Delete data in any collection
        """
        db_coll = self.db[coll]
        for i in range(2):
            try:
                result = db_coll.delete_many(query)
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if result.deleted_count == 0:
                    self.logger.debug('No data deleted in %s\n\tQuery: %s', coll, str(query))
                else:
                    self.logger.debug('%d documents deleted in %s', result.deleted_count, coll)
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return result

    def get_last_insert_time(self, coll):
        """
        Get datetime object of last insert
        """
        datetime_ = datetime.datetime(1970, 1, 2, 0, 0, 0)
        pipeline = list()
        sort = {'$sort':{'_id':-1}}
        pipeline.append(sort)
        limit = {'$limit':1}
        pipeline.append(limit)
        project = {'$project':{'_id':1}}
        pipeline.append(project)
        for i in range(2):
            try:
                data = self.get_data(coll, pipeline=pipeline)
            except AutoReconnect:
                call(["start_mongodb", self.opt_path])
                continue
            else:
                if data:
                    datetime_ = data[0]['_id'].generation_time
                    datetime_ = datetime_remove_timezone(datetime_)
                else:
                    self.logger.debug('Collection %s is empty, no last insert datetime', coll)
                break
        else:
            self.logger.error("Couldn't establish connection to mongodb server %s", self.uri)
        return datetime_
