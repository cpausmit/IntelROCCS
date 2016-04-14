#!/usr/bin/env python2.7
"""
File       : dm_storage_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for storage class
"""

# system modules
import unittest
import os
from datetime import datetime

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.services.phedex import PhEDExService
from cuadrnt.data_management.core.storage import StorageManager

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class StorageTests(unittest.TestCase):
    """
    A test class for service classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')
        self.storage = StorageManager(config=self.config)
        self.storage.drop_db()

    def tearDown(self):
        "Clean up"
        coll = 'test'
        query = dict()
        self.storage.delete_data(coll=coll, query=query)
        pipeline = list()
        match = {'$match':{}}
        pipeline.append(match)
        expected = list()
        result = self.storage.get_data(coll=coll, pipeline=pipeline)
        self.assertEqual(result, expected)
        self.storage.drop_db()

    #@unittest.skip("Skip Test")
    def test_cache(self):
        "Test storage cache"
        print ""
        phedex = PhEDExService(config=self.config)
        api = 'data'
        params = {'level':'block', 'dataset':'/DoubleElectron/Run2012D-22Jan2013-v1/AOD'}
        expected = '/DoubleElectron/Run2012D-22Jan2013-v1/AOD'
        phedex.fetch(api=api, params=params, cache_only=True, force_cache=True)
        cache_data = self.storage.get_cache(coll='phedex', api=api, params=params)
        try:
            result = cache_data['phedex']['dbs'][0]['dataset'][0]['name']
        except KeyError:
            self.assertTrue(False)
        else:
            self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_data(self):
        "Test general collection manipulation functions"
        coll = 'test'
        # insert
        data = [{'foo':'bar_1'}, {'foo':'bar_2'}]
        self.storage.insert_data(coll=coll, data=data)
        # get
        pipeline = list()
        match = {'$match':{'foo':'bar_2'}}
        pipeline.append(match)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        expected = 'bar_2'
        result = data[0]['foo']
        self.assertEqual(result, expected)
        # update
        query = {'foo':'bar_1'}
        data = {'$set':{'foo':'bar_3'}}
        self.storage.update_data(coll=coll, query=query, data=data)
        pipeline = list()
        match = {'$match':{'foo':'bar_3'}}
        pipeline.append(match)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        expected = 'bar_3'
        result = data[0]['foo']
        self.assertEqual(result, expected)
        # last insert timestamp
        data = [{'foo':'bar_4'}]
        datetime_1 = datetime.utcnow().replace(microsecond=0)
        self.storage.insert_data(coll=coll, data=data)
        datetime_2 = self.storage.get_last_insert_time(coll)
        self.assertTrue(datetime_1 <= datetime_2)

if __name__ == '__main__':
    unittest.main()
