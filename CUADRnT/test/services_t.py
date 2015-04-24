#!/usr/local/bin/python
"""
File       : services_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for service classes
"""

# system modules
import unittest

# package modules
from UADR.services.pop_db import PopDBService
from UADR.services.phedex import PhEDExService
from UADR.utils.config import get_config

class SSOUtilsTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config()

    def tearDown(self):
        "Clean up"
        pass

    def test_pop_db(self):
        "test pop_db functions"
        print ""
        expected = 'T2_US_Nebraska'
        pop_db = PopDBService(config=self.config)
        api = 'DSStatInTimeWindow'
        params = {'tstart':'2015-04-18', 'tstop':'2015-04-18', 'sitename':'T2_US_Nebraska'}
        json_data = pop_db.fetch(api, params, cache=False)
        result = json_data.get('SITENAME')
        self.assertEqual(result, expected)

    def test_phedex(self):
        "test phedex functions"
        print ""
        expected = 'T2_US_Nebraska'
        phedex = PhEDExService(config=self.config)
        api = 'blockReplicas'
        params = {'node':'T2_US_Nebraska', 'show_dataset':'n', 'group':'AnalysisOps'}
        json_data = phedex.fetch(api, params, cache=False)
        blocks = json_data.get('phedex').get('block')
        for block in blocks:
            replicas = block.get('replica')
            for replica in replicas:
                result = replica.get('node')
                self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
