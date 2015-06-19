#!/usr/bin/env python
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
from UADR.services.dbs import DBSService
from UADR.services.mit_db import MITDBService
from UADR.utils.config import get_config

@unittest.skip("Skipping Test")
class ServicesTests(unittest.TestCase):
    """
    A test class for service classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(config='cudrnt-test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    @unittest.skip("Skipping Test")
    def test_pop_db(self):
        "test pop_db functions"
        print ""
        expected = 'T2_US_Nebraska'
        pop_db = PopDBService(config=self.config)
        api = 'DSStatInTimeWindow/'
        params = {'tstart':'2015-04-18', 'tstop':'2015-04-18', 'sitename':'T2_US_Nebraska'}
        pop_db.fetch(api, params, cache_only=True)
        print "Done with caching"
        json_data = pop_db.fetch(api, params)
        result = json_data.get('SITENAME')
        self.assertEqual(result, expected)

    @unittest.skip("Skipping Test")
    def test_phedex(self):
        "test phedex functions"
        print ""
        expected = 'T2_US_Nebraska'
        phedex = PhEDExService(config=self.config)
        api = 'blockReplicas'
        params = {'node':'T2_US_Nebraska', 'show_dataset':'n', 'group':'AnalysisOps'}
        phedex.fetch(api, params, cache_only=True)
        print "Done with caching"
        json_data = phedex.fetch(api, params)
        blocks = json_data.get('phedex').get('block')
        for block in blocks:
            replicas = block.get('replica')
            for replica in replicas:
                result = replica.get('node')
                self.assertEqual(result, expected)

    def test_dbs(self):
        "test dbs functions"
        print ""
        expected = '/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM'
        dbs = DBSService(config=self.config)
        api = 'datasets'
        params = {'dataset':'/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM', 'detail':True}
        dbs.fetch(api, params, method='get', cache_only=True)
        print "Done with caching"
        json_data = dbs.fetch(api, params, method='get')
        result = json_data[0].get('dataset')
        self.assertEqual(result, expected)

    @unittest.skip("Skipping Test")
    def test_mit_db(self):
        "test mit_db functions"
        print ""
        mit_db = MITDBService(config=self.config)
        query = "SELECT Sites.SiteName FROM Sites"
        values = []
        data = mit_db.fetch(query, values, cache=False)
        print data

if __name__ == '__main__':
    unittest.main()
