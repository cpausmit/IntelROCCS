#!/usr/bin/env python2.7
"""
File       : dm_services_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for service classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.services.dbs import DBSService
from cuadrnt.data_management.services.crab import CRABService
from cuadrnt.data_management.services.intelroccs import IntelROCCSService
from cuadrnt.data_management.services.mit_db import MITDBService
from cuadrnt.data_management.services.phedex import PhEDExService
from cuadrnt.data_management.services.pop_db import PopDBService

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class ServicesTests(unittest.TestCase):
    """
    A test class for service classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_crab(self):
        "Test crab functions"
        print ""
        expected = 'T2_US_Nebraska'
        crab = CRABService(config=self.config)
        query = 'GLIDEIN_CMSSite =?= "%s"' % ('T2_US_Nebraska')
        attributes = ["GLIDEIN_CMSSite"]
        ads = crab.fetch_cluster_ads(query=query, attributes=attributes)
        for ad in ads:
            self.assertEqual(ad['GLIDEIN_CMSSite'], expected)

    #@unittest.skip("Skip Test")
    def test_dbs(self):
        "Test dbs functions"
        print ""
        dbs = DBSService(config=self.config)
        api = 'datasets'
        params = {'dataset':'/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM', 'detail':True}
        expected = '/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM'
        json_data = dbs.fetch(api=api, params=params, cache=False)
        result = json_data['data'][0]['dataset']
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_intelroccs(self):
        "Test intelroccs functions"
        print ""
        intelroccs = IntelROCCSService(config=self.config)
        api = 'Detox'
        file_ = 'SitesInfo.txt'
        json_data = intelroccs.fetch(api=api, params=file_, secure=False, cache=False)
        result = json_data['data']
        self.assertTrue(len(result) > 1)

    # only run this test if machine have access to db
    @unittest.skip("Skip Test")
    def test_mit_db(self):
        "Test mit_db functions"
        print ""
        mit_db = MITDBService(config=self.config)
        query = "SELECT SiteName FROM Sites WHERE SiteName=%s"
        values = ['T2_US_Nebraska']
        expected = 'T2_US_Nebraska'
        json_data = mit_db.fetch(query=query, values=values, cache=False)
        result = json_data['data'][0][0]
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_phedex(self):
        "Test phedex functions"
        print ""
        phedex = PhEDExService(config=self.config)
        api = 'data'
        params = {'level':'block', 'dataset':'/DoubleElectron/Run2012D-22Jan2013-v1/AOD'}
        expected = '/DoubleElectron/Run2012D-22Jan2013-v1/AOD'
        json_data = phedex.fetch(api=api, params=params, cache=False)
        result = json_data['phedex']['dbs'][0]['dataset'][0]['name']
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_pop_db(self):
        "Test pop_db functions"
        print ""
        pop_db = PopDBService(config=self.config)
        api = 'DSStatInTimeWindow/'
        params = {'tstart':'2015-04-18', 'tstop':'2015-04-18', 'sitename':'T2_US_Nebraska'}
        expected = 'T2_US_Nebraska'
        json_data = pop_db.fetch(api=api, params=params, cache=False)
        result = json_data['SITENAME']
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
