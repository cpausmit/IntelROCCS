#!/usr/bin/env python2.7
"""
File       : da_rankings_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for tool classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_analysis.rankings.delta import DeltaRanking

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class RankingsTests(unittest.TestCase):
    """
    A test class for rankings classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_managers(self):
        "Test managers"
        delta = DeltaRanking(config=self.config)
        delta.get_dataset_rankings()
        delta.get_site_rankings()
        subscriptions = []
        delta.get_site_storage_rankings(subscriptions)

if __name__ == '__main__':
    unittest.main()
