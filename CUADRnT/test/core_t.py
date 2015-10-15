#!/usr/bin/env python2.7
"""
File       : core_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for core classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.core.rocker_board import RockerBoard

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

#@unittest.skip("Skip Test")
class CoreTests(unittest.TestCase):
    """
    A test class for core classes
    Not strictly used as unittests as results are not checked here
    but rather used to run program and see if it crashes and check results manually after
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_rocker_board(self):
        "Test rocker_board functions"
        print ""
        rocker_board = RockerBoard(config=self.config)
        subscriptions = list()
        dataset_name = '/BBbarDMJets_scalar_Mchi-1_Mphi-1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/AODSIM'
        site_name = 'T2_US_Wisconsin'
        subscription = (dataset_name, site_name)
        subscriptions.append(subscription)
        rocker_board.subscribe(subscriptions)

if __name__ == '__main__':
    unittest.main()
