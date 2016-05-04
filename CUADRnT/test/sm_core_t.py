#!/usr/bin/env python2.7
"""
File       : sm_core_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for tool classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.system_management.core.rocker_board import RockerBoard

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

#@unittest.skip("Skip Test")
class CoreTests(unittest.TestCase):
    """
    A test class for core classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_core(self):
        "Test Core"
        rocker_board = RockerBoard(config=self.config)
        rocker_board.start()

if __name__ == '__main__':
    unittest.main()
