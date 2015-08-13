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
from UADR.utils.config import get_config
from UADR.core.rocker_board import RockerBoard

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

#@unittest.skip("Skip Test")
class ToolsTests(unittest.TestCase):
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
        "test rocker_board functions"
        print ""
        rocker_board = RockerBoard(config=self.config)
        rocker_board.start()

if __name__ == '__main__':
    unittest.main()
