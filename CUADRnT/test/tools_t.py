#!/usr/bin/env python2.7
"""
File       : tools_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for tool classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.tools.sites import SiteManager
#from cuadrnt.tools.datasets import DatasetManager

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class ToolsTests(unittest.TestCase):
    """
    A test class for tools classes
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
    def test_managers(self):
        "Test managers"
        print ""
        sites = SiteManager(config=self.config)
        #datasets = DatasetManager(config=self.config)
        #sites.update_db()
        #datasets.update_db()
        sites.update_cpu()
        available_sites = sites.get_available_sites()
        for site_name in available_sites:
            performance = sites.get_performance(site_name)
            print site_name, " : ", performance

if __name__ == '__main__':
    unittest.main()
