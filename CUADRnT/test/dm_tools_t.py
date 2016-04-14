#!/usr/bin/env python2.7
"""
File       : dm_tools_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for tool classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class ToolsTests(unittest.TestCase):
    """
    A test class for tools classes
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
        # sites = SiteManager(config=self.config)
        # sites.initiate_db()
        # sites.update_db()
        # sites.update_cpu()
        datasets = DatasetManager(config=self.config)
        # datasets.initiate_db()
        # datasets.update_db()
        replicas = datasets.get_current_num_replicas()
        # print "Number of active sites: ", len(sites.get_active_sites())
        # print "Number of available sites: ", len(sites.get_available_sites())
        # tot_performance = 0.0
        # tot_avail_storage = 0
        # tot_over_soft = 0
        # tot_cpus = 0
        # for site in sites.get_available_sites():
        #     tot_performance += sites.get_performance(site)
        #     tot_avail_storage += sites.get_available_storage(site)
        #     tot_over_soft += sites.get_over_soft_limit(site)
        #     tot_cpus += sites.get_max_cpu(site)
        # print "Total performance: ", tot_performance
        # print "Total storage available: ", tot_avail_storage
        # print "Total over soft limit: ", tot_over_soft
        # print "Total CPUs: ", tot_cpus
        # print "Number of datasets in AnalysisOps: ", len(datasets.get_db_datasets())
        # tot_replicas = 0
        # tot_size = 0.0
        # for dataset in datasets.get_db_datasets():
        #     tot_replicas += len(datasets.get_sites(dataset))
        #     tot_size += datasets.get_size(dataset)
        # print "Total number of replicas: ", tot_replicas
        # print "Total dataset size: %.2fGB" % (tot_size)
        # popularity = PopularityManager(config=self.config)
        # popularity.initiate_db()
        # popularity.update_db()

if __name__ == '__main__':
    unittest.main()
