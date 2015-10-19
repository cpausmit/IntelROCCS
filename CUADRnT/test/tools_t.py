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
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.popularity import PopularityManager
from cuadrnt.rankings.delta import DeltaRanking

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

#@unittest.skip("Skip Test")
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
        # sites = SiteManager(config=self.config)
        # sites.update_db()
        # sites.update_cpu()
        # datasets = DatasetManager(config=self.config)
        # datasets.update_db()
        # print "Number of active sites: ", len(sites.get_active_sites())
        # print "Number of available sites: ", len(sites.get_available_sites())
        # tot_performance = 0.0
        # tot_avail_storage = 0
        # for site in sites.get_available_sites():
        #     tot_performance += sites.get_performance(site)
        #     tot_avail_storage += sites.get_available_storage(site)
        # print "Total performance: ", tot_performance
        # print "Total storage available: ", tot_avail_storage
        # print "Number of datasets in AnalysisOps: ", len(datasets.get_db_datasets())
        # tot_replicas = 0
        # tot_size = 0.0
        # for dataset in datasets.get_db_datasets():
        #     tot_replicas += len(datasets.get_sites(dataset))
        #     tot_size += datasets.get_size(dataset)
        # print "Total number of replicas: ", tot_replicas
        # # print "Total dataset size: %.2fGB" % (tot_size)
        # popularity = PopularityManager(config=self.config)
        # popularity.initiate_db()
        rankings = DeltaRanking(config=self.config)
        dataset_rankings = rankings.dataset_rankings()
        for dataset, rank in dataset_rankings.items():
            print rank, " : ", dataset
        site_rankings = rankings.site_rankings()
        for site, rank in site_rankings.items():
            print rank, " : ", site

if __name__ == '__main__':
    unittest.main()
