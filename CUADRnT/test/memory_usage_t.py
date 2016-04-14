#!/usr/bin/env python2.7
"""
File       : memory_usage_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test memory usage of big fetch calls in program
"""

# system modules
import unittest
import os
import datetime
import logging

# package modules
from cuadrnt.data_management.services.phedex import PhEDExService
from cuadrnt.data_management.services.pop_db import PopDBService
from cuadrnt.utils.test_utils import total_size_of
from cuadrnt.utils.config import get_config
from cuadrnt.utils.utils import datetime_to_string
from cuadrnt.utils.utils import datetime_day

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')
logger = logging.getLogger(__name__)

@unittest.skip("Skip Test")
class MemoryTests(unittest.TestCase):
    """
    A class to test memory usage of program
    Specifically large fetches from online services
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_phedex_memory(self):
        "Test phedex data memory usage"
        print ""
        phedex = PhEDExService(self.config)
        api = 'blockreplicas'
        params = [('node', 'T2_*'), ('create_since', 0.0), ('complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        phedex_data = phedex.fetch(api=api, params=params, cache=False)
        total_size = total_size_of(phedex_data)
        logger.info('Total size of PhEDEx data in memory is %d bytes (%dMB)', total_size, total_size/10**6)

    #@unittest.skip("Skip Test")
    def test_pop_db_memory(self):
        "Test pop_db data memory usage"
        print ""
        pop_db = PopDBService(self.config)
        api = 'getDSdata'
        sitename = 'summary'
        aggr = 'day'
        n = 200000
        orderby = 'totcpu'
        tstart = datetime_to_string(datetime_day(datetime.datetime.utcnow() - datetime.timedelta(days=10)))
        tstop = datetime_to_string(datetime_day(datetime.datetime.utcnow()))
        params = {'sitename':sitename, 'tstart':tstart, 'tstop':tstop, 'aggr':aggr, 'n':n, 'orderby':orderby}
        pop_db_data = pop_db.fetch(api=api, params=params, cache=False)
        total_size = total_size_of(pop_db_data)
        logger.info('Total size of Pop DB data in memory is %d bytes (%dMB)', total_size, total_size/10**6)

if __name__ == '__main__':
    unittest.main()
