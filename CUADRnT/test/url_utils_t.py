#!/usr/local/bin/python
"""
File       : url_utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for url utils functions
"""

# system modules
import unittest
import json

# package modules
from UADR.utils.url_utils import fetch
from UADR.utils.config import get_config

class UrlUtilsTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config()

    def tearDown(self):
        "Clean up"
        pass

    def test_fetch(self):
        "Test fetch function"
        print ""
        name = 'phedex'
        expected = 'T2_US_Nebraska'
        target_url = self.config['services'][name]
        api = 'blockReplicas'
        params = {'node':'T2_US_Nebraska', 'show_dataset':'n', 'group':'AnalysisOps'}
        data = fetch(target_url, api, params)
        json_data = json.loads(data)
        blocks = json_data.get('phedex').get('block')
        for block in blocks:
            replicas = block.get('replica')
            for replica in replicas:
                result = replica.get('node')
                self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
