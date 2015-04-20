#!/usr/local/bin/python
"""
File       : sso_utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for sso_utils functions
"""

# system modules
import os
import json
import unittest

# package modules
from UADR.utils.sso_utils import get_sso_cookie
from UADR.utils.sso_utils import sso_fetch
from UADR.utils.config import get_config

class SSOUtilsTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config()

    def tearDown(self):
        "Clean up"
        pass

    def test_get_sso_cookie(self):
        "test check_cookie function"
        print ""
        cookie_path = os.path.join(os.environ['HOME'], '.globus')
        target_url = self.config['services']['pop_db']
        get_sso_cookie(cookie_path, target_url, debug=1)
        expected = os.path.exists(os.path.join(cookie_path, 'cern_sso_cookie'))
        self.assertTrue(expected)

    def test_sso_fetch(self):
        "test check_cookie function"
        print ""
        expected = 'T2_US_Nebraska'
        cookie_path = os.path.join(os.environ['HOME'], '.globus')
        target_url = self.config['services']['pop_db']
        api = 'DSStatInTimeWindow'
        params = {'tstart':'2015-04-18', 'tstop':'2015-04-18', 'sitename':'T2_US_Nebraska'}
        data = sso_fetch(cookie_path, target_url, api, params, debug=1)
        json_data = json.loads(data)
        result = json_data.get('SITENAME')
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
