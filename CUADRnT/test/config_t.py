#!/usr/local/bin/python
"""
File       : config_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for config functions
"""

# system modules
import unittest

# package modules
from UADR.utils.config import get_config

class ConfigTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        pass

    def tearDown(self):
        "Clean up"
        pass

    def test_get_config(self):
        "Test get_config function"
        print ""
        result = get_config(debug=1)
        self.assertNotEqual(result, dict())

if __name__ == '__main__':
    unittest.main()
