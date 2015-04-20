#!/usr/local/bin/python
"""
File       : utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for utils functions
"""

# system modules
import unittest

# package modules
from UADR.utils.utils import check_tool
from UADR.utils.utils import get_key_cert

class UtilsTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        pass

    def tearDown(self):
        "Clean up"
        pass

    def test_check_tool(self):
        "Test check_tool function"
        print ""
        result = check_tool('rm', debug=1)
        self.assertTrue(result)
        result = check_tool('invalid_tool', debug=1)
        self.assertFalse(result)

    def test_get_key_cert(self):
        "Test get_key_cert function"
        print ""
        result_key, result_cert = get_key_cert(debug=1)
        self.assertIsNotNone(result_key)
        self.assertIsNotNone(result_cert)

if __name__ == '__main__':
    unittest.main()
