#!/usr/bin/env python
"""
File       : web_utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for I/O utils functions
"""

# system modules
import unittest

# package modules
from UADR.utils.web_utils import get_secure_data
from UADR.utils.web_utils import get_data

@unittest.skip("Skipping Test")
class WebUtilsTests(unittest.TestCase):
    """
    A test class for web util functions
    """
    def setUp(self):
        "Set up for test"
        pass

    def tearDown(self):
        "Clean up"
        pass

    def test_get_data(self):
        "Test get_data function"
        print ""
        get_data()

    def test_get_secure_data(self):
        "Test get_data function"
        print ""
        get_secure_data()

if __name__ == '__main__':
    unittest.main()
