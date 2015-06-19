#!/usr/bin/env python
"""
File       : utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for utils functions
"""

# system modules
import unittest

# package modules
from UADR.utils.utils import check_tool
from UADR.utils.utils import bytes_to_gb

@unittest.skip("Skipping Test")
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
        result = check_tool('rm')
        self.assertTrue(result)
        result = check_tool('invalid_tool')
        self.assertFalse(result)

    def test_bytes_to_gb(self):
        "Test bytes_to_gb function"
        print ""
        expected = 146
        result = bytes_to_gb(146640731779)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
