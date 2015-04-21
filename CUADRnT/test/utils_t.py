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
from UADR.utils.utils import pop_db_timestamp_to_timestamp
from UADR.utils.utils import phedex_timestamp_to_timestamp
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import timestamp_day
from UADR.utils.utils import timestamp_to_date

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

    def test_pop_db_timestamp_to_timestamp(self):
        "Test pop_db_timestamp_to_timestamp function"
        print ""
        expected = 1386892800
        result = pop_db_timestamp_to_timestamp(1386892800000)
        self.assertEqual(result, expected)

    def test_phedex_timestamp_to_timestamp(self):
        "Test phedex_timestamp_to_timestamp function"
        print ""
        expected = 1336109098
        result = phedex_timestamp_to_timestamp(1336109098.46633)
        self.assertEqual(result, expected)

    def test_bytes_to_gb(self):
        "Test bytes_to_gb function"
        print ""
        expected = 146
        result = bytes_to_gb(146640731779)
        self.assertEqual(result, expected)

    def test_timestamp_day(self):
        "Test timestamp_day function"
        print ""
        expected = 1383782400
        result = timestamp_day(1383793400)
        self.assertEqual(result, expected)

    def test_timestamp_to_date(self):
        "Test timestamp_to_date function"
        print ""
        expected = '2013-11-07'
        result = timestamp_to_date(1383793400)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
