#!/usr/bin/env python
"""
File       : db_utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for DB utils functions
"""

# system modules
import unittest
from bson.objectid import ObjectId

# package modules
from UADR.utils.db_utils import get_object_id

@unittest.skip("Skipping Test")
class DbUtilsTests(unittest.TestCase):
    """
    A test class for DB util functions
    """
    def setUp(self):
        "Set up for test"
        pass

    def tearDown(self):
        "Clean up"
        pass

    def test_get_object_id(self):
        "Test get_object_id function"
        print ""
        api = 'test'
        params = {'foo':'bar'}
        result = get_object_id(api, params=params)
        expected = ObjectId('8905136bf8601698f98d7f90')
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
