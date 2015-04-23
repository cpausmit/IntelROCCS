#!/usr/local/bin/python
"""
File       : io_utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for I/O utils functions
"""

# system modules
import os
import unittest

# package modules
from UADR.utils.io_utils import get_data_path
from UADR.utils.io_utils import export_csv

class IoUtilsTests(unittest.TestCase):
    """
    A test class for util functions
    """
    def setUp(self):
        "Set up for test"
        pass

    def tearDown(self):
        "Clean up"
        pass

    def test_export_csv(self):
        "Test export_csv function"
        print ""
        file_name = 'test'
        headers = ('foo', 'bar')
        data = [('Bjorn', 'Barrefors')]
        export_csv(file_name, headers, data, debug=1)
        data_path = get_data_path()
        fs = open('%s/%s.csv' % (data_path, file_name), 'r')
        result = fs.read()
        expected = 'foo,bar\nBjorn,Barrefors\n'
        self.assertEqual(result, expected)
        os.remove(file_name)

if __name__ == '__main__':
    unittest.main()
