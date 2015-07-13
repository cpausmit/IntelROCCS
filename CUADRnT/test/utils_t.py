#!/usr/bin/env python
"""
File       : utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for all utils functions
"""

# system modules
import unittest
import os
import datetime
from bson.objectid import ObjectId

# package modules
from UADR.utils.config import get_config
from UADR.utils.db_utils import get_object_id
from UADR.utils.db_utils import datetime_to_object_id
from UADR.utils.io_utils import export_csv
from UADR.utils.utils import check_tool
from UADR.utils.utils import weighted_choice
from UADR.utils.utils import daterange
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import datetime_to_timestamp
from UADR.utils.utils import timestamp_to_datetime
from UADR.utils.utils import datetime_day
from UADR.utils.utils import datetime_to_string
from UADR.utils.utils import phedex_timestamp_to_datetime
from UADR.utils.utils import pop_db_timestamp_to_datetime
from UADR.utils.utils import datetime_remove_timezone

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

#@unittest.skip("Skip Test")
class UtilsTests(unittest.TestCase):
    """
    A test class for util functions
    Test web_utils in services test
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='cuadrnt-test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_get_config(self):
        "Test get_config function"
        print ""
        expected = 'bar'
        result = str(self.config['test']['foo'])
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_get_object_id(self):
        "Test get_object_id function"
        print ""
        api = 'test'
        params = {'foo':'bar'}
        expected = ObjectId('09f51db4a7ebda7328d11bd4')
        result = get_object_id(str(api)+str(params))
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_object_id(self):
        "Test get_object_id function"
        print ""
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = ObjectId('2183e2000000000000000000')
        result = datetime_to_object_id(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_export_csv(self):
        "Test export_csv function"
        print ""
        file_name = 'test'
        path = '/var/lib/cuadrnt'
        headers = ('foo', 'bar')
        data = [('Bjorn', 'Barrefors')]
        export_file = '%s/%s.csv' % (path, file_name)
        export_csv(file_name=file_name, path=path, headers=headers, data=data)
        expected = 'foo,bar\nBjorn,Barrefors\n'
        fs = open(export_file, 'r')
        result = fs.read()
        self.assertEqual(result, expected)
        os.remove(export_file)

    #@unittest.skip("Skip Test")
    def test_check_tool(self):
        "Test check_tool function"
        print ""
        tool = 'rm'
        result = check_tool(tool)
        self.assertTrue(result)
        tool = 'invalid_tool'
        result = check_tool(tool)
        self.assertFalse(result)

    #@unittest.skip("Skip Test")
    def test_weighted_choice(self):
        "Test weighted_choice function"
        print ""
        choices = {'foo':1.5, 'bar':5.9}
        expected = choices.keys()
        result = weighted_choice(choices)
        self.assertTrue(result in expected)

    #@unittest.skip("Skip Test")
    def test_daterange(self):
        "Test daterange function"
        print ""
        start_date = datetime.datetime(1987, 10, 27)
        end_date = datetime.datetime(1987, 10, 30)
        expected = [datetime.datetime(1987, 10, 27), datetime.datetime(1987, 10, 28), datetime.datetime(1987, 10, 29)]
        result = daterange(start_date, end_date)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_bytes_to_gb(self):
        "Test bytes_to_gb function"
        print ""
        bytes_ = 146640731779
        expected = 146.640731779
        result = bytes_to_gb(bytes_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_timestamp(self):
        "Test datetime_to_timestamp function"
        print ""
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = 562291200
        result = datetime_to_timestamp(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_timestamp_to_datetime(self):
        "Test timestamp_to_datetime function"
        print ""
        timestamp = 1434989041.102534
        expected = datetime.datetime(2015, 6, 22, 16, 4, 1)
        result = timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_day(self):
        "Test datetime_day function"
        print ""
        datetime_ = datetime.datetime(1987, 10, 27, 3, 6, 9)
        expected = datetime.datetime(year=1987, month=10, day=27)
        result = datetime_day(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_string(self):
        "Test datetime_to_string function"
        print ""
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = '1987-10-27'
        result = datetime_to_string(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_phedex_timestamp_to_datetime(self):
        "Test phedex_timestamp_to_datetime function"
        print ""
        timestamp = 1363976920.60798
        expected = datetime.datetime(2013, 3, 22, 18, 28, 40)
        result = phedex_timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_pop_db_timestamp_to_datetime(self):
        "Test pop_db_timestamp_to_datetime function"
        print ""
        timestamp = 1406246400000
        expected = datetime.datetime(2014, 7, 25, 0, 0)
        result = pop_db_timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_remove_timezone(self):
        "Test datetime_remove_timezone function"
        print ""
        expected = None
        result = datetime_remove_timezone(datetime.datetime.utcnow()).tzinfo
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
