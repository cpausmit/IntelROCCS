#!/usr/bin/env python2.7
"""
File       : utils_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for all utils functions
"""

# system modules
import unittest
import os
import json
import datetime
from bson.objectid import ObjectId

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.utils.db_utils import get_object_id
from cuadrnt.utils.db_utils import datetime_to_object_id
from cuadrnt.utils.io_utils import export_csv
from cuadrnt.utils.io_utils import export_json
from cuadrnt.utils.utils import check_tool
from cuadrnt.utils.utils import weighted_choice
from cuadrnt.utils.utils import daterange
from cuadrnt.utils.utils import bytes_to_gb
from cuadrnt.utils.utils import datetime_to_timestamp
from cuadrnt.utils.utils import timestamp_to_datetime
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.utils import datetime_to_string
from cuadrnt.utils.utils import phedex_timestamp_to_datetime
from cuadrnt.utils.utils import pop_db_timestamp_to_datetime
from cuadrnt.utils.utils import datetime_remove_timezone
from cuadrnt.utils.utils import get_json

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class UtilsTests(unittest.TestCase):
    """
    A test class for util functions
    Test web_utils in services test
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_get_config(self):
        "Test get_config function"
        expected = 'bar'
        result = str(self.config['test']['foo'])
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_get_object_id(self):
        "Test get_object_id function"
        api = 'test'
        params = {'foo':'bar'}
        expected = ObjectId('09f51db4a7ebda7328d11bd4')
        result = get_object_id(str(api)+str(params))
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_object_id(self):
        "Test get_object_id function"
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = ObjectId('2183e2000000000000000000')
        result = datetime_to_object_id(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_export_csv(self):
        "Test export_csv function"
        file_name = 'test'
        path = '/var/lib/cuadrnt'
        headers = ('foo', 'bar')
        data = [('Bjorn', 'Barrefors')]
        export_file = '%s/%s.csv' % (path, file_name)
        export_csv(headers=headers, data=data, path=path, file_name=file_name)
        expected = 'foo,bar\nBjorn,Barrefors\n'
        fd = open(export_file, 'r')
        result = fd.read()
        fd.close()
        self.assertEqual(result, expected)
        os.remove(export_file)

    #@unittest.skip("Skip Test")
    def test_export_json(self):
        "Test export_json function"
        file_name = 'test'
        path = '/var/lib/cuadrnt'
        data = [{'foo':'bar'}]
        export_file = '%s/%s.json' % (path, file_name)
        export_json(data=data, path=path, file_name=file_name)
        expected = 'bar'
        fd = open(export_file, 'r')
        result = json.load(fd)
        fd.close()
        self.assertEqual(result['foo'], expected)
        os.remove(export_file)

    #@unittest.skip("Skip Test")
    def test_check_tool(self):
        "Test check_tool function"
        tool = 'rm'
        result = check_tool(tool)
        self.assertTrue(result)
        tool = 'invalid_tool'
        result = check_tool(tool)
        self.assertFalse(result)

    #@unittest.skip("Skip Test")
    def test_weighted_choice(self):
        "Test weighted_choice function"
        choices = {'foo':1.5, 'bar':5.9}
        expected = choices.keys()
        result = weighted_choice(choices)
        self.assertTrue(result in expected)

    #@unittest.skip("Skip Test")
    def test_daterange(self):
        "Test daterange function"
        start_date = datetime.datetime(1987, 10, 27)
        end_date = datetime.datetime(1987, 10, 30)
        expected = [datetime.datetime(1987, 10, 27), datetime.datetime(1987, 10, 28), datetime.datetime(1987, 10, 29)]
        results = daterange(start_date, end_date)
        i = 0
        for result in results:
            self.assertEqual(result, expected[i])
            i += 1

    #@unittest.skip("Skip Test")
    def test_bytes_to_gb(self):
        "Test bytes_to_gb function"
        bytes_ = 146640731779
        expected = 146.640731779
        result = bytes_to_gb(bytes_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_timestamp(self):
        "Test datetime_to_timestamp function"
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = 562291200
        result = datetime_to_timestamp(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_timestamp_to_datetime(self):
        "Test timestamp_to_datetime function"
        timestamp = 1434989041.102534
        expected = datetime.datetime(2015, 6, 22, 16, 4, 1)
        result = timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_day(self):
        "Test datetime_day function"
        datetime_ = datetime.datetime(1987, 10, 27, 3, 6, 9)
        expected = datetime.datetime(year=1987, month=10, day=27)
        result = datetime_day(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_to_string(self):
        "Test datetime_to_string function"
        datetime_ = datetime.datetime(1987, 10, 27)
        expected = '1987-10-27'
        result = datetime_to_string(datetime_)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_phedex_timestamp_to_datetime(self):
        "Test phedex_timestamp_to_datetime function"
        timestamp = 1363976920.60798
        expected = datetime.datetime(2013, 3, 22, 18, 28, 40)
        result = phedex_timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_pop_db_timestamp_to_datetime(self):
        "Test pop_db_timestamp_to_datetime function"
        timestamp = 1406246400000
        expected = datetime.datetime(2014, 7, 25, 0, 0)
        result = pop_db_timestamp_to_datetime(timestamp)
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_datetime_remove_timezone(self):
        "Test datetime_remove_timezone function"
        expected = None
        result = datetime_remove_timezone(datetime.datetime.utcnow()).tzinfo
        self.assertEqual(result, expected)

    #@unittest.skip("Skip Test")
    def test_get_json(self):
        "Test get_json function"
        json_data = {'foo':[{'bar':1}, {'bar':2}]}
        field = 'foobar'
        expected = list()
        result = get_json(json_data, field)
        self.assertEqual(result, expected)
        field = 'foo'
        expected = [{'bar':1}, {'bar':2}]
        result = get_json(json_data, field)

if __name__ == '__main__':
    unittest.main()
