#!/usr/local/bin/python
"""
File       : historical_usage_analysis.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect historical data for popularity model
"""

# system modules
import os
import datetime
import logging
import sys
import getopt
import re
import time

# package modules
from UADR.utils.config import get_config
from UADR.utils.utils import pop_db_timestamp_to_timestamp
from UADR.utils.utils import phedex_timestamp_to_timestamp
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import timestamp_day
from UADR.utils.utils import timestamp_to_utc_date
from UADR.utils.utils import timestamp_to_pop_db_utc_date
from UADR.services.phedex import PhEDExService
from UADR.services.pop_db import PopDBService
from UADR.utils.io_utils import export_csv

class HUA(object):
    """
    HUA or Historical Usage Analysis collects historical data on user behavior
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.phedex = PhEDExService(self.config)
        self.pop_db = PopDBService(self.config)

    def get_datasets(self):
        """
        Get all analysis datasets in CMS
        """
        # NOTE: We would like to get all datasets and have pattern for non analysis datasets
        dataset_names = set()
        valid_datasets = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets = ''
        invalid_file = open('%s/etc/re_invalid_datasets' % (os.environ['CUADRNT_ROOT']), 'r')
        for pattern in invalid_file:
            invalid_datasets += pattern + '|'
        invalid_datasets = invalid_datasets[:-1]
        valid_dataset_re = re.compile(valid_datasets)
        invalid_dataset_re = re.compile(invalid_datasets)
        yesterday = timestamp_to_pop_db_utc_date(time.time()-86400)
        api = 'DSStatInTimeWindow'
        params = {'sitename':'summary', 'tstart':'1999-01-01', 'tstop':yesterday}
        json_data = self.pop_db.fetch(api, params)
        for dataset in json_data.get('DATA'):
            dataset_name = dataset.get('COLLNAME')
            if (invalid_dataset_re.match(dataset_name) or not valid_dataset_re.match(dataset_name)):
                continue
            dataset_names.add(dataset_name)
        print len(dataset_names)
        return list(dataset_names)[:5]

    def get_accesses(self, dataset_names):
        """
        Get accesses for all analysis datasets
        """
        # NOTE: For now we only use accesses as CPUH was not working for a while in pop db
        # NOTE: Run code in parallel
        dataset_accesses = dict()
        api = 'getSingleDSstat'
        for dataset_name in dataset_names:
            tmp_accesses = dict()
            params = {'sitename':'summary', 'aggr':'day', 'orderby':'naccess', 'name':dataset_name}
            json_data = self.pop_db.fetch(api, params)
            for data in json_data.get('data')[0].get('data'):
                timestamp = pop_db_timestamp_to_timestamp(data[0])
                accesses = data[1]
                tmp_accesses[timestamp] = accesses
            dataset_accesses[dataset_name] = tmp_accesses
        return dataset_accesses

    def get_size_gb(self, dataset_names):
        """
        Get dataset size in GB for datasets
        """
        dataset_sizes = dict()
        api = 'data'
        for dataset_name in dataset_names:
            size_b = 0
            params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
            json_data = self.phedex.fetch(api, params)
            dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0]
            for block in dataset.get('block'):
                size_b += block.get('bytes')
            dataset_sizes[dataset_name] = bytes_to_gb(size_b)
        return dataset_sizes

    def get_creation_date(self, dataset_names):
        """
        Get dataset size in GB for datasets
        """
        dataset_dates = dict()
        api = 'data'
        for dataset_name in dataset_names:
            params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
            json_data = self.phedex.fetch(api, params)
            dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0]
            dataset_dates[dataset_name] = phedex_timestamp_to_timestamp(dataset.get('time_create'))
        return dataset_dates

    def get_data_tier(self, dataset_names):
        """
        Get data tier of datasets
        """
        dataset_tiers = dict()
        for dataset_name in dataset_names:
            dataset_tiers[dataset_name] = dataset_name.split('/')[-1]
        return dataset_tiers

    def organize_data(self, dataset_names, dataset_dates, dataset_accesses, dataset_sizes, dataset_tiers):
        """
        Organize data into the structure:
            data format: [(header_1, header_2, ...), (data_1, data_2, ...)]
        """
        data = []
        timestamp_stop = timestamp_day(int(time.time()))
        step = 86400
        for dataset_name in dataset_names:
            timestamp_start = timestamp_day(dataset_dates[dataset_name])
            for timestamp in xrange(timestamp_start, timestamp_stop, step):
                try:
                    accesses = dataset_accesses[dataset_name][timestamp]
                except Exception:
                    accesses = 0
                row = (dataset_name, timestamp_to_utc_date(timestamp), accesses, dataset_sizes[dataset_name], dataset_tiers[dataset_name])
                data.append(row)
        return data

def main(argv):
    """
    Main driver for historical usage analysis
    """
    log_level = logging.WARNING
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: historical_usage_analysis.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: historical_usage_analysis.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: historical_usage_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: historical_usage_analysis.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: historical_usage_analysis.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: historical_usage_analysis.py --help"
                sys.exit()
        else:
            print "usage: historical_usage_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: historical_usage_analysis.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    logging.basicConfig(filename='%s/log/cuadrnt-%s.log' % (os.environ['CUADRNT_ROOT'], datetime.date.today().strftime('%Y%m%d')), format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M', level=log_level)
    file_name = 'hua'
    headers = ('dataset_name', 'date', 'accesses', 'size_gb', 'data_tier')
    hua = HUA()
    dataset_names = hua.get_datasets()
    dataset_accesses = hua.get_accesses(dataset_names)
    dataset_sizes = hua.get_size_gb(dataset_names)
    dataset_dates = hua.get_creation_date(dataset_names)
    dataset_tiers = hua.get_data_tier(dataset_names)
    data = hua.organize_data(dataset_names, dataset_dates, dataset_accesses, dataset_sizes, dataset_tiers)
    export_csv(file_name, headers, data)

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
