#!/usr/local/bin/python
"""
File       : historical_user_analysis.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect historical data for popularity model
"""

# system modules
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
    HUA or Historical User Analysis collects historical data on user behavior
    """
    def __init__(self, debug=0):
        self.debug = debug
        self.config = get_config()
        self.phedex = PhEDExService(self.config, debug)
        self.pop_db = PopDBService(self.config, debug)

    def get_datasets(self):
        """
        Get all analysis datasets in CMS
        """
        # NOTE: We would like to get all datasets and have pattern for non analysis datasets
        dataset_names = set()
        valid_dataset_re = re.compile('/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$')
        test_dataset_re = re.compile('^/GenericTTbar/HC-.*/GEN-SIM-RECO$')
        user_dataset_re = re.compile('^.*/USER$')
        yesterday = timestamp_to_pop_db_utc_date(time.time()-86400)
        api = 'DSStatInTimeWindow'
        params = {'sitename':'summary', 'tstart':'1999-01-01', 'tstop':yesterday}
        json_data = self.pop_db.fetch(api, params)
        for dataset in json_data.get('DATA'):
            dataset_name = dataset.get('COLLNAME')
            if (test_dataset_re.match(dataset_name) or user_dataset_re.match(dataset_name) or not valid_dataset_re.match(dataset_name)):
                continue
            dataset_names.add(dataset_name)
        print len(dataset_names)
        return dataset_names

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
            size_gb = 0
            params = {'dataset':dataset_name, 'level':'block', 'create_since':0}
            json_data = self.phedex.fetch(api, params)
            dataset = json_data.get('phedex').get('dataset')[0]
            for block in dataset.get('block'):
                size_gb += bytes_to_gb(block.get('bytes'))
            dataset_sizes[dataset_name] = size_gb
        return dataset_sizes

    def get_creation_date(self, dataset_names):
        """
        Get dataset size in GB for datasets
        """
        dataset_dates = dict()
        api = 'data'
        for dataset_name in dataset_names:
            params = {'dataset':dataset_name, 'level':'block', 'create_since':0}
            json_data = self.phedex.fetch(api, params)
            dataset = json_data.get('phedex').get('dataset')[0]
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

    def organize_data(self, dataset_names, dataset_accesses, dataset_sizes, dataset_tiers, dataset_dates):
        """
        Organize data into the structure:
            data format: [(header_1, header_2, ...), (data_1, data_2, ...)]
        """
        data = []
        date_stop = timestamp_day(int(time.time()))
        date_step = 86400
        for dataset_name in dataset_names:
            for timestamp in xrange(dataset_dates[dataset_name], date_stop, date_step):
                try:
                    accesses = dataset_accesses[dataset_name][timestamp]
                except Exception:
                    accesses = 0
                row = (dataset_name, timestamp_to_utc_date(timestamp), accesses, dataset_sizes[dataset_name], dataset_tiers[dataset_name])
                data.append(row)
        return data

def main(argv):
    """
    Main driver for historical analytics
    """
    debug = 0
    try:
        opts, args = getopt.getopt(argv, 'hd', ['help', 'debug'])
    except getopt.GetoptError:
        print "historical_analytics.py [--debug]"
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "historical_analytics.py [--debug]"
            sys.exit()
        elif opt in ('-d', '--debug'):
            debug = 1
    headers = ('dataset_name', 'date', 'accesses', 'size_gb', 'data_tier')
    hua = HUA(debug=debug)
    dataset_names = hua.get_datasets()
    dataset_accesses = hua.get_accesses(dataset_names)
    #dataset_sizes = hua.get_size_gb(dataset_names)
    #dataset_dates = hua.get_creation_date(dataset_names)
    #dataset_tiers = hua.get_data_tier(dataset_names)
    #data = hua.organize_data(dataset_names, dataset_accesses, dataset_sizes, dataset_tiers, dataset_dates)
    #hua.export_csv(file_name, headers, data, debug=self.debug)

if __name__ == "__main__":
    # TODO: Add support to pass debug parameter
    main(sys.argv[1:])
    sys.exit()
