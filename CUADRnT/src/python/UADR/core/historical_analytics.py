#!/usr/local/bin/python
"""
File       : historical_analytics.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect historical data for popularity model
"""

# system modules
import os
import sys
import time

# package modules
from UADR.utils.config import get_config
from UADR.utils.utils import pop_db_timestamp_to_timestamp
from UADR.utils.utils import phedex_timestamp_to_timestamp
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import timestamp_day
from UADR.utils.utils import timestamp_to_date
from UADR.services.phedex import PhEDExService
from UADR.services.pop_db import PopDBService

class HUA(object):
    """
    HUA or Historical User Analytics collects historical data on user behavior
    """
    def __init__(self, debug=0):
        self.config = get_config()
        self.phedex = PhEDExService(self.config, debug)
        self.pop_db = PopDBService(self.config, debug)
        self.data_path = '%s/data' % (os.environ.get('CUADRNT_ROOT'))

    def get_datasets(self):
        """
        Get all analysis datasets in CMS
        """
        # NOTE: We would like to get all datasets and have pattern for non analysis datasets
        dataset_names = set()
        api = 'blockReplicas'
        params = {'create_since':0, 'group':'AnalysisOps', 'show_dataset':'y'}
        json_data = self.phedex.fetch(api, params)
        datasets = json_data.get('phedex').get('dataset')
        for dataset in datasets:
            dataset_names.add(dataset.get('name'))
        return dataset_names

    def get_accesses(self, dataset_names):
        """
        Get accesses for all analysis datasets
        """
        # NOTE: For now we only use accesses as CPUH was not working for a while in pop db
        dataset_accesses = dict()
        test_dataset_re = re.compile('^/GenericTTbar/HC-.*/GEN-SIM-RECO$')
        yesterday = timestamp_to_date(time.time()-86400)
        api = 'DSStatInTimeWindow'
        params = {'sitename':'summary', 'tstart':'1999-01-01', 'tstop':yesterday}
        json_data = self.pop_db.fetch(api, params)
        for data in json_data.get('data')[0].get('data'):
            timestamp = pop_db_timestamp_to_timestamp(data[0])
            accesses = data[1]
            tmp_accesses[timestamp] = accesses
            dataset_accesses[dataset_name] = dataset_accesses
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

    def organize_data(self, headers, dataset_names, dataset_accesses, dataset_sizes, dataset_tiers, dataset_dates):
        """
        Organize data into the structure:
            data format: [(header_1, header_2, ...), (data_1, data_2, ...)]
        """
        data = []
        data.append(tuple(headers))
        date_stop = timestamp_day(int(time.time()))
        date_step = 86400
        for dataset_name in dataset_names:
            for timestamp in xrange(dataset_dates[dataset_name], date_stop, date_step):
                try:
                    accesses = dataset_accesses[dataset_name][timestamp]
                except Exception:
                    accesses = 0
                row = (dataset_name, timestamp_to_date(timestamp), accesses, dataset_sizes[dataset_name], dataset_tiers[dataset_name])
                data.append(row)
        return data

    def export(self, data):
        """
        Export data to file using csv format
        data format: [(header_1, header_2, ...), (data_1, data_2, ...)]
        """
        # FIXME: Get file path from config file
        export_file = '%s/historical_analytics.csv' % (self.data_path)
        fs = open(export_file)
        for row in data:
            text_row = ''
            for field in row:
                text_row += '%s,' % (field)
            text_row = text_row[:-1] + '\n'
            fs = open(export_file)
            fs.write(text_row, 'a')

def main():
    """
    Main driver for historical analytics
    """
    headers = ('dataset_name', 'date', 'accesses', 'size', 'data_tier')
    hua = HUA(debug=1)
    dataset_names = hua.get_datasets()
    dataset_accesses = hua.get_accesses(dataset_names)
    dataset_sizes = hua.get_size_gb(dataset_names)
    dataset_dates = hua.get_creation_date(dataset_names)
    dataset_tiers = hua.get_data_tier(dataset_names)
    data = hua.organize_data(headers, dataset_names, dataset_accesses, dataset_sizes, dataset_tiers, dataset_dates)
    hua.export(data)

if __name__ == "__main__":
    main()
    sys.exit(0)
