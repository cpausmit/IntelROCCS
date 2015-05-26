#!/usr/local/bin/python
"""
File       : historical_data_collector.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect historical data for popularity model
"""

# system modules
import logging
import sys
import getopt
import re
import time
import threading
import Queue

# package modules
from UADR.utils.config import get_config
from UADR.utils.utils import pop_db_timestamp_to_timestamp
from UADR.utils.utils import phedex_timestamp_to_timestamp
from UADR.utils.utils import bytes_to_gb
from UADR.utils.utils import timestamp_day
from UADR.utils.utils import timestamp_to_pop_db_utc_date
from UADR.services.phedex import PhEDExService
from UADR.services.pop_db import PopDBService
from UADR.utils.io_utils import export_csv

MAX_THREADS = 1

class HDC(object):
    """
    HDC or Historical Data Collector collects historical data on user behavior
    """
    def __init__(self):
        global MAX_THREADS
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.phedex = PhEDExService(self.config)
        self.pop_db = PopDBService(self.config)
        self.dataset_names = list()
        self.dataset_accesses = dict()
        self.dataset_sizes = dict()
        self.dataset_dates = dict()
        self.dataset_tiers = dict()
        MAX_THREADS = int(self.config['threading']['max_threads'])
        self.logger.info('Maximum number of threads: %d', MAX_THREADS)

    def get_dataset_names(self):
        """
        Get all analysis datasets in CMS
        """
        valid_datasets = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets = ''
        invalid_file = open('/var/opt/CUADRnT/re_invalid_datasets', 'r')
        for pattern in invalid_file:
            invalid_datasets += pattern + '|'
        invalid_datasets = invalid_datasets[:-1]
        valid_dataset_re = re.compile(valid_datasets)
        invalid_dataset_re = re.compile(invalid_datasets)
        yesterday = timestamp_to_pop_db_utc_date(time.time()-86400)
        api = 'DSStatInTimeWindow/'
        params = {'sitename':'summary', 'tstart':'1999-01-01', 'tstop':yesterday}
        json_data = self.pop_db.fetch(api, params)
        for dataset in json_data.get('DATA'):
            dataset_name = dataset.get('COLLNAME')
            if (invalid_dataset_re.match(dataset_name) or not valid_dataset_re.match(dataset_name)):
                continue
            self.dataset_names.append(dataset_name)
        self.logger.info('Found %d analysis datasets', len(self.dataset_names))

    def get_accesses(self, dataset_name):
        """
        Get accesses for all analysis datasets
        """
        # Notice that dictionaries are thread safe in Python
        tmp_accesses = dict()
        api = 'getSingleDSstat'
        params = {'sitename':'summary', 'aggr':'day', 'orderby':'naccess', 'name':dataset_name}
        json_data = self.pop_db.fetch(api, params)
        try:
            pop_db_data = json_data.get('data')[0].get('data')
        except Exception:
            self.dataset_names.remove(dataset_name)
            self.logger.warning('No data returned for %s', dataset_name)
            return 1
        else:
            for data in pop_db_data:
                timestamp = pop_db_timestamp_to_timestamp(data[0])
                accesses = data[1]
                tmp_accesses[timestamp] = accesses
            self.dataset_accesses[dataset_name] = tmp_accesses
        return 0

    def get_size_gb(self, dataset_name):
        """
        Get dataset size in GB for datasets
        """
        api = 'data'
        size_b = 0
        params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
        json_data = self.phedex.fetch(api, params)
        try:
            dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0]
            blocks = dataset.get('block')
        except Exception:
            self.dataset_names.remove(dataset_name)
            self.logger.warning('No data returned for %s', dataset_name)
            return 1
        else:
            for block in blocks:
                size_b += block.get('bytes')
            self.dataset_sizes[dataset_name] = bytes_to_gb(size_b)
        return 0

    def get_creation_date(self, dataset_name):
        """
        Get dataset size in GB for datasets
        """
        api = 'data'
        params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
        json_data = self.phedex.fetch(api, params)
        try:
            dataset = json_data.get('phedex').get('dbs')[0].get('dataset')[0]
            timestamp = dataset.get('time_create')
        except Exception:
            self.dataset_names.remove(dataset_name)
            self.logger.warning('No data returned for %s', dataset_name)
            return 1
        else:
            self.dataset_dates[dataset_name] = phedex_timestamp_to_timestamp(timestamp)
        return 0

    def get_data_tier(self, dataset_name):
        """
        Get data tier of datasets
        """
        self.dataset_tiers[dataset_name] = dataset_name.split('/')[-1]
        return 0

    def organize_data(self):
        """
        Organize data into the structure:
            data format: [(header_1, header_2, ...), (data_1, data_2, ...)]
        """
        data = []
        timestamp_stop = timestamp_day(int(time.time()))
        step = 86400
        for dataset_name in self.dataset_names:
            timestamp_start = timestamp_day(self.dataset_dates[dataset_name])
            age = 0
            for timestamp in xrange(timestamp_start, timestamp_stop, step):
                try:
                    accesses = self.dataset_accesses[dataset_name][timestamp]
                except Exception:
                    accesses = 0
                row = (dataset_name, age, accesses, self.dataset_sizes[dataset_name], self.dataset_tiers[dataset_name])
                data.append(row)
                age += 1
        return data

    def get_data(self, q):
        while True:
            dataset_name = q.get()
            if self.get_accesses(dataset_name):
                pass
            elif self.get_size_gb(dataset_name):
                pass
            elif self.get_creation_date(dataset_name):
                pass
            elif self.get_data_tier(dataset_name):
                pass
            q.task_done()

def main(argv):
    """
    Main driver for historical data collector
    """
    log_level = logging.WARNING
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: historical_data_collector.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: historical_data_collector.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: historical_data_collector.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: historical_data_collector.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: historical_data_collector.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: historical_data_collector.py --help"
                sys.exit()
        else:
            print "usage: historical_data_collector.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: historical_data_collector.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    logging.basicConfig(filename='/var/log/CUADRnT/cuadrnt.log', format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M', level=log_level)
    hdc = HDC()
    hdc.get_dataset_names()

    q = Queue.Queue()
    for i in range(MAX_THREADS):
        worker = threading.Thread(target=hdc.get_data, args=(q,))
        worker.daemon = True
        worker.start()
    for dataset_name in hdc.dataset_names:
        q.put(dataset_name)
    q.join()

    data = hdc.organize_data()
    file_name = 'hdc'
    headers = ('dataset_name', 'age', 'accesses', 'size_gb', 'data_tier')
    export_csv(file_name, headers, data)

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
