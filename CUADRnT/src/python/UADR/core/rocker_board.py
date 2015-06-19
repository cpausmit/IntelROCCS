#!/usr/bin/env python
"""
File       : rocker_board.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Collect historical data for popularity model
"""

# system modules
import logging
import sys
import getopt
import re
import threading
import Queue

# package modules
from UADR.utils.config import get_config
from UADR.tools.popularity import Popularity
from UADR.core.storage import StorageManager

MAX_THREADS = 1

class RockerBoard(object):
    """
    RockerBoard is a system balancing algorithm using popularity metrics to predict popularity
    and make appropriate replications to keep the system balanced
    """
    def __init__(self, config=dict()):
        global MAX_THREADS
        self.logger = logging.getLogger(__name__)
        self.config = get_config(config)
        self.phedex = PhEDExService(self.config)
        self.pop_db = PopDBService(self.config)
        self.dbs = DBSService(self.config)
        self.storage = StorageManager(self.config)

    def get_data(self):
        """
        Fetch all data needed to generate popularity metric
        """
        self.get_datasets()
        datasets = self.get_new_datasets()
        q = Queue.Queue()
        for i in range(MAX_THREADS):
            worker = threading.Thread(target=self.data_worker, args=(q,))
            worker.daemon = True
            worker.start()
        for name in datasets:
            q.put(name)
        q.join()

    def get_new_datasets(self):
        """
        Get all analysis datasets in CMS
        """
        object_id = datetime_to_object_id(timestamp_to_datetime(self.last_update))
        pipeline = list()
        match = {'$match':{'_id':{'$gte':object_id}}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll='dataset_data', pipeline=pipeline)
        return [d['name'] for d in data]

    def get_datasets(self):
        """
        Get new datasets in 'AnaysisOps'
        """
        valid_datasets_patterns = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets_patterns = ''
        invalid_file = open('/var/opt/CUADRnT/invalid_datasets_patterns', 'r')
        for pattern in invalid_file:
            invalid_datasets_patterns += pattern.strip() + '|'
        invalid_datasets_patterns = invalid_datasets_patterns[:-1]
        re_valid_datasets = re.compile(valid_datasets_patterns)
        re_invalid_datasets = re.compile(invalid_datasets_patterns)
        # Get date of latest update
        timestamp = datetime_to_timestamp(self.storage.get_last_insert_time('dataset_data'))
        self.last_update = timestamp
        # Get new replicas
        api = 'blockreplicas'
        params = {'node':'T*', 'create_since':timestamp, 'group':'AnalysisOps', 'show_dataset':'y'}
        json_data = self.phedex.fetch(api, params)
        # Index again
        dataset_data = list()
        for dataset in json_data['phedex']['dataset']:
            dataset_name = dataset['name']
            if (re_invalid_datasets.match(dataset_name) or not re_valid_datasets.match(dataset_name)):
                continue
            pipeline = list()
            match = {'$match':{'name':dataset_name}}
            pipeline.append(match)
            if self.storage.get_data('dataset_data', pipeline):
                continue
            dataset_data.append({'name':dataset_name})
        if dataset_data:
            self.storage.insert_data('dataset_data', dataset_data)

    def data_worker(self, q):
        """
        Spawn daemons to fetch online data in the background
        """
        while True:
            dataset_name = q.get()
            self.get_phedex_data(dataset_name)
            self.get_dbs_data(dataset_name)
            q.task_done()

    def get_phedex_data(self, dataset_name):
        """
        Get data from phedex for dataset
        """
        api = 'data'
        params = {'dataset':dataset_name, 'level':'block', 'create_since':1}
        json_data = self.phedex.fetch(api=api, params=params)
        dataset = json_data['phedex']['dbs'][0]['dataset'][0]
        creation_time = phedex_timestamp_to_datetime(dataset['time_create'])
        size_bytes = 0
        n_files = 0
        for block in dataset['block']:
            size_bytes += block['bytes']
            n_files += block['files']
        size_gb = bytes_to_gb(size_bytes)
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'creation_time':creation_time, 'size_gb':size_gb, 'n_files':n_files}}
        data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def get_pop_db_data(self):
        """
        Fetch all data needed to generate popularity metric from services and store in cache
        for quick access later
        """
        # cache blockreplicas, spawn external thread
        valid_datasets_patterns = '/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+/[0-9a-zA-Z\-_]+$'
        invalid_datasets_patterns = ''
        invalid_file = open('/var/opt/CUADRnT/invalid_datasets_patterns', 'r')
        for pattern in invalid_file:
            invalid_datasets_patterns += pattern.strip() + '|'
        invalid_datasets_patterns = invalid_datasets_patterns[:-1]
        re_valid_datasets = re.compile(valid_datasets_patterns)
        re_invalid_datasets = re.compile(invalid_datasets_patterns)
        api = 'DSStatInTimeWindow/'
        params = {'tstart':self.date, 'tstop':self.date, 'sitename':'summary'}
        self.pop_db.fetch(api=api, params=params, cache_only=True)
        object_id = get_object_id(str(api)+str(params))
        match = {'_id':object_id}
        unwind = '$data.DATA'
        project = {'name':'$data.DATA.COLLNAME', 'n_accesses':'$data.DATA.NACC', 'n_cpus':'$data.DATA.TOTCPU', 'n_users':'$data.DATA.NUSERS', '_id':0}
        pipeline = [{'$match':match}, {'$unwind':unwind}, {'$project':project}]
        data = self.storage.get_data(coll='pop_db', pipeline=pipeline)
        n_datasets = 0
        for dataset in data:
            dataset_name = dataset['name']
            if (re_invalid_datasets.match(dataset_name) or not re_valid_datasets.match(dataset_name)):
                continue
            print dataset
            self.storage.insert_data('dataset_data', data=dataset, id_=str(self.date)+str(dataset_name))
            n_datasets += 1
        self.logger.info('Found %d analysis datasets', n_datasets)

def main(argv):
    """
    Main driver for historical data collector
    """
    log_level = logging.WARNING
    config = 'cuadrnt.cfg'
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
    hdc.get_data()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
