#!/usr/bin/env python2.7
"""
File       : data_analysis.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Try to figure out data access patterns
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.io_utils import export_csv
from cuadrnt.utils.config import get_config
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.sites import SiteManager
from cuadrnt.tools.storage import StorageManager
from cuadrnt.tools.popularity import PopularityManager

class DataAnalysis(object):
    """
    Data Analysis is collecting data and prints it to be used by visualization
    software to better understand access patterns
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.datasets = DatasetManager(self.config)
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)
        self.popularity = PopularityManager(self.config)

    def start(self):
        """
        Begin Data Analysis
        """
        t1 = datetime.datetime.utcnow()
        dataset_name = '/PAHighPt/HIRun2013-PromptReco-v1/RECO'
        self.initiate_data(dataset_name)
        self.export_data(dataset_name)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Data Analysis took %s', str(td))

    def initiate_data(self, dataset_name):
        """
        Initiate data about dataset(s)
        """
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'name':dataset_name}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        self.datasets.insert_phedex_data(dataset_name)
        self.datasets.insert_dbs_data(dataset_name)
        self.popularity.insert_dataset(dataset_name)

    def export_data(self, dataset_name):
        """
        Get data from DB and export to file for usage in visualization
        """
        # get data from DB
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        db_data = self.storage.get_data(coll=coll, pipeline=pipeline)
        headers = ('dataset_name', 'date', 'popularity')
        data = list()
        for data_entry in db_data:
            data.append(tuple(data_entry['name'], data_entry['date'], data_entry['n_accesses']*data_entry['n_cpus']*data_entry['n_users']))
        export_csv(headers=headers, data=data, file_name='single_dataset')

def main(argv):
    """
    Main driver for Data Analysis
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='data_analysis.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: data_analysis.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: data_analysis.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: data_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: data_analysis.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: data_analysis.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: data_analysis.py --help"
                sys.exit()
        else:
            print "usage: data_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: data_analysis.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'data_analysis.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    data_analysis = DataAnalysis(config)
    data_analysis.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
