#!/usr/bin/env python2.7
"""
File       : miniaod_analysis.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Try to figure out data access patterns
"""

# system modules
import logging
import sys
import getopt
import datetime
import re
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.tools.storage import StorageManager
#from cuadrnt.services.phedex import PhEDExService

class MiniAODAnalysis(object):
    """
    Data Analysis is collecting data and prints it to be used by visualization
    software to better understand access patterns
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.storage = StorageManager(self.config)
        #self.phedex = self.phedex = PhEDExService(self.config)

    def start(self):
        """
        Begin Data Analysis
        """
        t1 = datetime.datetime.utcnow()
        n_datasets, size_all = self.get_n_datasets()
        print n_datasets
        print size_all
        #n_multiple, size_multiple = self.get_multiple_sites()
        #n_replicas = self.get_n_replicas()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('MINIAOD Analysis took %s', str(td))

    def get_n_datasets(self):
        """
        Collect the total number of MINIAOD[SIM] blocks
        """
        regex = re.compile(".*MINIAOD(SIM)?$")
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':{'$regex':regex}}}
        pipeline.append(match)
        group = {'$group':{'_id':None, 'count':{'$sum':1}, 'size_bytes':{'$sum':'$size_bytes'}}}
        pipeline.append(group)
        data = self.storage.get_data(coll, pipeline)
        n_datasets = data[0]['count']
        size_gb = data[0]['size_bytes']/10**9
        return n_datasets, size_gb

def main(argv):
    """
    Main driver for MINIAOD analysis
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='miniaod_analysis.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: miniaod_analysis.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: miniaod_analysis.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: miniaod_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: miniaod_analysis.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: miniaod_analysis.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: miniaod_analysis.py --help"
                sys.exit()
        else:
            print "usage: miniaod_analysis.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: miniaod_analysis.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'miniaod_analysis.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    miniaod_analysis = MiniAODAnalysis(config)
    miniaod_analysis.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
