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

# package modules
from UADR.utils.config import get_config
from UADR.tools.datasets import DatasetManager
from UADR.tools.sites import SiteManager
from UADR.tools.storage import StorageManager
from logging.handlers import TimedRotatingFileHandler

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

    def start(self):
        """
        Begin Data Analysis
        """
        dataset_name = '/PAHighPt/HIRun2013-PromptReco-v1/RECO'

def main(argv):
    """
    Main driver for Data Analysis
    """
    log_level = logging.WARNING
    config = get_config()
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
    log_file = 'rocker_board.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger(__name__)
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
