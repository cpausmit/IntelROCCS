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

    logging.basicConfig(filename='/var/log/cuadrnt/cuadrnt.log', format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M', level=log_level)
    # self.logger = logging.getLogger()
    # self.logger.setLevel(logging.DEBUG)
    # handler = logging.handlers.RotatingFileHandler('/var/log/CUADRnT/cuadrnt-test.log', mode='w', maxBytes=10*1024*1024, backupCount=2)
    # handler.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    # handler.setFormatter(formatter)
    # self.logger.addHandler(handler)
    data_analysis = DataAnalysis(config)
    data_analysis.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
