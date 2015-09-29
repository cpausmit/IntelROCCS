#!/usr/bin/env python2.7
"""
File       : initiate.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Initiate database
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.sites import SiteManager
from cuadrnt.tools.popularity import PopularityManager

class Initiate(object):
    """
    Initiate Database
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)

    def start(self):
        """
        Begin Initiating Database
        """
        t1 = datetime.datetime.utcnow()
        self.sites.initiate_db()
        self.datasets.initiate_db()
        self.popularity.initiate_db()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Initiate took %s', str(td))

def main(argv):
    """
    Main driver for Initiate
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='initiate.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: initiate.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: initiate.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: initiate.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: initiate.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: initiate.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: initiate.py --help"
                sys.exit()
        else:
            print "usage: initiate.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: initiate.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'initiate.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    initiate = Initiate(config)
    initiate.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
