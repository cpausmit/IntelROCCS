#!/usr/bin/env python2.7
"""
File       : update_db.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Update database and keep learning
"""

# system modules
import logging
import sys
import getopt
from logging.handlers import TimedRotatingFileHandler

# package modules
from UADR.utils.config import get_config
from UADR.tools.datasets import DatasetManager
from UADR.tools.sites import SiteManager

class UpdateDB(object):
    """
    Update DB with new dataset and site data
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)

    def start(self):
        """
        Begin Database Update
        """
        self.sites.update_db()
        self.datasets.update_db()

def main(argv):
    """
    Main driver for Update DB
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='update_db.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: update_db.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: update_db.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: update_db.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: update_db.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: update_db.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: update_db.py --help"
                sys.exit()
        else:
            print "usage: update_db.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: update_db.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'update_db.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    update_db = UpdateDB(config)
    update_db.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()