#!/usr/bin/env python2.7
"""
File       : ml_training.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Initiate database and train algorithms
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from UADR.utils.config import get_config
from UADR.tools.datasets import DatasetManager
from UADR.tools.sites import SiteManager
from UADR.tools.popularity import PopularityManager

class MLTrainer(object):
    """
    Machine Learning Trainer
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)

    def start(self):
        """
        Begin Machine Learning Trainer
        """
        t_start = datetime.datetime.utcnow()
        self.sites.initiate_db()
        self.datasets.initiate_db()
        self.popularity.initiate_db()
        t_stop = datetime.datetime.utcnow()
        tot_time = t_stop - t_start
        print tot_time

def main(argv):
    """
    Main driver for Machine Learning Trainer
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='ml_trainer.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: ml_trainer.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: ml_trainer.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: ml_trainer.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_trainer.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: ml_trainer.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: ml_trainer.py --help"
                sys.exit()
        else:
            print "usage: ml_trainer.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_trainer.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'ml_trainer.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    ml_trainer = MLTrainer(config)
    ml_trainer.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
