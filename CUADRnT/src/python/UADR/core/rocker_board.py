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

# package modules
from UADR.utils.utils import weighted_choice
from UADR.utils.config import get_config
from UADR.tools.rankings import DeltaRankings
from UADR.tools.storage import StorageManager

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
        self.rankings = DeltaRankings(self.config)
        self.storage = StorageManager(self.config)

    def start(self):
        """
        Begin Rocker Board Algorithm
        """
        self.sites.update_sites()
        self.datasets.update_datasets()
        self.balance()

    def balance(self):
        """
        Balance system by creating new replicas based on popularity
        """
        dataset_rankings = self.rankings.get_dataset_rankings()
        site_rankings = self.rankings.get_site_rankings()

def main(argv):
    """
    Main driver for Rocker Board Algorithm
    """
    log_level = logging.WARNING
    config = get_config()
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: rocker_board.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: rocker_board.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: rocker_board.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: rocker_board.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: rocker_board.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: rocker_board.py --help"
                sys.exit()
        else:
            print "usage: rocker_board.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: rocker_board.py --help"
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
    rocker_board = RockerBoard(config)
    rocker_board.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
