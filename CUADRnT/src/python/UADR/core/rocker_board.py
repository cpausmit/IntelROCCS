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
from UADR.services.intelroccs import IntelROCCSService
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
        self.intelroccs = IntelROCCSService(self.config)
        self.popularity = Popularity(self.config)
        self.storage = StorageManager(self.config)

    def start(self):
        """
        Begin Rocker Board Algorithm
        """
        self.popularity.get_data()
        self.balance()

    def weightedChoice(self, choices):
        """
        Do a weighted random selection
        """
        total = sum(w for c, w in choices.items())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.items():
            if upto + w > r:
                return c
            upto += w

    def get_site_popularity(self):
        """
        Generate site popularity based on dataset popularity and replicas
        """
        coll = 'site_data'
        # get sites
        api = 'SitesInfo.txt'
        data = self.intelroccs.fetch(api=api, secure=False)
        for row in data:
            site_status = site[0]
            site_name = site[4]
            site_quota = site[1]
            site_taken = site[2]
            # get popularity
            popularity = 0
            pipeline = list()
            project
            # insert into ml mongodb collection
            query = {'name':site_name}
            data = {'name':site_name, 'status':site_status, 'quota':site_quota, 'taken':site_taken, 'popularity':popularity}
            self.storage.update_data(coll, query=query, data=data, upsert=True)
        pipeline = list()
        match = {'$match':{'status':1}}
        pipeline.append(match)
        project = {'$project':{'name':1, 'popularity':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll, pipeline=pipeline)
        site_popularity = dict()
        for site in data:
            site_popularity[site['name']] = site['popularity']
        return site_popularity

    def balance(self):
        """
        Balance system by creating new replicas based on popularity
        """
        site_popularity = self.get_site_popularity()

def main(argv):
    """
    Main driver for Rocker Board Algorithm
    """
    log_level = logging.WARNING
    config = 'cuadrnt.cfg'
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

    logging.basicConfig(filename='/var/log/CUADRnT/cuadrnt.log', format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M', level=log_level)
    # self.logger = logging.getLogger()
    # self.logger.setLevel(logging.DEBUG)
    # handler = logging.handlers.RotatingFileHandler('/var/log/CUADRnT/cuadrnt-test.log', mode='w', maxBytes=10*1024*1024, backupCount=2)
    # handler.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    # handler.setFormatter(formatter)
    # self.logger.addHandler(handler)
    rb = RockerBoard()
    rb.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
