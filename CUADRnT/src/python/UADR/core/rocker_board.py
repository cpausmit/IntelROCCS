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
from UADR.services.phedex import PhEDExService
from UADR.tools.datasets import DatasetManager
from UADR.tools.storage import StorageManager
from UADR.rankings.delta import DeltaRanking

MAX_THREADS = 1

class RockerBoard(object):
    """
    RockerBoard is a system balancing algorithm using popularity metrics to predict popularity
    and make appropriate replications to keep the system balanced
    """
    def __init__(self, config=dict()):
        global MAX_THREADS
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.phedex = PhEDExService(self.config)
        self.datasets = DatasetManager(self.config)
        self.storage = StorageManager(self.config)
        self.rankings = DeltaRanking(self.config)
        self.max_gb = int(self.config['rocker_board']['max_gb'])
        self.min_rank = float(self.config['rocker_board']['min_rank'])

    def start(self):
        """
        Begin Rocker Board Algorithm
        """
        self.sites.update_sites()
        self.datasets.update_datasets()
        subscriptions = self.balance()
        for subscription in subscriptions:
            self.logger.info('site: %s\tdataset: %s', subscription[1], subscription[0])
        #self.subscribe(subscriptions)

    def balance(self):
        """
        Balance system by creating new replicas based on popularity
        """
        subscriptions = list()
        dataset_rankings = self.rankings.get_dataset_rankings()
        site_rankings = self.rankings.get_site_rankings()
        subscribed_gb = 0
        while subscribed_gb < self.max_gb:
            tmp_site_rankings = site_rankings
            dataset_name = weighted_choice(dataset_rankings)
            if dataset_rankings[dataset_name] < self.min_rank:
                break
            unavailable_sites = self.datasets.get_sites(dataset_name)
            for site_name in unavailable_sites:
                try:
                    del tmp_site_rankings[site_name]
                except:
                    continue
            site_name = weighted_choice(site_rankings)
            subscription = (dataset_name, site_name)
            subscriptions.append(subscription)
            size_gb = self.datasets.get_size(dataset_name)
            subscribed_gb += size_gb
        return subscriptions

    def subscribe(self, subscriptions):
        """
        Make subscriptions to phedex
        subscriptions = [(dataset_name, site_name), ...]
        """
        new_subscriptions = dict()
        for subscription in subscriptions:
            dataset_name = subscription[0]
            site_name = subscription[1]
            try:
                new_subscriptions[site_name].append(dataset_name)
            except:
                new_subscriptions[site_name] = list()
                new_subscriptions[site_name].append(dataset_name)
        for site_name, dataset_names in new_subscriptions.items():
            data = self.phedex.generate_xml(dataset_names)
            comments = 'This dataset is predicted to become popular and has therefore been automatically replicated by CUADRnT'
            api = 'subscribe'
            params = [('node', site_name), ('data', data), ('level','dataset'), ('move', 'n'), ('custodial', 'n'), ('group', 'AnalysisOps'), ('request_only', 'n'), ('no_mail', 'n'), ('comments', comments)]
            json_data = self.phedex.fetch(api=api, params=params)
            # insert into db

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
