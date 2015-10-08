#!/usr/bin/env python2.7
"""
File       : rocker_board.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Distribute data in system to keep it balanced (like a rocker board)
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.utils import weighted_choice
from cuadrnt.utils.utils import timestamp_to_datetime
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.config import get_config
from cuadrnt.services.phedex import PhEDExService
from cuadrnt.services.mit_db import MITDBService
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.sites import SiteManager
from cuadrnt.tools.storage import StorageManager
from cuadrnt.rankings.delta import DeltaRanking

class RockerBoard(object):
    """
    RockerBoard is a system balancing algorithm using popularity metrics to predict popularity
    and make appropriate replications to keep the system balanced
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.phedex = PhEDExService(self.config)
        self.mit_db = MITDBService(self.config)
        self.datasets = DatasetManager(self.config)
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)
        self.rankings = DeltaRanking(self.config)
        self.max_gb = int(self.config['rocker_board']['max_gb'])
        self.min_rank = float(self.config['rocker_board']['min_rank'])

    def start(self):
        """
        Begin Rocker Board Algorithm
        """
        t1 = datetime.datetime.utcnow()
        subscriptions = self.balance()
        for subscription in subscriptions:
            self.logger.info('site: %s\tdataset: %s', subscription[1], subscription[0])
        self.subscribe(subscriptions)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Rocker Board took %s', str(td))

    def balance(self):
        """
        Balance system by creating new replicas based on popularity
        """
        subscriptions = list()
        dataset_rankings = self.rankings.dataset_rankings()
        site_rankings = self.rankings.site_rankings()
        subscribed_gb = 0
        while subscribed_gb < self.max_gb:
            tmp_site_rankings = site_rankings
            dataset_name = weighted_choice(dataset_rankings)
            if (not dataset_name) or (dataset_rankings[dataset_name] < self.min_rank):
                break
            size_gb = self.datasets.get_size(dataset_name)
            unavailable_sites = set(self.datasets.get_sites(dataset_name))
            for site_name in tmp_site_rankings.keys():
                if (self.sites.get_available_storage(site_name) < size_gb) or (tmp_site_rankings[site_name] <= 0):
                    unavailable_sites.add(site_name)
            for site_name in unavailable_sites:
                try:
                    del tmp_site_rankings[site_name]
                except:
                    continue
            if not tmp_site_rankings:
                break
            site_name = weighted_choice(tmp_site_rankings)
            subscription = (dataset_name, site_name)
            subscriptions.append(subscription)
            subscribed_gb += size_gb
            avail_storage = self.sites.get_available_storage(site_name)
            self.logger.info('rank: %s\tsize: %.2f\tdataset: %s', dataset_rankings[dataset_name], size_gb, dataset_name)
            self.logger.info('rank: %s\tstorage: %d\site: %s', site_rankings[site_name], avail_storage, site_name)
            new_avail_storage = avail_storage - self.datasets.get_size(dataset_name)
            if new_avail_storage > 0:
                new_rank = 0.0
            else:
                new_rank = (site_rankings[site_name]/avail_storage)*new_avail_storage
            site_rankings[site_name] = new_rank
            del dataset_rankings[dataset_name]
        mini_datasets = self.miniaod_subscriptions()
        subscriptions += mini_datasets
        self.logger.info('Subscribed %dGB', subscribed_gb)
        return subscriptions

    def mini_subscriptions(self):
        """
        Make sure all miniaod[sim] datasets have at least one replica at a US site
        """
        # get all MINIAOD[SIM] datasets which do not have a replica at a US site.
        # get all US sites with rankings
        # follow the same selection procedure
        # add selection function

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
            comments = 'This dataset is predicted to become popular and has therefore been automatically replicated by cuadrnt'
            api = 'subscribe'
            params = [('node', site_name), ('data', data), ('level','dataset'), ('move', 'n'), ('custodial', 'n'), ('group', 'AnalysisOps'), ('request_only', 'n'), ('no_mail', 'n'), ('comments', comments)]
            json_data = self.phedex.fetch(api=api, params=params, cache=False)
            # insert into db
            group_name = 'AnalysisOps'
            request_id = 0
            request_type = 0
            try:
                request = json_data['phedex']
                request_id = request['request_created'][0]['id']
                request_created = timestamp_to_datetime(request['request_timestamp'])
            except:
                self.logger.warning('Subscription did not succeed\n\tSite:%s\n\tDatasets: %s', str(site_name), str(dataset_names))
                continue
            for dataset_name in dataset_names:
                coll = 'dataset_popularity'
                date = datetime_day(datetime.datetime.utcnow())
                pipeline = list()
                match = {'$match':{'name':dataset_name, 'date':date}}
                pipeline.append(match)
                project = {'$project':{'delta_rank':1, '_id':0}}
                pipeline.append(project)
                data = self.storage.get_data(coll=coll, pipeline=pipeline)
                dataset_rank = data[0]['delta_rank']
                query = "INSERT INTO Requests(RequestId, RequestType, DatasetId, SiteId, GroupId, Rank, Date) SELECT %s, %s, Datasets.DatasetId, Sites.SiteId, Groups.GroupId, %s, %s FROM Datasets, Sites, Groups WHERE Datasets.DatasetName=%s AND Sites.SiteName=%s AND Groups.GroupName=%s"
                values = (request_id, request_type, dataset_rank, request_created, dataset_name, site_name, group_name)
                self.mit_db.query(query=query, values=values, cache=False)

def main(argv):
    """
    Main driver for Rocker Board Algorithm
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='rocker_board.cfg')
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

    log_path = config['paths']['log']
    log_file = 'rocker_board.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    rocker_board = RockerBoard(config)
    rocker_board.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
