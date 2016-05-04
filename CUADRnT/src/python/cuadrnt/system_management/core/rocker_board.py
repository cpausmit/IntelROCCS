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
import operator
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.utils import weighted_choice
from cuadrnt.utils.utils import timestamp_to_datetime
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.services.phedex import PhEDExService
from cuadrnt.data_management.services.mit_db import MITDBService
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.popularity import PopularityManager
from cuadrnt.data_management.core.storage import StorageManager
from cuadrnt.data_analysis.rankings.ranker import Ranker

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
        self.popularity = PopularityManager(self.config)
        self.storage = StorageManager(self.config)
        self.rankings = Ranker(self.config)
        self.max_gb = int(self.config['rocker_board']['max_gb'])
        self.csv_data = list()

    def start(self, date=datetime_day(datetime.datetime.utcnow())):
        """
        Begin Rocker Board Algorithm
        """
        t1 = datetime.datetime.utcnow()
        # Get goals
        dataset_rankings = self.rankings.get_dataset_rankings(date)
        site_rankings = self.rankings.get_site_rankings(date)
        self.change_dataset_rankings(dataset_rankings)
        subscriptions = self.replicate(dataset_rankings, site_rankings)
        self.logger.info('SUBSCRIPTIONS')
        for subscription in subscriptions:
            self.logger.info('site: %s\tdataset: %s', subscription[1], subscription[0])
        # self.subscribe(subscriptions)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Rocker Board took %s', str(td))

    def change_dataset_rankings(self, dataset_rankings):
        """
        Change the ranks from being the target number of replicas to being the
        change in number of replicas required to reach the goal
        """
        current_replicas = self.datasets.get_current_num_replicas()
        for dataset in current_replicas:
            dataset_rankings[dataset['name']] -= dataset['n_replicas']

    def replicate(self, dataset_rankings, site_rankings):
        """
        Balance system by creating new replicas based on popularity
        """
        subscriptions = list()
        subscribed_gb = 0
        sites_available_storage_gb = self.sites.get_all_available_storage()
        while (subscribed_gb < self.max_gb) and site_rankings:
            tmp_site_rankings = dict()
            for k, v in site_rankings.items():
                tmp_site_rankings[k] = v
            dataset = max(dataset_rankings.iteritems(), key=operator.itemgetter(1))
            dataset_name = dataset[0]
            dataset_rank = dataset[1]
            if (not dataset_name) or (dataset_rank < 1):
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
                del dataset_rankings[dataset_name]
                continue
            site_name = weighted_choice(tmp_site_rankings)
            subscription = (dataset_name, site_name)
            subscriptions.append(subscription)
            subscribed_gb += size_gb
            sites_available_storage_gb[site_name] -= size_gb
            self.logger.info('%s : added', dataset_name)
            if sites_available_storage_gb[site_name] <= 0:
                del site_rankings[site_name]
            dataset_rankings[dataset_name] -= 1
        self.logger.info('Subscribed %dGB', subscribed_gb)
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
            comments = 'This dataset is predicted to become popular and has therefore been automatically replicated by cuadrnt'
            api = 'subscribe'
            params = [('node', site_name), ('data', data), ('level','dataset'), ('move', 'n'), ('custodial', 'n'), ('group', 'AnalysisOps'), ('request_only', 'n'), ('no_mail', 'n'), ('comments', comments)]
            json_data = self.phedex.fetch(api=api, params=params, method='post')
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
                coll = 'dataset_rankings'
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
    config = get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg')
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
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=2)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    rocker_board = RockerBoard(config)
    rocker_board.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
