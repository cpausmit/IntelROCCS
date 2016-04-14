#!/usr/bin/env python2.7
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic class for all ranking algorithms
"""

# system modules
import logging
import datetime
import json
import math
import numpy as np
from sklearn.externals import joblib

# package modules
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager
from cuadrnt.data_management.core.storage import StorageManager

class GenericRanking(object):
    """
    Generic Ranking class
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.storage = StorageManager(self.config)
        self.max_replicas = int(config['rocker_board']['max_replicas'])
        self.name = 'generic'
        self.data_path = self.config['paths']['data']
        self.data_tiers = config['tools']['valid_tiers'].split(',')
        self.preprocessed_data = dict()
        self.clf_trend = dict()
        self.clf_avg = dict()

    def predict_trend(self, features, data_tier):
        """
        Predict trend based on features
        """
        prediction = self.clf_trend[data_tier].predict(features)
        return prediction[0]

    def predict_avg(self, features, data_tier):
        """
        Predict trend based on features
        """
        prediction = self.clf_avg[data_tier].predict(features)
        return prediction[0]

    def train(self):
        """
        Training classifier and regressor
        """
        for data_tier in self.data_tiers:
            fd = open(self.data_path + '/training_data_' + data_tier + '.json', 'r')
            self.preprocessed_data[data_tier] = json.load(fd)
            fd.close()
            tot = len(self.preprocessed_data[data_tier]['features'])
            p = int(math.ceil(tot*0.8))
            training_features = np.array(self.preprocessed_data[data_tier]['features'][:p])
            trend_training_classifications = np.array(self.preprocessed_data[data_tier]['trend_classifications'][:p])
            avg_training_classifications = np.array(self.preprocessed_data[data_tier]['avg_classifications'][:p])
            t1 = datetime.datetime.utcnow()
            self.clf_trend[data_tier].fit(training_features, trend_training_classifications)
            self.clf_avg[data_tier].fit(training_features, avg_training_classifications)
            t2 = datetime.datetime.utcnow()
            td = t2 - t1
            self.logger.info('Training %s for data tier %s took %s', self.name, data_tier, str(td))
            joblib.dump(self.clf_trend[data_tier], self.data_path + '/' + self.name + '_trend_' + data_tier + '.pkl')
            joblib.dump(self.clf_avg[data_tier], self.data_path + '/' + self.name + '_avg_' + data_tier + '.pkl')

    def test(self):
        """
        Test accuracy/score of classifier and regressor
        """
        for data_tier in self.data_tiers:
            tot = len(self.preprocessed_data[data_tier]['features'])
            p = int(math.floor(tot*0.2))
            test_features = np.array(self.preprocessed_data[data_tier]['features'][p:])
            trend_test_classifications = np.array(self.preprocessed_data[data_tier]['trend_classifications'][p:])
            avg_test_classifications = np.array(self.preprocessed_data[data_tier]['avg_classifications'][p:])
            accuracy_trend = self.clf_trend[data_tier].score(test_features, trend_test_classifications)
            accuracy_avg = self.clf_avg[data_tier].score(test_features, avg_test_classifications)
            self.logger.info('The accuracy of %s trend classifier for data tier %s is %.3f', self.name, data_tier, accuracy_trend)
            self.logger.info('The accuracy of %s avg regressor for data tier %s is %.3f', self.name, data_tier, accuracy_avg)
