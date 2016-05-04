#!/usr/bin/env python2.7
"""
File       : svm.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: SVM ranking algorithm
"""

# system modules
import logging
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.externals import joblib

# package modules
# from cuadrnt.utils.utils import datetime_day
from cuadrnt.data_analysis.rankings.generic import GenericRanking

class SVMRanking(GenericRanking):
    """
    Use Support Vector Machines to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.name = 'svm'
        for data_tier in self.data_tiers:
            try:
                self.clf_trend[data_tier] = joblib.load(self.data_path + '/' + self.name + '_trend_' + data_tier + '.pkl')
                self.clf_avg[data_tier] = joblib.load(self.data_path + '/' + self.name + '_avg_' + data_tier + '.pkl')
            except:
                self.logger.info('%s classifier and regressor for data tier %s need to be trained', self.name, data_tier)
                self.clf_trend[data_tier] = SVC(kernel='poly', probability=True, C=0.5)
                self.clf_avg[data_tier] = SVR()
        self.train()
        self.test()
