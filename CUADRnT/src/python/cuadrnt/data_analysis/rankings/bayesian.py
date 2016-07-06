#!/usr/bin/env python2.7

###
# File        : bayesian.js
# Authors     : Bjorn Barrefors
# Project     : CMS-DDM -> https://github.com/bbarrefors/CMS-DDM
# Description : Popularity prediction using Bayesian Ridge Regression with a Gaussian Naive Bayes distribution
#
# TODO : Follow coding conventions
# DOCS : Documentation needs update
# TEST : No testing have yet been done
# NOTE : Still need to apply my flavor
###

"""
File       : bayesian.py
Author     : Bjorn Barrefors
Description: Bayesian based ranking algorithm
"""

# system modules
import logging
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import BayesianRidge
from sklearn.externals import joblib

# package modules
from cuadrnt.data_analysis.rankings.generic import GenericRanking

class BayesianRanking(GenericRanking):
    """
    Use Naive Bayes and Bayesian Ridge to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.name = 'bayesian'
        for data_tier in self.data_tiers:
            try:
                self.clf_trend[data_tier] = joblib.load(self.data_path + '/' + self.name + '_trend_' + data_tier + '.pkl')
                self.clf_avg[data_tier] = joblib.load(self.data_path + '/' + self.name + '_avg_' + data_tier + '.pkl')
            except:
                self.logger.info('%s classifier and regressor for data tier %s need to be trained', self.name, data_tier)
                self.clf_trend[data_tier] = GaussianNB()
                self.clf_avg[data_tier] = BayesianRidge()
        self.train()
        self.test()
