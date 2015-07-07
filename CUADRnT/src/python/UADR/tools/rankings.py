#!/usr/bin/env python
"""
File       : rankings.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate popularity metric
"""

# system modules
import logging
#import datetime

# package modules
from UADR.tools.sites import SiteManager
from UADR.tools.storage import StorageManager

class SimpleRankings(object):
    """
    Rankings for simple algorithm
    Uses last days popularity metrics to generate rankings
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)

class CRABRankings(object):
    """
    Rankings for CRAB values algorithm
    Uses CRAB queue to generate rankings
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)

class DeltaRankings(object):
    """
    Rankings for delta values algorithm
    Uses popularity metrics from last 2 weeks to generate rankings
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)

class MLRankings(object):
    """
    Rankings for machine learning algorithm
    Uses machine learning model to generate rankings
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.storage = StorageManager(self.config)
        # TODO: Use different ML algorithms
