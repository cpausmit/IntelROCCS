#!/usr/bin/env python2.7
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic class for all ranking algorithms
"""

# system modules
import logging

# package modules
from cuadrnt.tools.sites import SiteManager
from cuadrnt.tools.datasets import DatasetManager
from cuadrnt.tools.popularity import PopularityManager
from cuadrnt.tools.storage import StorageManager

class GenericRanking(object):
    """
    Rankings for simple algorithm
    Uses last days popularity metrics to generate rankings
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.storage = StorageManager(self.config)
