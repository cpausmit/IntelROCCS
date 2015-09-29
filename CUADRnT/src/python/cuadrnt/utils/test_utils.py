#!/usr/bin/env python2.7
"""
File       : test_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions used in tests
"""

# system modules
import logging
import sys

# Get module specific logger
logger = logging.getLogger(__name__)

def total_size_of(data_structure):
    """
    Recursively get the total size of data structure
    """
    total_size = 0
    if isinstance(data_structure, dict):
        total_size += 280
        for k, v in data_structure.items():
            total_size += sys.getsizeof(k) + total_size_of(v)
    elif isinstance(data_structure, list):
        total_size += 72
        for v in data_structure:
            total_size += total_size_of(v)
    else:
        total_size += sys.getsizeof(data_structure)
    return total_size
