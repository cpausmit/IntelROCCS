#!/usr/bin/env python2.7
"""
File       : io_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful I/O functions
"""

# system modules
import logging
import json

# Get module specific logger
logger = logging.getLogger(__name__)

def export_csv(headers=tuple(), data=list(), path='/var/lib/cuadrnt', file_name='data'):
    """
    Export data to <path>/<file_name>.csv
    Headers format: (header_1, header_2, ...)
    data format: [(data_1_1, data_1_2, ...), (data_2_1, data_2_2, ...), ...]
    """
    export_file = '%s/%s.csv' % (path, file_name)
    logger.debug('Exporting to file: %s', export_file)
    # write headers
    header_str = ''
    for field in headers:
        header_str += '%s,' % (field)
    header_str = header_str[:-1] + '\n'
    fd = open(export_file, 'w')
    fd.write(header_str)
    fd.close()
    # write data
    fd = open(export_file, 'a')
    for row in data:
        row_str = ''
        for field in row:
            row_str += '%s,' % (field)
        row_str = row_str[:-1] + '\n'
        fd.write(row_str)
    fd.close()

def export_json(data, path='/var/lib/cuadrnt', file_name='data'):
    """
    Export JSON data to a file using dump
    """
    export_file = '%s/%s.json' % (path, file_name)
    logger.debug('Exporting to file: %s', export_file)
    fd = open(export_file, 'w')
    json.dump(data, fd)
    fd.close()
