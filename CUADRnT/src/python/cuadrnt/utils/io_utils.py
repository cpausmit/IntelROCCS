#!/usr/bin/env python2.7
"""
File       : io_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful I/O functions
"""

# system modules
import logging

# Get module specific logger
logger = logging.getLogger(__name__)

def export_csv(headers=tuple(), data=list(), file_name='data', path='/var/lib/cuadrnt'):
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
