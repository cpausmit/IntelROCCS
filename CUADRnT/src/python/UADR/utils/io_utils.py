#!/usr/bin/env python
"""
File       : io_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful I/O functions
"""

# system modules
import logging

# Get module specific logger
logger = logging.getLogger(__name__)

def get_data_path():
    """
    Data stored in /var/lib/CUADRnT
    """
    return '/var/lib/CUADRnT'

def export_csv(file_name, headers=tuple(), data=list()):
    """
    Export data to <file_name>.csv
    Headers format: (header_1, header_2, ...)
    data format: [(data_1_1, data_1_2, ...), (data_2_1, data_2_2, ...), ...]
    """
    data_path = get_data_path()
    export_file = '%s/%s.csv' % (data_path, file_name)
    fs = open(export_file, 'w')
    header_str = ''
    for field in headers:
        header_str += '%s,' % (field)
    header_str = header_str[:-1] + '\n'
    fs.write(header_str)
    fs.close()
    fs = open(export_file, 'a')
    for row in data:
        row_str = ''
        for field in row:
            row_str += '%s,' % (field)
        row_str = row_str[:-1] + '\n'
        fs.write(row_str)
    fs.close()
