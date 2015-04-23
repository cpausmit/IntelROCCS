#!/usr/local/bin/python
"""
File       : io_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful I/O functions
"""

# system modules
import os

def get_data_path():
    return '%s/data' % (os.environ.get('CUADRNT_ROOT'))

def export_csv(file_name, headers=tuple(), data=list(), debug=0):
    """
    Export data to <file_name>.csv
    Headers format: (header_1, header_2, ...)
    data format: [(data_1_1, data_1_2, ...), (data_2_1, data_2_2, ...), ...]
    """
    data_path = get_data_path()
    export_file = '%s/%s.csv' % (data_path, file_name)
    if debug:
        print "Exporting data to %s" % (export_file)
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
