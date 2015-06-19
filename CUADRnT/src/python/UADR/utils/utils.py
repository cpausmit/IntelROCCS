#!/usr/bin/env python
"""
File       : utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions
"""

# system modules
import logging
import os
import datetime
import calendar

# Get module specific logger
logger = logging.getLogger(__name__)

def bytes_to_gb(bytes):
    """
    Convert bytes to gb assuming 1000 conversion, not 1024
    """
    return int(bytes/10**9)

def datetime_to_timestamp(datetime_):
    """
    Return datetime as timestamp
    """
    return calendar.timegm(datetime_.timetuple())

def timestamp_to_datetime(timestamp):
    """
    Convert timestamp to datetime
    """
    return datetime.datetime.utcfromtimestamp(timestamp)

def datetime_day(datetime_):
    """
    Round datetime to previous midnight GMT time
    """
    td = datetime.timedelta(hours=datetime_.hour, minutes=datetime_.minute, seconds=datetime_.second, microseconds=datetime_.microsecond)
    datetime_day = datetime_ - td
    return datetime_day

def timestamp_day(timestamp):
    """
    Round timestamp to previous midnight GMT time
    """
    days = int(timestamp/86400)
    day_timestamp = days*86400
    return day_timestamp

def datetime_to_pop_db_date(datetime_):
    """
    Format datetime string to pop db date string
    YYYY-MM-DD
    """
    return datetime_.strftime('%Y-%m-%d')

def phedex_timestamp_to_datetime(timestamp):
    """
    PhEDEx stores timestamps down to milliseconds
    """
    return timestamp_to_datetime(int(timestamp))

def timestamp_to_pop_db_utc_date(timestamp):
    """
    Convert timestamp to date string of format YYYY-MM-DD
    """
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def pop_db_timestamp_to_timestamp(timestamp):
    """
    Popularity DB stores timestamps padded with extra zeros
    """
    return int(timestamp/10**3)

def check_tool(tool):
    """
    Check if a command line tool exists
    """
    for _dir in os.environ['PATH'].split(':'):
        tool_path = os.path.join(_dir, tool)
        if os.path.exists(tool_path):
            return True
    else:
        logger.error('Command line tool %s not found', tool)
        return False
