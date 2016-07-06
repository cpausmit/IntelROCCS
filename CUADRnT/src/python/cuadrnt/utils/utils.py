#!/usr/bin/env python2.7
"""
File       : utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions
           : Internally only use datetime class since mongodb does this
           : MIT DB uses timestamps so need to convert to this
           : Only use UTC times
           : Convert as needed when dealing with other services such as phedex and pop DB
           : Only use datetime and timestamps down to seconds
           : Often will only care about dates, not times
           : Convert sizes using 1000 conversions, not 1024, as phedex uses this conversion
"""

# system modules
import logging
import os
import calendar
import random
from datetime import datetime, timedelta

# Get module specific logger
logger = logging.getLogger(__name__)

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

def weighted_choice(choices):
    """
    Do a weighted random selection
    """
    total = sum(weight for item, weight in choices.items())
    rand = random.uniform(0, total)
    upto = 0
    for choice, weight in choices.items():
        if upto + weight >= rand:
            return choice
        upto += weight

def daterange(start_date, end_date):
    """
    Return datetime range
    For consistency with range function it does not include end_date
    """
    for days in range(int((end_date - start_date).days)):
        yield start_date + timedelta(days)

def bytes_to_gb(bytes_):
    """
    Convert bytes to GB assuming 1000 conversion, not 1024, since this is what phedex does
    Some datasets can be less than 1GB so use float to avoid divide by zero issues
    """
    return float(bytes_)/10**9

def datetime_to_timestamp(datetime_):
    """
    Return datetime as timestamp
    """
    return int(calendar.timegm(datetime_.timetuple()))

def timestamp_to_datetime(timestamp):
    """
    Convert timestamp to datetime
    """
    return datetime.utcfromtimestamp(int(timestamp))

def datetime_day(datetime_):
    """
    Round datetime to previous midnight
    """
    return datetime_.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

def datetime_to_string(datetime_):
    """
    Format datetime string to date string
    YYYY-MM-DD
    """
    return datetime_.strftime('%Y-%m-%d')

def phedex_timestamp_to_datetime(timestamp):
    """
    PhEDEx stores timestamps down to milliseconds using decimals
    """
    return timestamp_to_datetime(int(timestamp))

def pop_db_timestamp_to_datetime(timestamp):
    """
    Popularity DB stores timestamps padded with 3 extra zeros
    """
    return timestamp_to_datetime(int(timestamp/10**3))

def datetime_remove_timezone(datetime_):
    """
    Datetime objects might sometimes be stored with timezone information
    """
    return datetime_.replace(tzinfo=None)

def get_json(json_data, field):
    """
    Safely get json field, return empty list if doesn't exist
    """
    try:
        return json_data[field]
    except (KeyError, TypeError, IndexError):
        return list()
