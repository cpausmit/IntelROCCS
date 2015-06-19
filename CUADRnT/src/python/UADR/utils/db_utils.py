#!/usr/bin/env python
"""
File       : db_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions to deal with mongodb
"""

# system modules
import logging
import hashlib
from bson.objectid import ObjectId

# Get module specific logger
logger = logging.getLogger(__name__)

def get_object_id(string=''):
    """
    Create a valid object id hash based on api and params
    """
    digest = hashlib.md5(str.encode(string)).hexdigest()
    object_id = ObjectId(digest[:24])
    return object_id

def datetime_to_object_id(datetime_):
    """
    Generate object ID based on a datetime
    """
    object_id = ObjectId.from_datetime(datetime_)
    return object_id
