#!/usr/bin/env python2.7
"""
File       : mit_db.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: MIT DB service access module
"""

# system modules
import logging
import MySQLdb

# package modules
from UADR.services.generic import GenericService
from UADR.tools.storage import StorageManager

class MITDBService(GenericService):
    """
    Helper class to access MIT DB
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.service = 'mit_db'
        host = str(self.config[self.service]['host'])
        user = str(self.config[self.service]['user'])
        passwd = str(self.config[self.service]['passwd'])
        db = str(self.config[self.service]['db'])
        self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
        self.storage = StorageManager(self.config)

    def fetch(self, query, values=tuple(), cache=True, cache_only=False, force_cache=False):
        """
        Get MIT DB data
        """
        if cache:
            json_data = list()
            if not force_cache:
                json_data = self.storage.get_cache(self.service, query, values)
            if not json_data:
                json_data = self.get_data(query=query, values=values)
                self.storage.insert_cache(self.service, query, values, json_data)
            if not cache_only:
                return json_data
        else:
            json_data = self.get_data(query=query, values=values)
            return json_data

    def get_data(self, query, values=tuple()):
        """
        Submit query to MIT DB
        """
        data = []
        values = tuple([str(value) for value in values])
        try:
            with self.conn:
                cur = self.conn.cursor()
                cur.execute(query, values)
                for row in cur:
                    data.append(row)
        except Exception, e:
            self.logger.error('Query failed with message %s\n\tQuery: %s %s' % (e, str(query), str(values)))
        return data
