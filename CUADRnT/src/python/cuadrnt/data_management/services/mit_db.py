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
from cuadrnt.data_management.services.generic import GenericService

class MITDBService(GenericService):
    """
    Helper class to access MIT DB
    """
    def __init__(self, config=dict()):
        GenericService.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.SERVICE = 'mit_db'
        host = str(self.config[self.SERVICE]['host'])
        user = str(self.config[self.SERVICE]['user'])
        passwd = str(self.config[self.SERVICE]['passwd'])
        db = str(self.config[self.SERVICE]['db'])
        self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)

    def query(self, query, values=tuple(), cache=True, cache_only=False, force_cache=False):
        """
        Get MIT DB data
        """
        if cache:
            json_data = list()
            if not force_cache:
                json_data = self.storage.get_cache(self.SERVICE, query, values)
            if not json_data:
                json_data = self.call(query=query, values=values)
                self.storage.insert_cache(self.SERVICE, query, values, json_data)
            if not cache_only:
                return json_data
        else:
            json_data = self.call(query=query, values=values)
            return json_data

    def call(self, query, values=tuple()):
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
