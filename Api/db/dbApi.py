#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Access to MIT database. IP address of machine where script is on need to have permission to access
# the database.
#
# Returns a list with touples of the data requested. Example of what should be passed:
# query="SELECT * WHERE DatasetName=%s", values=touple("Dataset1")
# Values are optional
#
# In case of error an exception is thrown. This needs to be dealt with by the caller.
#---------------------------------------------------------------------------------------------------
import sys, os, MySQLdb

class dbApi():
    def __init__(self):
        host = "t3btch039.mit.edu"
        #db = "IntelROCCS"
        db = "SiteStorage" # ^^Will switch database^^
        user = "cmsSiteDb"
        passwd = "db78user?Cms"
        self.dbCon = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def dbQuery(self, query, values=()):
        data = []
        values = tuple([str(value) for value in values])
        try:
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute(query, values)
                for i in range(cur.rowcount):
                    row = cur.fetchone()
                    data.append(row)
        except MySQLdb.Error, e:
            raise Exception("FATAL - Database failure:  %d: %s" % (e.args[0],e.args[1]))
        return data

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./dbAccess.py <'db query'> ['value1', 'value2', ...]
# Example: $ python ./dbAccess.py 'SELECT * WHERE DatasetName=%s' 'Dataset1'
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./dbAccess.py <'db query'> ['value1', 'value2', ...]"
        sys.exit(2)
    dbaccess = dbAccess()
    query = sys.argv[1]
    values = []
    for v in sys.argv[2:]:
        values.append(v)
    values = tuple(values)
    data = dbaccess.dbQuery(query, values=values)
    print data
    sys.exit(0)
