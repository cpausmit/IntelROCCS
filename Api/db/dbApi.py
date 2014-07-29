#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Access to MIT database. IP address of machine where script is on need to have permission to access
# the database.
#
# Returns a list with tuples of the data requested. Example of what should be passed:
# query="SELECT * WHERE DatasetName=%s", values=["Dataset1"]
# Values are optional and can be any sequency which can be converted to a tuple.
# Example of returned data: [('Dataset1', 2), ('Dataset2', 5)]
#
# In case of error an error message is printed to the log, currently specified by environemental
# variable INTELROCCS_LOG, and '0' is returned. User will have to check that something is returned.
#---------------------------------------------------------------------------------------------------
import sys, os, MySQLdb, datetime

class dbApi():
    def __init__(self):
        self.logFile = os.environ['INTELROCCS_LOG']
        host = "t3btch039.mit.edu"
        #db = "IntelROCCS"
        db = "SiteStorage" # ^^Will switch database^^
        user = "cmsSiteDb"
        passwd = "db78user?Cms"
        self.dbCon = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)

#===================================================================================================
#  M A I N   F U N C T I O N
#===================================================================================================
    def dbQuery(self, query, values=()):
        data = []
        values = tuple([str(value) for value in values])
        try:
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute(query, values)
                for row in cur:
                    data.append(row)
        except MySQLdb.Error, e:
            with open(self.logFile, 'a') as logFile:
                logFile.write("%s DB ERROR: %s\nError msg: %s\n for query: %s\n and values: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(e.args[0]), str(e.args[1]), str(query), str(values)))
        except TypeError, e:
            with open(self.logFile, 'a') as logFile:
                logFile.write("%s DB ERROR: %s\nMost likely caused by an incorrect number of values\n for query: %s\n and values: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(e), str(query), str(values)))            
            return 0
        return data

#===================================================================================================
#  S C R I P T
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./dbApi.py <'db query'> ['value1', 'value2', ...]
# Example: $ python ./dbApi.py 'SELECT * WHERE DatasetName=%s' 'Dataset1'
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python ./dbApi.py <'db query'> ['value1', 'value2', ...]"
        sys.exit(2)
    dbApi = dbApi()
    query = sys.argv[1]
    values = []
    for v in sys.argv[2:]:
        values.append(v)
    data = dbApi.dbQuery(query, values=values)
    if not data:
        print "DB call failed, see log (%s) for more details" % (dbApi.logFile)
        sys.exit(1)
    print data
    sys.exit(0)
