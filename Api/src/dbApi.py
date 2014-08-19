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
# If we can't connect to the databse an exception is thrown.
# If a query fail we print an error message and return an empty list, leaving it up to caller to
# abort or keep executing.
#---------------------------------------------------------------------------------------------------
import sys, os, MySQLdb, datetime, subprocess
import initDb

class dbApi():
	def __init__(self):
		host = os.environ['DB_SERVER']
		db = os.environ['DB_DB']
		user = os.environ['DB_USER']
		passwd = os.environ['DB_PW']
		try:
			self.dbCon = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
		except MySQLdb.Error, e:
			raise Exception(" FATAL (%s - %s) -- Could not connect to db %s:%s" % (str(e.args[0]), str(e.args[1]), host, db))

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
			print(" ERROR -- %s\nError msg: %s\n for query: %s\n and values: %s" % (str(e.args[0]), str(e.args[1]), str(query), str(values)))
		except TypeError, e:
			print(" ERROR -- %s\nMost likely caused by an incorrect number of values\n for query: %s\n and values: %s" % (str(e), str(query), str(values)))
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
		print "DB call failed"
		sys.exit(1)
	print data
	sys.exit(0)
