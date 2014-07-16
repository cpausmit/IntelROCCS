#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Keeps a record of how many replicas exist for each dataset. Will only record changes and therefore
# an exact number of replicas can be extracted for any day. 
#
# Needs to be ran once a day to keep an accurate record.
#
#---------------------------------------------------------------------------------------------------
import sys, os
BASEDIR = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
for root, dirs, files in os.walk(BASEDIR):
    sys.path.append(root)
import dbAccess, phedex

class updateDb():
    def __init__(self):
        self.dbaccess = dbAccess.dbAccess()
        self.phdx = phedex.phedex()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def insertSubscription(self, json_data):
        request_id = json_data.get('phedex').get('request_created')[0].get('id')
        # TODO : Definition for subscriptions table?
        query = "INSERT INTO "
        values = []
        self.dbaccess.dbQuery(query, values=tuple(values))

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./updateDB.py <function> <dataset> <replicas>
if __name__ == '__main__':
    if not (len(sys.argv) == 4):
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(2)
    updatedb = updateDB()
    func = getattr(updatedb, sys.argv[1], None)
    if not func:
        print "Function %s is not available" % (sys.argv[1])
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(3)
    args = []
    for arg in sys.argv[2:]:
        args.append(arg)
    try:
        data = func(*args)
    except TypeError, e:
        print e
        print "Usage: python ./updateDB.py <function> <dataset> <replicas>"
        sys.exit(3)
    print data
    sys.exit(0)
