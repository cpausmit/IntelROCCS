#!/usr/bin/python
#-------------------------------------------------------------------------------------------------
#
# This script is given a set of phedex request ids and will read the relevant information of the
# phedex request.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re
import phedexRequestInfo

if not os.environ.get('DETOX_DB') or not os.environ.get('MONITOR_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

#===================================================================================================
#  H E L P E R S
#===================================================================================================

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
reqId = 0

usage  = "\n"
usage += " readRequestInfo.py  <requestId>\n"
usage += "\n"
usage += "   requestId  - id of the relevant request\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    reqId = str(sys.argv[1])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

# investigate what was done in this request
requestInfo = phedexRequestInfo.PhedexRequestInfo(reqId)

if not requestInfo.isValid():
    print ' WARNING - request id returns an empty request'
    sys.exit(0)
    
sources = requestInfo.getSources()
destinations = requestInfo.getDestinations()
datasets = requestInfo.getDatasets()
move = requestInfo.isMove()
group = requestInfo.getGroup()

# print summary for debugging
print ' Is move: ' + str(move)
print ' Group:   ' + group
for source in sources:
    print ' <- ' + source
for destination in destinations:
    print ' -> ' + destination
for dataset in datasets:
    print ' -- ' + dataset
