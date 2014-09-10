#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script uses the phedex database to extract the transfer/deletion history of a given dataset.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb
from xml.etree import ElementTree

# setup definitions
if not os.environ.get('MONITOR_DB'):
    print '\n ERROR - MONITOR environment not defined: source setupMonitor.sh\n'
    sys.exit(0)

#===================================================================================================
#  H E L P E R S
#===================================================================================================

#===================================================================================================
#  M A I N
#===================================================================================================
debug = False

# command line arguments
short = False
if   len(sys.argv)<2:
    print ' not enough arguments\n'
    sys.exit(1)
elif len(sys.argv)==2:
    dataset = str(sys.argv[1])
else:
    print ' too many arguments\n'
    sys.exit(2)

# first make sure not to analyze any weird data (require /*/*/* name pattern)
if not re.search(r'/.*/.*/.*',dataset,re.S):
    print ' Error: Dataset does NOT match expected pattern'
    sys.exit(3)

# make a reasonable file name
fileName = dataset.replace('/','%')
fileName = os.environ.get('MONITOR_DB') + '/datasets/' + fileName

# test whether we know this dataset already
if os.path.exists(fileName):
    pass
else:        # check failed so need to go to the source
    cmd = 'wget --no-check-certificate -O ' + fileName + \
          ' https://cmsweb.cern.ch/phedex/datasvc/xml/prod/requestlist?dataset=' + dataset
    print ' CMD: ' + cmd
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        print line


if debug:
    document = ElementTree.parse(fileName)
    for request in document.findall('request'):
        print ' Request: type=%s decision=%s'%(request.attrib['type'],request.attrib['approval'])
        for node in request.findall('node'):
            print '  -> Node: time=%s name=%s decision=%s'%\
                  (node.attrib['time_decided'],node.attrib['name'],node.attrib['decision'])

sys.exit(0)
