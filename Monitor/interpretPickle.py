#!/usr/bin/python

import readline 
import code
import cPickle as pickle
import sys
from Dataset import *

if len(sys.argv) < 2:
    sys.stderr.write("Usage: %s <Pickle file> [--quiet]\n"%(sys.argv[0]))
    sys.exit(1)

pklJar = open(sys.argv[1],"rb")
pklDict = pickle.load(pklJar) # open the pickle jar
datasetSet = pklDict["datasetSet"] # eat the pickle
nSiteAccess = pklDict["nSiteAccess"]
if len(sys.argv)==3 and sys.argv[2] =="--quiet":
  pass
else:
  i=0
  for k,v in datasetSet.iteritems():
      if i == 10: break
      print v
      i += 1
print 'pklDict = { "datasetSet" : datasetSet, "nSiteAccess" : nSiteAccess }'
vars = globals().copy()
vars.update(locals())
shell = code.InteractiveConsole(vars)
shell.interact()
