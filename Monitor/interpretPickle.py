#!/usr/bin/python

import readline 
import code
import cPickle as pickle
import sys
from Dataset import Dataset,Request 

if len(sys.argv) < 2:
		sys.stderr.write("Usage: %s <Pickle file> [--quiet]\n"%(sys.argv[0]))
		sys.exit(1)

pklJar = open(sys.argv[1],"rb")
datasets = pickle.load(pklJar) 
if len(sys.argv)==3 and sys.argv[2] =="--quiet":
	pass
else:
	i=0
	for k in datasets:
			if i == 10: break
			print k
			print datasets[k]
			i += 1
vars = globals().copy()
vars.update(locals())
shell = code.InteractiveConsole(vars)
shell.interact()
