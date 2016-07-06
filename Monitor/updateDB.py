#!/usr/bin/env python

from random import shuffle
from os import environ 
import findDatasetProperties as fDP
from sys import argv

label = int(argv[1])

phedexFile = open(environ['DETOX_DB']+'/status/DatasetsInPhedexAtSites.dat','r')
dss = []
for line in phedexFile:
  dss.append(line.split()[0])

counter=0
ntotal=len(dss)
per = ntotal/5+1
cursor = fDP.getDbCursor()
for i in xrange(per*(label-1),per*label):
  print i-per*(label-1),'/',ntotal/5,' (',label,')'
  fDP.findDatasetProperties(dss[i],False,cursor)
