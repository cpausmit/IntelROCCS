#!/usr/bin/env python

from os import system
import subprocess
import json
from StringIO import StringIO
from sys import argv

label = int(argv[1])

with open('ds_skimmed.list','r') as fds:
  lds = list(fds)

nDS = len(lds)
split = 10 # split into 10 batches
nPer = nDS/split+1

outlist = []

for iD in xrange((label-1)*nPer,label*nPer):
  if iD%100==0:
    print label,iD
    #flush every 100 queries
    with open('creationTimes_%i.txt'%label,'a') as outfile:
      for line in outlist:
        outfile.write(line)
    outlist = []
  ds = lds[iD].strip()
  #ds = '/GluGluToHToGG_M-125_14TeV-powheg-pythia6/TP2023SHCALDR-SHCALMar26_PU140BX25_PH2_1K_FB_V6-v1/GEN-SIM-DIGI-RAW'

  cmd = 'curl -k -H "Accept: application/json" --cert /home/snarayan/.globus/usercert.pem --key /home/snarayan/.globus/userkey.pem "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/blocks?dataset=%s&detail=true"'%ds

  for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
    payload = json.load(StringIO(line))

  creation = 0

  for block in payload:
    creation = max(creation,block['creation_date'])
  if creation>0:
    outlist.append('%s\t%i\n'%(ds,creation))

with open('creationTimes_%i.txt'%label,'a') as outfile:
  for line in outlist:
    outfile.write(line)
