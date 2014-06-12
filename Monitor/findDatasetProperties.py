#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script uses das_client.py to extract the given dataset properties. It will determine the
# number of files and the dataset size.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, glob, subprocess, json, pprint

short = False
if   len(sys.argv)<2:
    print 'not enough arguments\n'
    sys.exit(1)
elif len(sys.argv)==2:
    dataset = str(sys.argv[1])
elif len(sys.argv)==3:
    dataset = str(sys.argv[1])
    short = True
else:
    print 'too many arguments\n'
    sys.exit(2)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    # this is the text including the size units, that need to be converted
    sizeGb  = float(sizeTxt[0:-2])
    units   = sizeTxt[-2:]
    # decide what to do for the given unit
    if   units == 'MB':
        sizeGb = sizeGb/1024.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1024.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(0)

    # return the size in GB as a float
    return sizeGb

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
# use das client to find the present size of the dataset
cmd = 'das_client.py --format=plain --limit=0 --query="file dataset=' + \
      dataset + ' | sum(file.size), count(file.name)" | sort -u'
if debug>0:
    print ' CMD: ' + cmd
nFiles = 0
sizeGb = 0.
for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
    line = line[:-1]
    if   re.search('file.name',line):
        if debug>0:
            print ' count ' + line
        nFiles = int(line.split("=")[1])
    elif re.search('file.size',line):
        if debug>0:
            print ' size  ' + line
        size = line.split("=")[1]
        sizeGb = convertSizeToGb(size)

averageSizeGb = 0
if nFiles>0:
    averageSizeGb = sizeGb/nFiles

if short:
    print '%d %.1f %.3f %s'%(nFiles,sizeGb,averageSizeGb,dataset)
else:
    print ' nFiles:%d  size:%.1f GB [average file size:%.3f GB] -- dataset:%s'\
          %(nFiles,sizeGb,averageSizeGb,dataset)

sys.exit(0)
