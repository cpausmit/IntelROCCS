#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, MySQLdb, json
import datetime, time
import phedexDataset

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class DeprecateDataHandler:
    def __init__(self):
        self.accessDas = False
        self.fileName = 'DeprecatedSets.txt'
        self.deprecated = {}


    def shouldAccessDas(self,flag):
       self.accessDas = flag
       statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
       
       if not os.path.isfile(statusDir+'/'+self.fileName):
           self.accessDas = True
           return
       #also check that the file is not empty
       if not os.path.getsize(statusDir+'/'+self.fileName) > 0:
           self.accessDas = True

    def extractDeprecatedData(self):
        if self.accessDas:
            try:
                self.retrieveDeprecatedData()
            except:
                raise
        self.readDeprecatedData()

    def retrieveDeprecatedData(self):
        cmd = os.environ['DETOX_BASE'] + '/' + \
            'das_client.py --query=\'dataset status=DEPRECATED\' --limit=50000'

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)

        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(20*60)  # 20 minutes
        try:
            strout, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            return
            #raise Exception(" FATAL -- DAS call timed out, stopping")

        if process.returncode != 0:
            print "    - DAS queries got bad status " + str(process.returncode)
            return
            #raise Exception(" FATAL -- DAS call timed out, stopping")

        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        ouputFile = open(statusDir + '/' + self.fileName,'w')
        datasets = strout.split(os.linesep)
        for dset in datasets:
            dset = dset.strip()
            if dset.startswith('/'):
                ouputFile.write(dset+'\n')

    def readDeprecatedData(self):
        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        inputFile = open(statusDir + '/' + self.fileName,'r')
        
        for line in inputFile.xreadlines():
            datasetName = line.strip()
            self.deprecated[datasetName] = 1
            
        totalSets = len(self.deprecated)
        print " -- found " + str(totalSets) +" deprecated sets"
                
    def getDeprecatedSets(self):
        return self.deprecated

    def isDeprecated(self,dset):
        if dset in self.deprecated:
            return True
        else:
            return False




