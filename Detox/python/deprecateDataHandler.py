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
        self.fileName = 'DeprecatedSets.txt'
        self.deprecated = {}


    def shouldAccessDas(self):
       statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
       fileName = statusDir + '/' + self.fileName

       if not os.path.isfile(fileName):
           return True
       #also check that the file is not empty
       if not os.path.getsize(fileName) > 0:
           return True

       timeNow = datetime.datetime.now()
       deltaNhours = datetime.timedelta(seconds = 60*60*24*7)
       modTime = datetime.datetime.fromtimestamp(os.path.getmtime(fileName))
       print "  -- last time cache renewed on " + str(modTime)
       if (timeNow-deltaNhours) < modTime:
           return False
       return True

    def extractDeprecatedData(self):
        accessDas = self.shouldAccessDas()
        if accessDas:
            try:
                self.retrieveDeprecatedData()
            except:
                raise
        else:
            print "  -- reading from cache --"
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
        outputFile = open(statusDir + '/' + self.fileName,'w')
        datasets = strout.split(os.linesep)
        for dset in datasets:
            dset = dset.strip()
            if dset.startswith('/'):
                outputFile.write(dset+'\n')
        outputFile.close()

    def readDeprecatedData(self):
        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        inputFile = open(statusDir + '/' + self.fileName,'r')
        
        for line in inputFile.xreadlines():
            datasetName = line.strip()
            self.deprecated[datasetName] = 1
        inputFile.close()
                
    def getDeprecatedSets(self):
        return self.deprecated

    def isDeprecated(self,dset):
        if dset in self.deprecated:
            return True
        else:
            return False




