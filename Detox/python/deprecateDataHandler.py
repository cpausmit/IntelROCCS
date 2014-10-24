#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, MySQLdb, json
import datetime, time, urllib2
import phedexDataset, phedexApi

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
       deltaNhours = datetime.timedelta(seconds = 60*60)
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
        deprecated = self.getDatasets('DEPRECATED')
        deleted    = self.getDatasets('DELETED')
        invalid    = self.getDatasets('INVALID')
        
        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        outputFile = open(statusDir + '/' + self.fileName,'w')
        
        for dset in deprecated:
            outputFile.write(dset+'\n')
        for dset in deleted:
            outputFile.write(dset+'\n')
        #for dset in invalid:
        #    outputFile.write(dset+'\n')
        outputFile.close()

    def getDatasets(self,pattern):

        webServer = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
        args = 'datasets?dataset_access_type=' + pattern
        url = webServer + args

        authHandler = phedexApi.HTTPSGridAuthHandler()
        opener = urllib2.build_opener(authHandler)
        request = urllib2.Request(url)

        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(2*60)  # 2 minutes
        try:
            response = opener.open(request)
            data = json.load(response)
            signal.alarm(0)
        except:
            print "Failed to retrcieve deprecated datasets"
            return
            #raise Exception(" FATAL -- DAS call timed out, stopping")

        datasets = []
        for dset in data:
            datasets.append(dset['dataset'])
        return datasets
                           
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




