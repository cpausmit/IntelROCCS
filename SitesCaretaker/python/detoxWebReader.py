#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, json

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class DetoxWebReader:
    def __init__(self):
        self.webLocation = {}
        self.siteSpace = {}
        self.siteDiskSpace = {}
        self.extractDetoxData()
        self.extractAllSiteSizes()
    
    def extractDetoxData(self):
        webServer = os.environ.get('CARETAKER_DETOXWEB') + '/SitesInfo.txt'
        url = '"' + webServer + '"'
        cmd = 'curl -k -H "Accept: text" ' + url
        print ' Access Detox Web:\n ' + cmd

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to DeadSites timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to DeadSites failed, stopping")

        lines = mystring.splitlines()
        for li in lines:
            if li.startswith('#'):
                continue
            items = li.split()
            siteActive = int(items[0])
            quota      = float(items[1])
            filled     = float(items[2])
            lcopy      = float(items[3])
            siteName   = items[4] 
            
            if siteActive == 0:
                continue
            self.siteSpace[siteName] = (quota,filled,lcopy)

    def getDatasetsForSite(self,siteName):
        webServer = os.environ.get('CARETAKER_DETOXWEB') + '/result/'
        url = '"' + webServer + siteName +'/RemainingDatasets.txt' + '"'
        cmd = 'curl -k -H "Accept: text" ' + url
        #print ' Access Detox Web:\n' + cmd

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to DeadSites timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to DeadSites failed, stopping")

        datasets = {}
        if mystring.find('Not Found') != -1 :
            return datasets
        lines = mystring.splitlines()
        for li in lines:
            if li.startswith('#'):
                continue
            items = li.split()
            if len(items) < 5:
                continue
            rank = float(items[0])
            size = float(items[1])
            reps = float(items[3])
            name = items[4]
            if reps > 1:
                continue
            datasets[name] = (rank,size)
        return datasets

    def getJunkDatasets(self,siteName):
        webServer = os.environ.get('CARETAKER_DETOXWEB') + '/result/'
        url = '"' + webServer + siteName +'/DeprecatedSets.txt' + '"'
        cmd = 'curl -k -H "Accept: text" ' + url
        #print ' Access Detox Web:\n' + cmd

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to DeadSites timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to DeadSites failed, stopping")

        datasets = {}
        if mystring.find('Not Found') != -1 :
            return datasets
        lines = mystring.splitlines()
        for li in lines:
            if li.startswith('#'):
                continue
            items = li.split()
            if len(items) < 4:
                continue
            reps = float(items[3])
            name = items[3]
            datasets[name] = 1
        return datasets

    def extractAllSiteSizes(self):

        webServer = os.environ.get('CARETAKER_DETOXWEB') + '/status/'
        url = '"' + webServer +'/DatasetsInPhedexAtSites.dat' + '"'
        cmd = 'curl -k -H "Accept: text" ' + url

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to DeadSites timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to DeadSites failed, stopping")

        lines = mystring.splitlines()
        for li in lines:
            if not li.startswith('/'):
                continue
            items = li.split()
            if len(items) < 5:
                continue
            datasetName = items[0]
            group = items[1]
            if group != 'AnalysisOps':
                continue
            size = float(items[3])
            site = items[7]
            if site not in self.siteDiskSpace:
                self.siteDiskSpace[site] = 0
            self.siteDiskSpace[site] = self.siteDiskSpace[site] + size

    def getSiteSpace(self):
        return self.siteSpace
    
    def siteDiskUsage(self,site):
        if site not in self.siteDiskSpace:
            return 0
        return self.siteDiskSpace[site]
