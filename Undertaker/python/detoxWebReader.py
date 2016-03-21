#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, json, math

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class DetoxWebReader:
    def __init__(self):
        self.webLocation = {}
        self.siteSpace = {}
        self.siteDiskSpace = {}
        self.stuckAtSite = {}
        self.extractDetoxData()
        self.extractAllSiteSizes()
        self.extractStuckDsets()
        self.getWorstStuck()
    
    def extractDetoxData(self):
        webServer = os.environ.get('UNDERTAKER_DETOXWEB') + '/SitesInfo.txt'
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
        readThatBlock = False
        redSomeLines = False
        for li in lines:
            if 'AnalysisOps' in li:
                readThatBlock = True
            if li.startswith('#'):
                if redSomeLines:
                    readThatBlock = False
                continue
            if not readThatBlock:
                continue
            items = li.split()
            siteActive = int(items[0])
            quota      = float(items[1])
            filled     = float(items[2])
            lcopy      = float(items[3])
            siteName   = items[4] 
            
            if siteActive == 0:
                continue
	    if quota > 0: pass
	    else:
		continue
            self.siteSpace[siteName] = (quota,filled,lcopy)
            redSomeLines = True

    def getDatasetsForSite(self,siteName):
        webServer = os.environ.get('UNDERTAKER_DETOXWEB') + '/result/'
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

        readThatBlock = False
        redSomeLines = False
        lines = mystring.splitlines()
        for li in lines:
            if 'AnalysisOps' in li:
                readThatBlock = True
            if li.startswith('#'):
                if redSomeLines:
                    readThatBlock = False
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
            redSomeLines = True
        return datasets

    def getJunkDatasets(self,siteName):
        webServer = os.environ.get('UNDERTAKER_DETOXWEB') + '/result/'
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
            reps = float(items[2])
            name = items[3]
            datasets[name] = 1
        return datasets

    def extractAllSiteSizes(self):

        webServer = os.environ.get('UNDERTAKER_DETOXWEB') + '/status/'
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

    def extractStuckDsets(self):

        webServer = os.environ.get('UNDERTAKER_DETOXWEB')
        url = '"' + webServer +'/TransferStats.txt' + '"'
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
            raise Exception(" FATAL -- Call to TransferStats timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to TransferStats failed, stopping")

        lines = mystring.splitlines()
        for li in lines:
            if li.startswith('#'):
                continue
            items = li.split()
            if len(items) < 4:
                continue
            nstuck = int(items[0])
            siteName = items[3]
            self.stuckAtSite[siteName] = nstuck

    def getFilledwLC(self):
        thesites = []
        for site in sorted(self.siteSpace,self.sort_by_fill):
            quota = self.siteSpace[site][0]
            filled = self.siteSpace[site][1]
            lcopy = self.siteSpace[site][2]
	    if filled < 0.9*quota:
                continue

	    #if site.startswith('T2_') and lcopy > 0.9*filled :
            #    thesites.append(site)
	    #if not site.startswith('T2_'):
		#continue
	    #if filled > quota:
            thesites.append(site)
        return thesites

    def sort_by_fill(self, a, b):
	quota = self.siteSpace[a][0]
        filled = self.siteSpace[a][1]
	fA = filled/quota
	quota = self.siteSpace[b][0]
        filled = self.siteSpace[b][1]
	fB = filled/quota
	if fA > fB: return 1
	else:       return -1

    def getSiteSpace(self):
        return self.siteSpace
    
    def siteDiskUsage(self,site):
        if site not in self.siteDiskSpace:
            return 0
        return self.siteDiskSpace[site]

    def getLeastFilled(self):
	l = []
	for site in sorted(self.siteSpace,sort_by_fill,reverse=True):
	    l.append(site)
	return l

    def getWorstStuck(self):
        count = 0
        aa = self.stuckAtSite
        worstFive = dict(sorted(aa.items(),key = lambda x: x[1],reverse=True)[:5])
        return worstFive
