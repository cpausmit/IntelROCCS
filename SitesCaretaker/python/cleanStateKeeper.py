#===================================================================================================
#  C L A S S
#===================================================================================================
import sys, os, subprocess, re, time, shutil, string, signal, glob
import datetime

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class CleanStateKeeper:
    def __init__(self):

        if not os.environ.get('CARETAKER_DB'):
            raise Exception(' FATAL -- CARETAKER environment not defined: source setup.sh\n')

        self.basedir = os.environ['CARETAKER_DB']+'/'+os.environ['CARETAKER_TRDIR']

        self.siteDeletions = {}
        self.siteTransfers = {}
        self.dsetTransfers = {}
        self.sitePendings = {}

        self.dsetPlaces = {}
        self.deletionFiles = {}
        self.startTranferTime = {}
        self.dateNow = datetime.datetime.now()

        self.olderRequests()
        self.extractDetoxDatasets()

    def olderRequests(self):
        for fileName in glob.glob(self.basedir+'/*'):
            lastpart = fileName.split('/')[-1]
            siteFrom = lastpart.split('-')[0]
            siteTo   = lastpart.split('-')[1]

            if siteFrom not in self.deletionFiles:
                self.deletionFiles[siteFrom] = []
            self.deletionFiles[siteFrom].append(fileName)

            crtime = os.path.getmtime(fileName)
            crdate = datetime.datetime.fromtimestamp(crtime)
            self.startTranferTime[siteFrom] = crdate

            if siteFrom not in self.siteDeletions:
                self.siteDeletions[siteFrom] = []
            if siteTo not in self.siteTransfers:
                self.siteTransfers[siteTo] = []

            fileIn = open(fileName,'r')
            for line in fileIn:
                line = line.strip()
                self.siteDeletions[siteFrom].append(line)
                self.siteTransfers[siteTo].append(line)
                self.dsetTransfers[line] = siteTo
            fileIn.close()

    def extractDetoxDatasets(self):
        webServer = os.environ.get('CARETAKER_DETOXWEB') + '/status/'
        url = '"' + webServer + '/DatasetsInPhedexAtSites.dat' + '"'
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
            raise Exception(" FATAL -- Call to Detox web timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to Detox web failed, stopping")

        lines = mystring.splitlines()
        for li in lines:
            items = li.split()
            if len(items) < 5:
                continue
            dsetname = items[0]
            group = items[1]
            valid = int(items[5])
            sitename = items[7]
            if sitename in self.siteDeletions:
                continue
            if group != 'AnalysisOps':
                continue
            if valid != 1:
                continue

            if dsetname in self.dsetPlaces:
                self.dsetPlaces[dsetname].append(sitename)
            else:
                self.dsetPlaces[dsetname] = [sitename]

    def processPending(self):
        for siteFrom in self.siteDeletions:
            daysPassed = (self.dateNow - self.startTranferTime[siteFrom]).days
            printedLine = ""

            datasets = self.siteDeletions[siteFrom]
            transfd = 0
            for dset in datasets:
                # check if dataset already tranferred or not
                if dset in self.dsetPlaces:
                    transfd = transfd + 1
                else:
                    siteTo = self.dsetTransfers[dset]
                    if siteTo not in self.sitePendings:
                        self.sitePendings[siteTo] = []
                    self.sitePendings[siteTo].append(dset)
                    if (daysPassed) > 2:
                        newLine = " !! " + siteFrom + ' --> ' + siteTo + " might be stuck !!"
                        if newLine != printedLine :
                            printedLine = newLine
                            print "\n " + printedLine + "\n"
                        print "  " + dset

            allSets = len(datasets)
            print "\n " + siteFrom
            print " -- Need to offload " + str(allSets) + " datasets"
            print " -- done with " + str(transfd)
            if float(allSets-transfd)/allSets < 0.1:
                # means all sets are found on other sites
                # can delete the file and proceed
                print " -- All datasets for " + siteFrom + " are accounted --"
                allFiles = self.deletionFiles[siteFrom]
                for fileName in allFiles:
                    print "  -deleting file " + fileName 
                    os.remove(fileName)

        return self.sitePendings

    def pendingSets(self):
        datasets = []
        for site in self.sitePendings:
            dsets = self.sitePendings[site]
            for dset in dsets:
                datasets.append(dset)
        return datasets

    def canProceed(self):
        if len(self.sitePendings.keys()) > 3:
            return False
        else :
            return True
