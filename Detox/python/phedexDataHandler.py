#====================================================================================================
#  C L A S S
#====================================================================================================

import os, subprocess, re, signal, sys, MySQLdb
import datetime, time
import phedexDataset


class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm
	
class PhedexDataHandler:
    def __init__(self):
        self.phedexDatasets = {}
   
    def shouldAccessPhedex(self):
        # number of hours until it will rerun
        renewMinInterval = int(os.environ.get('DETOX_CYCLE_HOURS'))     
        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        filename = os.environ['DETOX_PHEDEX_CACHE']
        
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(renewMinInterval-1))
        modTime = datetime.datetime.fromtimestamp(0)
        if os.path.isfile(statusDir+'/'+filename):
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(statusDir+'/'+filename))

            #also check that the file is not empty
            if not os.path.getsize(statusDir+'/'+filename) > 0:
                return True

        if (timeNow-deltaNhours) < modTime:
            return False
        
        return True
    
    def extractPhedexData(self,federation,allSites):

        webServer = 'https://cmsweb.cern.ch/'
        phedexBlocks = 'phedex/datasvc/xml/prod/blockreplicas'
        args = 'subscribed=y&node=' + federation + '*'
        
        cert = os.environ['DETOX_X509UP']
        url = '"'+webServer+phedexBlocks+'?'+args+'"'
        #cmd = 'curl --cert ' + cert + ' -k -H "Accept: text/xml" ' + url
        cmd = 'curl -k -H "Accept: text/xml" ' + url
    
        print ' Access phedexDb: ' + cmd
        tmpname = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/tmp.txt'
        tmpfile = open(tmpname, "w")

        process = subprocess.Popen(cmd, stdout=tmpfile, stderr=subprocess.PIPE,
                                   bufsize=4096,shell=True)

        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(30*60)  # 30 minutes
        try:
            strout, error = process.communicate()
            tmpfile.close()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to PhEDEx timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to PhEDEx failed, stopping")
     
        tmpfile = open(tmpname, "r")
        strout = tmpfile.readline()
        tmpfile.close()
        #os.remove(tmpname)

        records = re.split("block\ bytes",strout)
        for line in records:
            
            if "name" not in line:
                continue
            
            datasetName = (re.findall(r"name='(\S+)'", line))[0]
            
            if "#" in datasetName:
                datasetName = (re.split("#",datasetName))[0]

            if datasetName not in self.phedexDatasets:
                self.phedexDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
            dataset = self.phedexDatasets[datasetName]

            sublines = re.split("<replica\ ",line)
            for subline in sublines[1:]:
            
                group = None
                size = None
                creationTime = None
                isValid = 1
                isCustodial = 0
            
                t2Site = re.findall(r"node='(\w+)'",subline)[0]
                if t2Site not in allSites:
                    continue
                if allSites[t2Site].getStatus() == 0:
                    continue
                
                res = re.findall(r"group='(\S+)'",subline)
                if len(res) > 0:
                    group = res[0]
                else:
                    group = "undef"

                if group != "AnalysisOps":
                    continue
                    
                custodial = re.findall(r"custodial='(\w+)'",subline)
                if custodial == 'y':
                    isCustdial = 1
    
                complete = re.findall(r"complete='(\w+)'",subline)
                if complete == 'n':
                    isValid = 0

                user = re.findall(r"USER",datasetName)
                if len(user) > 0:
                    isCustodial = 1
    
                nfiles = int(re.findall(r"files='(\d+)",subline)[0])
                creationTime = int(re.findall(r"time_update='(\d+)",subline)[0])
                size   = float(re.findall(r"bytes='(\d+)'",subline)[0])
                size   = size/1024/1024/1024

                dataset.updateForSite(t2Site,size,group,creationTime,nfiles,isCustodial,isValid)

        # Create our local cache files of the status per site
        filename = os.environ['DETOX_PHEDEX_CACHE']
        outputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                          + filename, "w")
        for datasetName in self.phedexDatasets:
            line = self.phedexDatasets[datasetName].printIntoLine()
            if(len(line) < 10):
                continue
            outputFile.write(line)
            
        outputFile.close()

    def readPhedexData(self):

        filename = os.environ['DETOX_PHEDEX_CACHE']
        inputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                         + filename, "r")
        
        for line in inputFile.xreadlines():
            items = line.split()
            datasetName = items[0]
            
            if datasetName not in self.phedexDatasets:
                self.phedexDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
                
            dataset = self.phedexDatasets[datasetName]
            dataset.fillFromLine(line)
        inputFile.close();

    def findIncomplete(self):
        print "### These datasets have diffrent sizes at different sites:"
        for datasetName in self.phedexDatasets:
            dataset = self.phedexDatasets[datasetName]
            dataset.findIncomplete()
        print "###" 

    def getPhedexDatasets(self):
        return self.phedexDatasets

    def getPhedexDatasetsAtSite(self,site):
        dsets = []
        for datasetName in self.phedexDatasets.keys():
            dataset = self.phedexDatasets[datasetName]
            if dataset.isOnSite(site):
                dsets.append(self.phedexDatasets[datasetName])
        return dsets

    def getDatasetsAtSite(self,site):
        dsets = []
        for datasetName in self.phedexDatasets.keys():
            dataset = self.phedexDatasets[datasetName]
            if dataset.isOnSite(site):
                dsets.append(datasetName)
        return dsets

    def getDatasetsByRank(self,site):
        dsets = {}
        for datasetName in self.phedexDatasets.keys():
            dataset = self.phedexDatasets[datasetName]
            if dataset.isOnSite(site):
                dsets[datasetName] = dataset.getLocalRank(site)

  #      sorted(dsets,cmp=compareByRank)
        return sorted(dsets,key=dsets.get,reverse=True)

    def checkDataComplete(self):
        #this is not called at the moment because there is no reason to do so
        
        #we will access local dataset that has info from central database
        #checks if dataset is indeed complete

        db = os.environ.get('DETOX_SITESTORAGE_DB')
        server = os.environ.get('DETOX_SITESTORAGE_SERVER')
        user = os.environ.get('DETOX_SITESTORAGE_USER')
        pw = os.environ.get('DETOX_SITESTORAGE_PW')
        db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
        cursor = db.cursor()

        sql = "select  Datasets.DatasetName,DatasetProperties.NFiles from DatasetProperties,Datasets "
        sql = sql + "where DatasetProperties.DatasetId=Datasets.DatasetId "
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                name = row[0]
                nFilesDb = int(row[1])
                nFiles = 0
                if name in self.phedexDatasets:
                    nFiles = self.phedexDatasets[name].filesGlobal()

                    if nFilesDb > 0 and nFiles < nFilesDb:
                        
                        print "%3d %5d %-s " %( nFiles, nFilesDb, name)
                
        except:
            pass

        
            

