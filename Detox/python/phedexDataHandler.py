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

class PhedexDataHandler:
    def __init__(self,allSites):
        self.phedexDatasets = {}
        self.allSites = allSites

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

    def extractPhedexData(self,federation):
        webServer = 'https://cmsweb.cern.ch/'
        phedexBlocks = 'phedex/datasvc/json/prod/blockreplicas'
        args = 'show_dataset=y&subscribed=y&node=' + federation + '*'

        cert = os.environ['DETOX_X509UP']
        url = '"'+webServer+phedexBlocks+'?'+args+'"'
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
        os.remove(tmpname)

        dataJson = json.loads(strout)
        datasets = (dataJson["phedex"])["dataset"]
        for dset in datasets:
            datasetName = dset["name"]

            user = re.findall(r"USER",datasetName)
            blocks = dset["block"]
            for block in blocks:
                replicas = block["replica"]
                for siterpl in replicas:

                    group = siterpl["group"]
                    if group == 'IB RelVal':
                        group = 'IB-RelVal'
#                    if group != "AnalysisOps":
#                        continue

                    site = str(siterpl["node"])
                    if site not in self.allSites:
                        continue
                    if self.allSites[site].getStatus() == 0:
                        continue

                    if datasetName not in self.phedexDatasets:
                        self.phedexDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
                    dataset = self.phedexDatasets[datasetName]

                    size = float(siterpl["bytes"])/1000/1000/1000
                    compl = siterpl["complete"]
                    cust = siterpl["custodial"]
                    subs = int(float(siterpl["time_create"]))
                    made = int(float(siterpl["time_update"]))
                    files = int(siterpl["files"])
                    iscust = 0

                    if len(user) > 0 or cust == 'y':
                        iscust = 1
                    valid = 1
                    if compl == 'n' and (made-subs) < 60*24*14:
                        valid = 0

                    dataset.updateForSite(site,size,group,made,files,iscust,valid)


        # Create our local cache files of the status per site
        filename = os.environ['DETOX_PHEDEX_CACHE']
        outputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                          + filename, "w")
        for datasetName in self.phedexDatasets:
            line = self.phedexDatasets[datasetName].printIntoLine()

            # CP- Max I do not understand this test, what are you trying to catch here?
            if len(line) < 10:
                print " SKIPING " + datasetName
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
        inputFile.close()

    def findIncomplete(self):
        for datasetName in self.phedexDatasets:
            dataset = self.phedexDatasets[datasetName]
            dataset.findIncomplete()

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
        return sorted(dsets,key=dsets.get,reverse=True)




