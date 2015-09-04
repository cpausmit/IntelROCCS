#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, MySQLdb, json
import datetime, time, json, httplib
import phedexDataset
import phedexApi

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class LockedDataset:
    def __init__(self,name):
        self.datasetName = name
        self.entries = []
        self.lockedAtSites = {}
        self.blocks = {}

    def appendEntry(self,lineHash, fullName):
        self.entries.append(lineHash)
        parts = fullName.split('#')
        if len(parts) > 1:
            site = lineHash['site']
            self.blocks[parts[1]] = site

    def processEnrties(self):
        for item in self.entries:
            status = item['lock']
            site = item['site']

            if status == True: 
                self.lockedAtSites[site] = True
            else:
                self.lockedAtSites[site] = False

    def getLockedSites(self):
        sites = []
        for site in self.lockedAtSites:
            if self.lockedAtSites[site] == True:
                sites.append(site)
        return sites

    def getLockedBlocks(self):
        blocks = []
        for block in self.blocks:
            site =  self.blocks[block]
            if self.lockedAtSites[site] == True:
                blocks.append(block)
        return blocks

    def getLockedBlocksAtSite(self,siteName):
        blocks = []
        for block in self.blocks:
            site =  self.blocks[block]
            if site != siteName:
                continue
            if self.lockedAtSites[site] == True:
                blocks.append(block)
        return blocks

class PhedexDataHandler:
    def __init__(self,allSites):
        self.newAccess = False
        self.phedexGroups = (os.environ['DETOX_GROUP']).split(',')
        self.lockfile = "result/DetoxLockfile.json"
        self.phedexDatasets = {}
        self.otherDatasets = {}
        self.runAwayGroups = {}
        self.allSites = allSites
        self.localyLocked = {}
        self.makeLockFile()

        self.lockedSets = {}
        lockFiles =  (os.environ['DETOX_LOCKFILES']).split(',')

        for lockF in lockFiles:
            self.getLockInformation(lockF)

        self.epochTime = int(time.time())

    def shouldAccessPhedex(self):
        # number of hours until it will rerun
        renewMinInterval = int(os.environ.get('DETOX_CYCLE_HOURS'))
        statusDir = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        fileName = statusDir + '/' + os.environ['DETOX_PHEDEX_CACHE']
        if not os.path.isfile(fileName):
            return True
        if not os.path.getsize(fileName) > 0:
            return True

        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(renewMinInterval-1))
        modTime = datetime.datetime.fromtimestamp(os.path.getmtime(fileName))
        if (timeNow-deltaNhours) < modTime:
            print "  -- last time cache renewed on " + str(modTime)
            return False
        return True

    def extractPhedexData(self,federation):
        if self.shouldAccessPhedex() :
            try:
                self.retrievePhedexData(federation)
                self.newAccess = True
            except:
                raise
        else:
            print "  -- reading from cache --"

        self.readPhedexData()

    def retrievePhedexData(self,federation):
        phedexDatasets = {}
        webServer = 'https://cmsweb.cern.ch/'
        phedexBlocks = 'phedex/datasvc/json/prod/blockreplicas'
        args = 'show_dataset=y&subscribed=y&' + federation

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
        #os.remove(tmpname)
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

                    site = str(siterpl["node"])

                    if datasetName not in phedexDatasets:
                        phedexDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
                    dataset = phedexDatasets[datasetName]

                    size = float(siterpl["bytes"])/1000/1000/1000
                    strdone = siterpl["complete"]
                    isdone = 1
                    if strdone == 'n' :
                        isdone = 0
                    cust = siterpl["custodial"]
                    reqtime = int(float(siterpl["time_create"]))
                    updtime = int(float(siterpl["time_update"]))
                    files = int(siterpl["files"])
                    
                    iscust = 0
                    if len(user) > 0 or cust == 'y':
                        iscust = 1
                    if 'GenericTTbar' in datasetName:
                        iscust = 1
		    #if 'MinBias' in datasetName:
		    #    iscust = 1

                    dataset.updateForSite(site,size,group,files,iscust,reqtime,updtime,isdone)

        del dataJson
        # Create our local cache files of the status per site
        filename = os.environ['DETOX_PHEDEX_CACHE']
        outputFile = open(os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/'
                          + filename, "w")
        for datasetName in phedexDatasets:
            phedexDatasets[datasetName].setFinalValues()
            line = phedexDatasets[datasetName].printIntoLine()
            #any dataset line should be above 10 characters
            if len(line) < 10:
                print " SKIPING " + datasetName
                continue
            outputFile.write(line)
        outputFile.close()

    def readPhedexData(self):
        filename = os.environ['DETOX_PHEDEX_CACHE']
        fileName = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + filename
	if not os.path.exists(fileName):
	    return phedexDatasets

        phedGroups = []
        for group in self.phedexGroups:
            subgroups = group.split('+')
            phedGroups.extend(subgroups)

	inputFile = open(fileName,'r')
        for line in inputFile.xreadlines():
            items = line.split()
            datasetName = items[0]
            group = items[1]
            siteName = items[5]
            size = float(items[2])

	    if siteName not in self.allSites:
                continue
            if self.allSites[siteName].getStatus() == 0:
                continue
            if group == 'local':
                continue

            dataset = None
            if group not in phedGroups:
                if datasetName not in self.otherDatasets:
                    self.otherDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
                dataset = self.otherDatasets[datasetName]
            else:
                if datasetName not in self.phedexDatasets:
                    self.phedexDatasets[datasetName] = phedexDataset.PhedexDataset(datasetName)
                dataset = self.phedexDatasets[datasetName]
            dataset.fillFromLine(line)
            dataset.setFinalValues()
        inputFile.close()

        #if self.phedexGroup == 'AnalysisOps':
        #    self.printRunawaySets()

    def getPhedexDatasets(self):
        return self.phedexDatasets

    def getLockedDatasets(self):
        return self.lockedSets

    def isLocalyLocked(self,site,dset):
        if site in self.localyLocked:
            if dset in self.localyLocked[site]:
                return True
        return False

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


    def renewedCache(self):
        return self.newAccess

    def getRunAwayGroups(self,site):
        groups = []
        if site in self.runAwayGroups:
            groups = self.runAwayGroups[site]
        return groups

    def getRunAwaySets(self,site):
        runAway = {}
        for dset in self.phedexDatasets:
            if dset not in self.otherDatasets:
                continue

            dataset = self.otherDatasets[dset]
            if not dataset.isOnSite(site):
                continue
            group = dataset.group(site)
            runAway[dset] = group
        return runAway

    def printRunawaySets(self):
        siteSizes = {}
        siteSets = {}
        for dset in self.phedexDatasets:
            if dset not in self.otherDatasets:
                continue

            dataset = self.otherDatasets[dset]
            for site in dataset.siteNames:
                group = dataset.group(site)
                size  = dataset.size(site)
                if site not in siteSizes:
                    siteSizes[site] = 0
                    siteSets[site] = 0
                siteSizes[site] = siteSizes[site] + size/1000
                siteSets[site] = siteSets[site] + 1
                if site not in self.runAwayGroups:
                    dsetName = dataset.dataset
                    if 'AOD' in dsetName:
#                        if group != 'RelVal' and group != 'DataOps':
                         if not site.startswith('T1_'):
                            if group == 'local':
                               continue
                            print site
                            print dataset.dataset
                            print group + '--> AnalysisOps'
                            phedex = phedexApi.phedexApi(logPath='./')
                            check,response = phedex.changeGroup(site,dsetName,'AnalysisOps')
                            if check:
                                print " ERROR - phedexApi.updateRequest failed"
                                print response
                                del phedex
                            else:
                                del phedex
                                continue

                    self.runAwayGroups[site] = [group]
                else:
                    if group not in self.runAwayGroups[site]:
                        self.runAwayGroups[site].append(group)

        if (len(siteSizes) < 1):
            return
        print " !! WARNING !! - those sites have datasets in wrong groups"
        for site in sorted(siteSizes):
            print ' %3d %6.2f TB'%(siteSets[site],siteSizes[site]) + ": " + site

    def getLockInformation(self, url):
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
            raise Exception(" FATAL -- Call to Lock File timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to Lock File failed, stopping")
        
        dataJason = json.loads(mystring)
        for site in dataJason:
            if site == 'lastupdate':
                continue
            datasets = dataJason[site]
            for dset in datasets:
                temphash = datasets[dset]
                temphash['site'] = site

                names = dset.split('#')
                datasetName = names[0]

                if datasetName not in self.lockedSets:
                    self.lockedSets[datasetName] = LockedDataset(datasetName)
                    #print 'locking ' + datasetName
                #print temphash
                self.lockedSets[datasetName].appendEntry(temphash,dset)
    
        for dset in self.lockedSets:
            self.lockedSets[dset].processEnrties()
        del dataJason
                

    def makeLockFile(self):        
        lockfile = "result/DetoxLockfile.json"
        reqman_server = "cmsweb.cern.ch"
        reqman_url = "/reqmgr2/data/request"

        active_states = ['assigned','acquired','running-open','running-closed','completed',
                         "force-complete","closed-out"]

        proxy = os.environ['DETOX_X509UP']

        #it fails connection from time to time, try 3 times 
        for itry in range(0,5):
            connection = httplib.HTTPSConnection(reqman_server,cert_file = proxy,key_file = proxy)
            # connection.set_debuglevel(1)
            headers = {"Content-type": "application/x-www-form-urlencoded",
                       "Accept": "application/json"}
            params = "&".join("status="+x for x in active_states)
            url = "%s?%s"%(reqman_url,params)
            connection.request('GET',url,"",headers)
            response = connection.getresponse()
            if response.status == 200:
                break
            print '  - bad responce, retrying local lock file'
            time.sleep(5)

        info = json.loads(response.read())
        data = info['result'][0]
    
        site_locks = dict()
        for workflow,workflow_info in data.iteritems():
            # skip workflows with missing information
            if not 'SiteWhitelist' in workflow_info: continue

            # extract active dataset names
            datasets = dict()
            for param in ['InputDataset','InputDatasets','OutputDatasets','MCPileup','DataPileup']:
                if param in workflow_info:
                    entry = workflow_info[param]
                    if isinstance(entry, basestring):
                        datasets[entry] = 1
                    else:
                        for ds in entry:
                            datasets[ds] = 1
        
            # update site lock list
            site_whitelist = workflow_info['SiteWhitelist']
            for site in site_whitelist:
                self.localyLocked [site] = []
                for ds in datasets:
                    if not site in site_locks:
                        self.localyLocked[site].append(ds)
                        site_locks[site] = dict()
                    site_locks[site][ds] = {"lock":True}
                
        # store results
        with open(self.lockfile,"w") as f:
            json.dump(site_locks,f,indent=2)
        del info
        print " Lock file has been succcesfully created: %s" % self.lockfile
        print "  Number of active workflows: %d" % len(data)
