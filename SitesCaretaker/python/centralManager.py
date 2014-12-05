#===================================================================================================
#  C L A S S
#===================================================================================================
import sys, os, subprocess, re, time, datetime, smtplib, shutil, string, glob
import siteStatus,siteReadinessHandler,detoxWebReader, cleanStateKeeper, dbInfoHandler
import phedexApi

class CentralManager:
    def __init__(self):

        if not os.environ.get('CARETAKER_DB'):
            raise Exception(' FATAL -- DETOX environment not defined: source setup.sh\n')

        self.sitesToDisable = {}
        self.sitesToEnable = {}
        self.siteSizeShift = {}

        self.dbInfoHandler = dbInfoHandler.DbInfoHandler()
        self.siteReadinessHandler = siteReadinessHandler.SiteReadinessHandler()
	self.detoxWebReader = detoxWebReader.DetoxWebReader()
        self.cleanStateKeeper = cleanStateKeeper.CleanStateKeeper()

        self.allSites =  self.dbInfoHandler.getAllSites()
        self.basedir = os.environ['CARETAKER_DB']
        self.currentDate = datetime.datetime.now().date()

    def extractReadinessData(self):
        try:
            self.siteReadinessHandler.extractReadinessData()
        except:
            self.sendEmail("Problems detected while running Sites Caretaker",\
                               "Execution was terminated, check log to correct problems.")
            raise

    def findStateChanges(self):

        print ""
        print "W A I T I N G  R O O M:"
        for site in sorted(self.allSites):
            siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
            if siteInfo.inWaitingRoom() :
                print " - " + site
        print ""
        print "G R A V E  Y A R D:"
        for site in sorted(self.allSites):
            siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
            if siteInfo.isDead() :
                print " - " + site

        print "\nSites that need to be disabled:"
        for site in sorted(self.allSites):
            siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
            dbStatus = self.allSites[site].getStatus()
            isDead    = siteInfo.isDead() 
            whenDead  = siteInfo.declaredDead()
            if dbStatus == 1 and isDead == True:
                print " - " + site
                print "   declared dead on " + str(whenDead)
                self.sitesToDisable[site] = self.allSites[site]

        print "\nSites that need to be enabled:"
        for site in sorted(self.allSites):
            siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
            dbStatus = self.allSites[site].getStatus()
            isDead  = siteInfo.isDead()
            lastTimeDead = siteInfo.lastTimeDead()

            if dbStatus == 0 and isDead == False:
                if (self.currentDate - datetime.timedelta(days=14)) > lastTimeDead :
                    print " - " + site
                    print "   became live on " + str(lastTimeDead)
                    self.sitesToEnable[site] = self.allSites[site]
        
        print "\nNeed to be added into the Detox database"
        for site in self.siteReadinessHandler.getSites():     
            if site not in self.allSites:
                siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
                isDead  = siteInfo.isDead()
                lastTimeDead = siteInfo.lastTimeDead()

                if isDead == False:
                    if (self.currentDate - datetime.timedelta(days=14)) > lastTimeDead :   
                        print " - " + site

    def enableSites(self):
        if len(self.sitesToEnable.keys()) < 1:
            return

        sitesPending = {}
        self.deletion = self.basedir + '/' + os.environ['CARETAKER_TRDIR']
        for fileName in glob.glob(self.deletion+'/*'):
            siteName = fileName.split('-')[1]
            sitesPending[siteName] = 1

        for site in self.sitesToEnable:
            if site in sitesPending:
                continue
            #determine if there are datasets and site quota
            # at 89% so that nothing happns to the site
            spaceUsed = self.detoxWebReader.siteDiskUsage(site)
            targetQuota = spaceUsed/1000*1.1
            
            self.dbInfoHandler.enableSite(site,targetQuota)

    def processPending(self):
        pending = self.cleanStateKeeper.processPending()
        for siteName in pending:
            datasetSizes = self.dbInfoHandler.extractDatasetSizes(pending[siteName])
            sizeShift = 0
            for dset in datasetSizes:
                sizeShift = sizeShift + datasetSizes[dset]
            self.siteSizeShift[siteName] = sizeShift

    def resignDatasets(self):
        #get last copy datasets on the dead site
        #assign them to sites with lowest % of last copies
        #make sure that sites can host them
        #make sure to update sites info
        basedir = self.basedir + '/' + os.environ['CARETAKER_TRDIR']
        
        self.siteSpace = self.detoxWebReader.getSiteSpace()
        pendingSets = self.cleanStateKeeper.pendingSets()
        setsToSites = {}
        siteToSites = {}
        for siteName in self.sitesToDisable:
            print "Re-signing datasets for SITE=" + siteName
            siteToSites[siteName] = []
            datasets = self.detoxWebReader.getDatasetsForSite(siteName)
            deprecated = self.detoxWebReader.getJunkDatasets(siteName)

            if len(datasets.keys()) < 1:
                break
            self.dbInfoHandler.setDatasetRanks(datasets)
            for site in sorted(self.siteSpace,cmp=self.localCompare):
                if site in self.sitesToDisable:
                    continue
                if site in self.siteSizeShift:
                    continue
                if site == 'T2_TH_CUNSTDA':
                    continue
                if site == 'T1_US_FNAL_Disk':
                    continue

                #make sure sets do not go the dead or waiting room site
                siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
                dbStatus = self.allSites[site].getStatus() 
                if  dbStatus == 0 or siteInfo.inWaitingRoom() or siteInfo.isDead():
                    continue


                sizeCanTake = (self.siteSpace[site][0]*0.9 - self.siteSpace[site][1])*1000
                addedSize = 0
                addedSets = 0

                for dset in datasets.keys():
                    #part of the datasets might be already in pending request
                    if dset in pendingSets:
                        del datasets[dset]
                        continue
                    #make sure it is not deprectaed set
                    if dset in deprecated:
                        del datasets[dset]
                        continue

                    addedSize = addedSize + datasets[dset][1]
                    datasets[dset][1]
                    if (addedSize > sizeCanTake or addedSize > 40000) and addedSets > 0:
                        addedSize = addedSize - datasets[dset][1]
                        break
                    if site not in setsToSites:
                        setsToSites[site] = [dset]
                    else:
                        setsToSites[site].append(dset)
                    del datasets[dset]
                    addedSets = addedSets + 1
                print " - site " + site + " can take " + str(addedSize)
                siteToSites[siteName].append(site)
                (quota,totalSize,lastCopy) = self.siteSpace[site]
                totalSize = totalSize + addedSize
                lastCopy = lastCopy + addedSize
                self.siteSpace[site] = (quota,totalSize,lastCopy)
                if len(datasets.keys()) < 1:
                    break
            #limit ourselves with one site at a time
            break


        for siteFrom in siteToSites:
            for siteTo in siteToSites[siteFrom]:
                fileOut = open(basedir+'/'+siteFrom+'-'+siteTo,'w')
                dsets = setsToSites[siteTo]
                for dset in dsets:
                    fileOut.write(dset+'\n')
                fileOut.close()

        #now cache files are created, time to submit actuall deletion request
        #disable deletions for now, no need 
        #for siteName in self.sitesToDisable:
            #print "Deleting datasets for SITE=" + siteName
            #datasets = self.detoxWebReader.getDatasetsForSite(siteName)
            #self.submitDeletionRequest(siteName,datasets)

        #deletion requests went out, now time to submit transfer requests
        for site in setsToSites:
            dsets = setsToSites[site]
            self.submitTransferRequest(site,dsets)
        #all requests are out, time to disable site
        for site in self.sitesToDisable:
            self.dbInfoHandler.disableSite(site)
            break

    def submitDeletionRequest(self,site,datasets2del):
        if len(datasets2del) < 1:
            return
        phedex = phedexApi.phedexApi(logPath='./')
        # compose data for deletion request
        check,data = phedex.xmlData(datasets=datasets2del,instance='prod')
        if check:
            print " ERROR - phedexApi.xmlData failed"
            sys.exit(1)
            
        # here the request is really sent
        message = 'IntelROCCS -- Automatic Cache Release Request (next check ' + \
            'in about %s hours).'%(os.environ['DETOX_CYCLE_HOURS']) + \
            ' Summary at: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'
        check,response = phedex.delete(node=site,data=data,comments=message,instance='prod')
        if check:
            print " ERROR - phedexApi.delete failed"
            print response
            sys.exit(1)

        respo = response.read()
        matchObj = re.search(r'"id":"(\d+)"',respo)
        reqid = int(matchObj.group(1))

        rdate = (re.search(r'"request_date":"(.*?)"',respo)).group(1)
        rdate = rdate[:-3]
        self.dbInforhandler.logRequest(site,datasets2del,reqid,rdate,1)

    def submitTransferRequest(self,site,datasets2trans):
        if len(datasets2trans) < 1:
            return
        phedex = phedexApi.phedexApi(logPath='./')
        # compose data for deletion request
        check,data = phedex.xmlData(datasets=datasets2trans,instance='prod')
        if check:
            print " ERROR - phedexApi.xmlData failed"
            print data
            sys.exit(1)
            
        # here the request is really sent
        message = 'IntelROCCS -- Automatic Transfer Request'
        check,response = phedex.subscribe(node=site,data=data,comments=message)
        if check:
            print " ERROR - phedexApi.subscribe failed"
            print response
            sys.exit(1)

        respo = response.read()
        matchObj = re.search(r'"id":"(\d+)"',respo)
        reqid = int(matchObj.group(1))

        rdate = (re.search(r'"request_date":"(.*?)"',respo)).group(1)
        rdate = rdate[:-3]
        self.dbInfoHandler.logRequest(site,datasets2trans,reqid,rdate,0)

    def canProceed(self):
        return self.cleanStateKeeper.canProceed()

    def localCompare(self,a,b):
        sa = self.siteSpace[a]
        sb = self.siteSpace[b]
        #lastCopyPercA = sa[2]/sa[0]
        #lastCopyPercB = sb[2]/sb[0]
        spaceFreeA = sa[0]-sa[1]
        spaceFreeB = sb[0]-sb[1]
        if spaceFreeA >= spaceFreeB:
            return -1
        else:
            return 1

    def printResults(self):
        for site in sorted(self.allSites):
            siteInfo = self.siteReadinessHandler.getSiteReadiness(site)
            if siteInfo.hadProblems():
                print site
                siteInfo.printResults()

    def checkProxyValid(self):
        process = subprocess.Popen(["/usr/bin/voms-proxy-info","-file",
                                    os.environ['CARETAKER_X509UP']],
                                   shell=True,stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT)

        output, err = process.communicate()
        p_status = process.wait()
        if p_status != 0:
            self.sendEmail("Problems detected while running Sites Caretaker",\
                               "Execution was terminated, check log to correct problems.")
            raise Exception(" FATAL -- Bad proxy file " + os.environ['CARETAKER_X509UP'])

        m = (re.findall(r"timeleft\s+:\s+(\d+):(\d+):(\d+)",output))[0]
        hours = int(m[0])
        mins = int(m[1])
        if hours > 0:
            pass
        elif mins < 10:
            self.sendEmail("Problems detected while running Sites Caretaker",\
                               "Execution was terminated, check log to correct problems.")
            raise Exception(" FATAL -- Bad proxy file " + os.environ['CARETAKER_X509UP'])

    def submitUpdateRequest(self,site,reqid):
        # here we brute force deletion to be approved
        phedex = phedexApi.phedexApi(logPath='./')
        check,response = phedex.updateRequest(decision='approve',request=reqid,node=site,instance='prod')
        if check:
            print " ERROR - phedexApi.updateRequest failed - reqid="+ str(reqid)
            print response
        del phedex

    def sendEmail(self,subject,body):
        emails = os.environ['CARETAKER_EMAIL_LIST']
        To = emails.split(",")
        From = "maxi@t3btch039.mit.edu"
        Subj = subject
        Text = "" + body + ""

        Body = string.join((
            "From: %s" % From,
            "To: %s" % To,
            "Subject: %s" % Subj,
            "",
            Text,
            ), "\r\n")

        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(From, To, Body)
        except Exception:
            print "Error: unable to send email"

   
