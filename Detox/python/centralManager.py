#====================================================================================================
#  C L A S S
#====================================================================================================

import sys, os, subprocess, re, time, datetime, smtplib, MySQLdb, shutil, string, glob
import phedexDataHandler, popularityDataHandler
import siteProperties, datasetProperties
import siteStatus
import phedexApi
	
class CentralManager:
    def __init__(self):
        
        if not os.environ.get('DETOX_DB'):
            raise Exception(' FATAL -- DETOX environment not defined: source setup.sh\n')

        self.DETOX_NCOPY_MIN = int(os.environ['DETOX_NCOPY_MIN'])
        self.DETOX_USAGE_MIN = float(os.environ['DETOX_USAGE_MIN'])
        self.DETOX_USAGE_MAX = float(os.environ['DETOX_USAGE_MAX'])

        self.allSites = {}
        self.getAllSites()

        self.sitePropers = {}
        self.dataPropers = {}

        self.phedexHandler = phedexDataHandler.PhedexDataHandler()
        self.popularityHandler = popularityDataHandler.PopularityDataHandler(self.allSites)
        

    def getDbConnection(self,db=os.environ.get('DETOX_SITESTORAGE_DB')):
        # configuration
        #db = os.environ.get('DETOX_SITESTORAGE_DB')
        server = os.environ.get('DETOX_SITESTORAGE_SERVER')
        user = os.environ.get('DETOX_SITESTORAGE_USER')
        pw = os.environ.get('DETOX_SITESTORAGE_PW')
        # open database connection
        connection = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
        # prepare a cursor object using cursor() method
        return connection
    

    def extractPhedexData(self,federation):
        if self.phedexHandler.shouldAccessPhedex() :
            try:
                self.phedexHandler.extractPhedexData(federation,self.allSites)
            except:
                self.sendEmail()
                raise
        else:
            self.phedexHandler.readPhedexData()

        self.phedexHandler.findIncomplete()
        #self.phedexHandler.checkDataComplete()

    def extractPopularityData(self):
        try:
            self.popularityHandler.extractPopularityData()
        except :
            self.sendEmail()
            raise

    def getAllSites(self):
        db = os.environ.get('DETOX_SITESTORAGE_DB')
        server = os.environ.get('DETOX_SITESTORAGE_SERVER')
        user = os.environ.get('DETOX_SITESTORAGE_USER')
        pw = os.environ.get('DETOX_SITESTORAGE_PW')
        table = os.environ.get('DETOX_QUOTAS')

        print ' Access quota table (%s) in site storage database (%s) to find all sites.'%(table,db)
        #db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
        #cursor = db.cursor()
        connection = self.getDbConnection()
        cursor = connection.cursor()
        
        group = "AnalysisOps"
        sql = "select * from " + table + ' where GroupName=\'' + group +'\''
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                siteName = row[0]
                willBeUsed = int(row[4])
                sizeTb = float(row[2])
                siteSizeGb = sizeTb * 1024
                self.allSites[siteName] = siteStatus.SiteStatus(siteName)
                self.allSites[siteName].setStatus(willBeUsed)
                self.allSites[siteName].setSize(siteSizeGb)
        except:
            print ' Error(%s) -- could not retrieve sites'%(sql)
            print sys.exc_info()
            connection.close()
            sys.exit(1)
        # close connection to the database
        connection.close()

        #siteName = "T2_US_CHECK"
        #self.allSites[siteName] = siteStatus.SiteStatus(siteName)
        #self.allSites[siteName].setStatus(1)
        #self.allSites[siteName].setSize(250*1024)

    def checkProxyValid(self):
        process = subprocess.Popen(["/usr/bin/voms-proxy-info","-file",os.environ['DETOX_X509UP'] ],
                                   shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
        output, err = process.communicate()
        p_status = process.wait()
        if p_status != 0:
            self.sendEmail()
            raise Exception(" FATAL -- Bad proxy file " + os.environ['DETOX_X509UP'])

        m = (re.findall(r"timeleft\s+:\s+(\d+):(\d+):(\d+)",output))[0]
        hours = int(m[0])
        mins = int(m[1])
        if hours > 0:
            pass
        elif mins < 10:
            self.sendEmail()
            raise Exception(" FATAL -- Bad proxy file " + os.environ['DETOX_X509UP'])

    def rankDatasetsLocally(self):
        for site in sorted(self.allSites):
            self.rankLocallyAtSite(site)
        
    def rankLocallyAtSite(self,site):
        secsPerDay = 60*60*24
        now = float(time.time())
        phedexSets = self.phedexHandler.getPhedexDatasets()
        usedSets = self.popularityHandler.getUsedDatasets()

        phedexDsetNames = self.phedexHandler.getDatasetsAtSite(site)

        for datasetName in sorted(phedexDsetNames):

            phedexDset = phedexSets[datasetName]
            creationDate = phedexDset.creationTime(site)
            size = phedexDset.size(site)
            used = 0
            nAccessed = 0
            lastAccessed = now

            if datasetName in usedSets.keys():
                if usedSets[datasetName].isOnSite(site):
                    nAccessed = usedSets[datasetName].timesUsed(site)
                    if size > 1:
                        nAccessed = nAccessed/size
                    date = usedSets[datasetName].lastUsed(site)
                    dateTime = date+' 00:00:01'
                    pattern = '%Y-%m-%d %H:%M:%S'
                    lastAccessed = int(time.mktime(time.strptime(dateTime, pattern)))
        
                    if (now-creationDate)/secsPerDay > ((now-lastAccessed)/secsPerDay-nAccessed):
                        used = 1
                        
            # calculate the rank of the given dataset according to its access patterns and size
            datasetRank = (1-used)*(now-creationDate)/(60*60*24) + \
                          used*( (now-lastAccessed)/(60*60*24)-nAccessed) - size/100
            phedexSets[datasetName].setLocalRank(site,datasetRank)

        statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        outputFile = open(statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE'],'w')
        dsets = self.phedexHandler.getDatasetsByRank(site)
        #print site
        #for datasetName in dsets:
        #    print phedexSets[datasetName].rank(site)
        
       
        origFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']
        copyFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']+'-local'
        shutil.copy2(origFile,copyFile)


    def printUsagePatterns(self):
        phedexSets = self.phedexHandler.getPhedexDatasets()
        usedSets = self.popularityHandler.getUsedDatasets()

        today = time.time()
        for datasetName in sorted(phedexSets.keys()):
            phedexDset = phedexSets[datasetName]
            siteNames = phedexDset.locatedOnSites()
            nSites = 0
            nFiles = 0
            size = 0
            nUsed = 0
            for site in siteNames:
                if self.allSites[site].getStatus() == 0:
                    continue
                crDate = phedexDset.creationTime(site)
                #pick datasets based on when they were created
                if (today-crDate) < 90*24*60*60 :
                    continue

                nSites = nSites + 1
                size = phedexDset.size(site)
                nFiles = phedexDset.getNfiles(site)
                if datasetName in usedSets.keys():
                    usedDset = usedSets[datasetName]
                    if usedDset.isOnSite(site):
                        nUsed = nUsed + usedDset.timesUsed(site)
            if nSites > 0:
                print str(nSites) + " " + str(size) + " " + str(nFiles) + " " + str(nUsed)
        
    def rankDatasetsGlobally(self):
        secsPerDay = 60*60*24
        now = float(time.time())
        phedexSets = self.phedexHandler.getPhedexDatasets()
        usedSets = self.popularityHandler.getUsedDatasets()

        for datasetName in sorted(phedexSets.keys()):
            
            phedexDset = phedexSets[datasetName]
            
            siteNames = phedexDset.locatedOnSites()
            globalRank = 0
            nSites = 0
            for site in siteNames:
                if self.allSites[site].getValid() == 0:
                    continue
                
                localRank = phedexDset.getLocalRank(site)
                globalRank = globalRank + localRank
                nSites = nSites+1
                
            if nSites < 1:
                globalRank = 9999
            else:
                globalRank = globalRank/nSites

            phedexDset.setGlobalRank(globalRank)

    def makeDeletionLists(self):
       for site in sorted(self.allSites.keys()):
           if self.allSites[site].getStatus() == 0:
               continue
           self.sitePropers[site] = siteProperties.SiteProperties(site)

       phedexSets = self.phedexHandler.getPhedexDatasets()

       for datasetName in phedexSets.keys():
           onSites = phedexSets[datasetName].locatedOnSites()
           if len(onSites) < 1:
               continue
           rank =    phedexSets[datasetName].getGlobalRank()
           self.dataPropers[datasetName] = datasetProperties.DatasetProperties(datasetName)
           self.dataPropers[datasetName].append(onSites)
           for site in onSites:
               size = phedexSets[datasetName].size(site)
               part = phedexSets[datasetName].isPartial(site)
               cust = phedexSets[datasetName].isCustodial(site)
               vali = phedexSets[datasetName].isValid(site)
               self.sitePropers[site].addDataset(datasetName,rank,size,vali,part,cust)

       for site in sorted(self.allSites.keys()):
           if self.allSites[site].getStatus() == 0:
               continue
           size = self.allSites[site].getSize()
           sitePr = self.sitePropers[site]
           sitePr.setSiteSize(size)
           taken = sitePr.spaceTaken()

           size2del = -1
           if taken > size*self.DETOX_USAGE_MAX :
               size2del = sitePr.spaceTaken() - size*self.DETOX_USAGE_MIN
           sitePr.setSpaceToFree(size2del)

       for i in range(1, 4):
           self.unifyDeletionLists(i)

       # now it all done, calculate for each site space taken by last copies
       for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
           sitePr = self.sitePropers[site]
           sitePr.lastCopySpace(self.dataPropers,self.DETOX_NCOPY_MIN)

       self.printResults()

    def unifyDeletionLists(self,iteration):
        for site in self.sitePropers.keys():
            self.sitePropers[site].makeWishList()

        for datasetName in self.dataPropers.keys():
            dataPr = self.dataPropers[datasetName]
            count_wishes = 0
            for site in self.sitePropers.keys():
                sitePr = self.sitePropers[site]
                if(sitePr.onWishList(datasetName)):
                    count_wishes = count_wishes + 1

            if dataPr.nSites()-dataPr.nBeDeleted() - count_wishes > (self.DETOX_NCOPY_MIN-1):
                # grant wishes to all sites
                for site in self.sitePropers.keys():
                    sitePr = self.sitePropers[site]
                    if sitePr.onWishList(datasetName):
                        sitePr.grantWish(datasetName)
                        dataPr.addDelTarget(site)
            else:
                # here all sites want to delete this set need to pick one to host this set
                # and grant wishes to others pick one with the least space of protected
                # datasets
                for iter in range(0,2):
                    nprotected = 0
                    for site in sorted(dataPr.mySites(), cmp=self.sortByProtected):              
                        sitePr = self.sitePropers[site]
                    
                        if datasetName == '/SingleElectron/Run2012C-EcalRecover_11Dec2012-v1/AOD' :
                            print site
                            
                        if sitePr.pinDataset(datasetName):
                            nprotected = nprotected + 1
                            if nprotected >= self.DETOX_NCOPY_MIN:
                                breakout = True
                                break

                    #here could not find last copy site
                    #need to remove this dataset from all sites
                    if nprotected < self.DETOX_NCOPY_MIN:
                        for site in dataPr.mySites():
                            sitePr = self.sitePropers[site]
                            sitePr.revokeWish(datasetName)
                            dataPr.removeDelTarget(site)
                    else:
                        break
                    
                for site in self.sitePropers.keys() :
                    sitePr = self.sitePropers[site]
                    if(sitePr.onWishList(datasetName)):
                        sitePr.grantWish(datasetName)
                        dataPr.addDelTarget(site)

    def printResults(self):
        resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']
        beDeleted = glob.glob(resultDirectory + "/*")
        for subd in beDeleted:
            shutil.rmtree(subd)
            
        today = str(datetime.date.today())
        ttime = time.strftime("%H:%M")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            sitedir = resultDirectory + "/" + site
            if not os.path.exists(sitedir):
                os.mkdir(sitedir)
                
            file_timest = sitedir + "/Summary.txt"
            file_remain = sitedir + "/RemainingDatasets.txt"
            file_delete = sitedir + "/DeleteDatasets.txt"
            file_protected = sitedir + "/ProtectedDatasets.txt"

            outputFile = open(file_timest,'w')
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- D E L E T I O N  P A R A M E T E R S ----\n\n")
            outputFile.write("Upper Threshold     : %8.2f\n"%(self.DETOX_USAGE_MAX))
            outputFile.write("Lower Threshold     : %8.2f\n"%(self.DETOX_USAGE_MIN))
            outputFile.write("\n")
            outputFile.write("#- S P A C E  S U M M A R Y ----------------\n\n")
            outputFile.write("Total Space     [TB]: %8.2f\n"%(sitePr.siteSizeGb()/1024))
            outputFile.write("Space Used      [TB]: %8.2f\n"%(sitePr.spaceTaken()/1024))
            outputFile.write("Space to delete [TB]: %8.2f\n"%(sitePr.spaceDeleted()/1024))
            outputFile.write("Space last CP   [TB]: %8.2f\n"%(sitePr.spaceLastCp()/1024))
            outputFile.close()
            
            outputFile = open(file_delete,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n#\n")
            outputFile.write("#   Rank      Size nsites nsites  DatasetName \n")
            outputFile.write("#[~days]      [GB] before after               \n")
            outputFile.write("#---------------------------------------------\n")
            for dset in sitePr.delTargets():
                dataPr = self.dataPropers[dset]
                rank =   sitePr.dsetRank(dset)
                size = sitePr.dsetSize(dset)
                nsites = dataPr.nSites()
                ndeletes = dataPr.nBeDeleted()
                outputFile.write("%8.1f %9.1f %6d %6d  %s\n"\
                                 %(rank,size,nsites,nsites-ndeletes,dset))
            outputFile.close()

            outputFile = open(file_remain,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n\n")
            outputFile.write("#   Rank      Size nsites nsites  DatasetName \n")
            outputFile.write("#[~days]      [GB] before after               \n")
            outputFile.write("#---------------------------------------------\n")
            delTargets = sitePr.delTargets()
            for dset in sitePr.allSets():
                if dset in delTargets: continue
                dataPr = self.dataPropers[dset]
                rank = sitePr.dsetRank(dset)
                size = sitePr.dsetSize(dset)
                nsites = dataPr.nSites()
                ndeletes = dataPr.nBeDeleted()
                outputFile.write("%8.1f %9.1f %6d %6d  %s\n"\
                                 %(rank,size,nsites,nsites-ndeletes,dset))
            outputFile.close()


    def requestDeletions(self):
        
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]

            datasets2del = sitePr.delTargets()
            if len(datasets2del) < 1:
                continue

            totalSize = 0
            for dataset in datasets2del:
                totalSize =  totalSize + self.dataPropers[dataset].mySize()
            print "Deletion request for site " + site
            print " -- Number of datassetes     = " + str(len(datasets2del))
            print "%s %0.2f %s" %(" -- Total size to be deleted =",totalSize/1024,"TB")
            
            phedex = phedexApi.phedexApi(logPath='./')
            # compose data for deletion request
            check,data = phedex.xmlData(datasets=datasets2del,instance='prod')
            if check: 
                print " ERROR - phedexApi.xmlData failed"
                sys.exit(1)
                
            # here the request is really sent
            message = 'IntelROCCS -- Automatic Cache Release Request (if not acted upon will repeat ' + \
                      'in about %s hours).'%(os.environ['DETOX_CYCLE_HOURS']) + \
                      ' Summary at: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'
            check,response = phedex.delete(node=site,data=data,comments=message,instance='prod')
            if check:
                print " ERROR - phedexApi.delete failed"
                print response
                sys.exit(1)
                
            respo = response.read()
            matchObj = re.search(r'"id":"(\d+)"',respo)
            id = int(matchObj.group(1))
            
            date = (re.search(r'"request_date":"(.*?)"',respo)).group(1)
            date = date[:-3]
            myCnf = os.environ['DETOX_MYSQL_CONFIG']

            for dataset in datasets2del:
                dataPr = self.dataPropers[dataset]
                rank =   sitePr.dsetRank(dataset)
                size =   dataPr.mySize()
                group = 'AnalysisOps'
        
                #db = MySQLdb.connect(read_default_file=myCnf,read_default_group="mysql")
                #cursor = db.cursor()
                connection = self.getDbConnection()
                cursor = connection.cursor()
                sql = "insert into Requests(RequestId,RequestType,SiteName,Dataset,Size,Rank,GroupName," + \
                      "TimeStamp) values ('%d', '%d', '%s', '%s', '%d', '%d', '%s', '%s' )" % \
                      (id, 1, site, dataset,size,rank,group,date)
                    
                # ! this could be done in one line but it is just nice to see what is deleted !
                try:
                    cursor.execute(sql)
                    connection.commit()
                except:
                    print " -- FAILED insert mysql info: %s"%(sql)
                    # CP -- rollback is not needed because if commit failed nothing has been written
                    ## connection.rollback()
                # close the connection to the database
                connection.close()

    def showCacheRequests(self):
        for site in sorted(self.allSites.keys()):
             if self.allSites[site].getStatus() != 0:
                 self.showCacheRequestsForSite(site)
        
    def showCacheRequestsForSite(self,site):
        siteRequests = []
        requestDetails = {}
        requestSizes = {}
        requestTime = {}
        dranks = {}
        
        myCnf = os.environ['DETOX_MYSQL_CONFIG']
        
        #db = MySQLdb.connect(read_default_file=myCnf,read_default_group="mysql")
        #cursor = db.cursor()
        connection = self.getDbConnection(os.environ.get('DETOX_HISTORY_DB'))
        cursor = connection.cursor()
        sql = "select  RequestId,SiteName,Dataset,Size,Rank,GroupName,TimeStamp from Requests " + \
              " where SiteName='" + site + "' order by RequestId DESC LIMIT 1000"

        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except MySQLdb.Error,e:
            print e[0],e[1]
            print " -- FAILED extract mysql info: %s"%(sql)
            connection.close()
            return
        # close connection to the database
        connection.close()
        
        for row in results:
            reqid   = row[0]
            site    = row[1]
            dataset = row[2]
            size    = row[3]
            rank    = row[4]
            tstamp  = row[6]

            dranks[dataset] = rank 
            
            if reqid not in siteRequests:
                siteRequests.append(reqid)

            if reqid in requestDetails.keys():
                if dataset not in requestDetails[reqid]:
                    (requestDetails[reqid]).append(dataset)
                    requestSizes[reqid] = requestSizes[reqid] + size
            else:
                requestDetails[reqid] = [dataset]
                requestSizes[reqid] = size
                requestTime[reqid] = tstamp

        resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']
        outputFile = open(resultDirectory + '/' + site + '/DeletionHistory.txt','w')
        
        outputFile.write("# ======================================================================\n")
        outputFile.write("# This is the list of the 20 last phedex deletion requests at this site.\n")
        outputFile.write("#  ---- note: presently those requests are not necessarily approved.    \n")
        outputFile.write("# ======================================================================\n")

        
        counter = 0
        prevReqSize = 0
        for reqid in sorted(siteRequests,reverse=True):
            if prevReqSize == requestSizes[reqid]:
                continue
            prevReqSize = requestSizes[reqid]
            outputFile.write("#\n# PhEDEx Request: %s (%10s, %.1f GB)\n"%\
                             (reqid,requestTime[reqid],requestSizes[reqid]))
            
            outputFile.write("#\n# Rank   DatasetName\n")
            outputFile.write("# ---------------------------------------\n")

            for dataset in sorted(dranks, key=dranks.get, reverse=True):
                if dataset in requestDetails[reqid]:
                    outputFile.write("  %-6d %s\n" %(dranks[dataset],dataset))
            
#            for dataset in requestDetails[reqid]:
#                outputFile.write("  %-6d %s\n" %(dranks[dataset],dataset))
            outputFile.write("\n")
            counter = counter + 1
            if counter > 20:
                break
            
        if len(siteRequests) < 1:
            outputFile.write("# no requests found")

        outputFile.close()

    def showRunawayDatasets(self):
         for site in sorted(self.allSites.keys()):
             if self.allSites[site].getStatus() != 0:
                 self.showRunawaysForSite(site)
                 
    def showRunawaysForSite(self,site):
        print "\n" + site
        phedexSets = self.phedexHandler.getPhedexDatasetsAtSite(site)
        groups = {}
        for dataset in phedexSets:
            groupName = dataset.group(site)
            if groupName=='AnalysisOps':
                continue
            if groupName=='DataOps':
                continue
            if groupName=='FacOps':
                continue
            if groupName=='local':
                continue
            if groupName=='heavy-ions':
                continue
            
            if groupName not in groups.keys():
                groups[groupName] = dataset.size(site)
            else:
                groups[groupName] = groups[groupName] + dataset.size(site)

        for groupName in sorted(groups.keys(), key=groups.get, reverse=True):
            print "%-14s %0.1f " %(groupName,groups[groupName])

    def sendEmail(self):
        emails = os.environ['DETOX_EMAIL_LIST']
        To = emails.split(",")
        From = "maxi@t3btch039.mit.edu"
        Subj = "Problems detected while running Cache Release"
        Text = """Execution was terminated, check log to correct problems."""
        
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
        
    def sortByProtected(self,item1,item2):
        r1 = self.sitePropers[item1].spaceFree()
        r2 = self.sitePropers[item2].spaceFree()
        if r1 < r2:
            return 1
        elif r1 > r2:
            return -1
        else:
            return 0
