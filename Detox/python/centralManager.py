#===================================================================================================
#  C L A S S
#===================================================================================================
import sys, os, subprocess, re, time, datetime, smtplib, MySQLdb, shutil, string, glob 
import math,statistics
import phedexDataHandler, popularityDataHandler, phedexApi, deprecateDataHandler
import siteProperties, datasetProperties
import siteStatus, deletionRequest
import dbInfoHandler
import spreadLowRankSets 

class CentralManager:
    def __init__(self):

        if not os.environ.get('DETOX_DB'):
            raise Exception(' FATAL -- DETOX environment not defined: source setup.sh\n')

        self.DETOX_NCOPY_MIN = int(os.environ['DETOX_NCOPY_MIN'])
        self.DETOX_USAGE_MIN = float(os.environ['DETOX_USAGE_MIN'])
        self.DETOX_USAGE_MAX = float(os.environ['DETOX_USAGE_MAX'])

        self.phedexGroups = (os.environ['DETOX_GROUP']).split(',')

        self.allSites = {}
        self.dbInfoHandler = dbInfoHandler.DbInfoHandler()
        self.getAllSites()

        self.sitePropers = {}
        self.dataPropers = {}
        self.dataAccCorr = {}

        self.delRequests = {}
        self.siteRequests = {}
        self.epochTime = int(time.time())

        self.phedexHandler = phedexDataHandler.PhedexDataHandler(self.allSites)
        self.popularityHandler = popularityDataHandler.PopularityDataHandler(self.allSites)
        self.deprecatedHandler = deprecateDataHandler.DeprecateDataHandler()

        self.lowRankSpreader = spreadLowRankSets.SpreadLowRankSets(self.dbInfoHandler)


    def getAllSites(self):
        self.allSites = self.dbInfoHandler.allSites

        for site in sorted(self.allSites):
            if self.allSites[site].getStatus() == 0:
                print ' Site not active, status=%d  - %s'%(self.allSites[site].getStatus(),site)
            else:
                print ' Site --- active, status=%d  - %s'%(self.allSites[site].getStatus(),site)

    def extractPhedexData(self,federation):
        try:
            self.phedexHandler.extractPhedexData(federation)
        except:
            self.sendEmail("Problems detected while running Cache Release",\
                               "Execution was terminated, check log to correct problems.")
            raise

    def extractDeprecatedData(self):
        self.deprecatedHandler.extractDeprecatedData()

    def extractPopularityData(self):
        try:
            self.popularityHandler.extractPopularityData()
        except :
            self.sendEmail("Problems detected while running Cache Release",\
                               "Execution was terminated, check log to correct problems.")
            raise


    def checkProxyValid(self):
        process = subprocess.Popen(["/usr/bin/voms-proxy-info","-file",os.environ['DETOX_X509UP']],
                                   shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output, err = process.communicate()
        p_status = process.wait()
        if p_status != 0:
            self.sendEmail("Problems detected while running Cache Release",\
                               "Execution was terminated, check log to correct problems.")
            raise Exception(" FATAL -- Bad proxy file " + os.environ['DETOX_X509UP'])

        m = (re.findall(r"timeleft\s+:\s+(\d+):(\d+):(\d+)",output))[0]
        hours = int(m[0])
        mins = int(m[1])
        if hours > 0:
            pass
        elif mins < 10:
            self.sendEmail("Problems detected while running Cache Release",\
                               "Execution was terminated, check log to correct problems.")
            raise Exception(" FATAL -- Bad proxy file " + os.environ['DETOX_X509UP'])

    def rankDatasetsLocally(self):
        for site in sorted(self.allSites):
            self.rankLocallyAtSite(site)

    def rankLocallyAtSite(self,site):
        secsPerDay = 60*60*24
        now = float(time.time())
        phedexSets = self.phedexHandler.getPhedexDatasets()
        lockedSets = self.phedexHandler.getLockedDatasets() 

        for dset in phedexSets:
            if dset in lockedSets:
                lsites = lockedSets[dset].getLockedSites()
                for lsite in lsites:
                    if phedexSets[dset].group(lsite) == 'DataOps':
                        phedexSets[dset].setCustodial(lsite,1)
            for lsite in sorted(self.allSites):
                if self.phedexHandler.isLocalyLocked(lsite,dset):
                    phedexSets[dset].setCustodial(lsite,1)

        usedSets = self.popularityHandler.getUsedDatasets()
        phedexDsetNames = self.phedexHandler.getDatasetsAtSite(site)
        for datasetName in sorted(phedexDsetNames):
            phedexDset = phedexSets[datasetName]
            creationDate = phedexDset.creationTime(site)
            size = phedexDset.size(site)
            used = 0
            nAccessed = 0
            lastAccessed = now

            if datasetName in usedSets:
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
                          used*( (now-lastAccessed)/(60*60*24)-nAccessed) - size/1000
            phedexSets[datasetName].setLocalRank(site,datasetRank)
            phedexSets[datasetName].setIfUsed(site,used)

        statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
        today = str(datetime.date.today())
        outputFile = open(statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE'],'w')
        outputFile.write("# -- " + today + "\n\n")
        outputFile.write("#   Rank      Size  DatasetName \n")
        outputFile.write("#[~days]      [GB] \n")
        outputFile.write("#-------------------------------\n")
        dsets = self.phedexHandler.getDatasetsByRank(site)
        for datasetName in dsets:
            phedexDset = phedexSets[datasetName]
            rank = int(phedexDset.getLocalRank(site))
            size = float(phedexDset.size(site))
            outputFile.write("%8.1f %9.1f %s\n"%(rank,size,datasetName))
        outputFile.close()

        origFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']
        copyFile = statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE']+'-local'
        shutil.copy2(origFile,copyFile)

    def findExtraUsage(self):
        usedSets = self.popularityHandler.getUsedDatasets()
        phedexDsets = self.phedexHandler.getPhedexDatasets()

        for dataset in usedSets.keys():
            phedexSites = []
            if dataset in phedexDsets:
                phedexSites = phedexDsets[dataset].locatedOnSites(self.phedexGroups)
            popSites = usedSets[dataset].locatedOnSites()

            if len(phedexSites) < 1 :
                continue
            remoteSites = list(set(popSites) - set(phedexSites))
            if len(remoteSites) < 1 :
                continue

            missedNacc = {}
            for reqid in self.delRequests:
                delReq = self.delRequests[reqid]
                if delReq.hasDataset(dataset):
                    tstamp = delReq.getTimeStamp()
                    siteName = delReq.siteName()
                    if siteName not in remoteSites:
                        continue
                    missedNacc[siteName] = usedSets[dataset].timesUsedSince(tstamp,siteName)

            for siteName in remoteSites:
                if siteName in missedNacc:
                    continue
                missedNacc[siteName] = usedSets[dataset].timesUsed(siteName)

            nAccessed = 0
            for siteName in missedNacc:
                nAccessed = nAccessed + missedNacc[siteName]

            if nAccessed > 0:
                self.dataAccCorr[dataset] = nAccessed

    def rankDatasetsGlobally(self, phedexGroup):
        secsPerDay = 60*60*24
        now = float(time.time())
        phedexSets = self.phedexHandler.getPhedexDatasets()
        usedSets = self.popularityHandler.getUsedDatasets()
        phedGroups = phedexGroup.split('+')
            
        for datasetName in sorted(phedexSets.keys()):
            phedexDset = phedexSets[datasetName]
            siteNames = phedexDset.locatedOnSites(phedGroups)
            globalRank = 0
            nSites = 0
            for site in siteNames:
                if site not in self.allSites:
                    continue

                if self.allSites[site].getValid(phedGroups) == 0:
                    continue

                localRank = phedexDset.getLocalRank(site)
                globalRank = globalRank + localRank
                nSites = nSites+1

            if nSites < 1:
                globalRank = 9999
            else:
                globalRank = globalRank/nSites

            if datasetName in self.dataAccCorr:
                maxSize = 0.1
                for site in siteNames:
                    size = phedexDset.size(site)
                    if size > maxSize:
                        maxSize = size
                if maxSize > 0:
                    nAccessed = self.dataAccCorr[datasetName]
                    globalRank = globalRank - nAccessed/maxSize

            phedexDset.setGlobalRank(globalRank)

    def makeDeletionLists(self,phedexGroup):
       self.sitePropers.clear()
       self.dataPropers.clear()
       phedGroups = phedexGroup.split('+')
            
       for site in sorted(self.allSites.keys()):
           if self.allSites[site].getStatus() == 0:
               continue
           self.sitePropers[site] = siteProperties.SiteProperties(site)

       phedexSets = self.phedexHandler.getPhedexDatasets()
       for datasetName in phedexSets:
           phedexSets[datasetName].findIncomplete()

       missing = 0
       for datasetName in phedexSets:
           onSites = phedexSets[datasetName].locatedOnSites(phedGroups)
           if len(onSites) < 1:
               continue

           rank =       phedexSets[datasetName].getGlobalRank()
           trueSize =   phedexSets[datasetName].getTrueSize()
           trueNfiles = phedexSets[datasetName].getTrueNfiles()
           self.dataPropers[datasetName] = datasetProperties.DatasetProperties(datasetName)
           self.dataPropers[datasetName].append(onSites)
           self.dataPropers[datasetName].setId(self.dbInfoHandler.getDatasetId(datasetName))
           self.dataPropers[datasetName].setTrueSize(trueSize)
           self.dataPropers[datasetName].setTrueNfiles(trueNfiles)
           for site in onSites:
               isDeprecated = self.deprecatedHandler.isDeprecated(datasetName,site)
               size = phedexSets[datasetName].size(site)
               part = phedexSets[datasetName].isPartial(site)
               cust = phedexSets[datasetName].isCustodial(site)
               vali = phedexSets[datasetName].isValid(site)
               reqtime = phedexSets[datasetName].reqTime(site)
               updtime = phedexSets[datasetName].updTime(site)
               isdone = phedexSets[datasetName].isDone(site)
               wasUsed = phedexSets[datasetName].getIfUsed(site)
               self.sitePropers[site].addDataset(datasetName,rank,size,vali,part,
                                                 cust,isDeprecated,reqtime,updtime,wasUsed,isdone)

       for site in sorted(self.allSites.keys()):
           if self.allSites[site].getStatus() == 0:
               continue
           if not self.allSites[site].getValid([phedexGroup]):
               continue
               
           size = self.allSites[site].getSize(phedexGroup)

           sitePr = self.sitePropers[site]
           sitePr.setSiteSize(size)
           taken = sitePr.spaceTaken()

           size2del = -1
           if taken > size*self.DETOX_USAGE_MAX :
               size2del = sitePr.spaceTaken() - size*self.DETOX_USAGE_MIN
           size2del = min(100*1000,size2del)
           sitePr.setSpaceToFree(size2del)

       #determine if we need to call it again
       #call it if there are sites that should delete more 
       #and have datasets to add to wish list
       if phedexGroup == 'DataOps':
           ncopyMin = 0
           banInvalid = False
       else:
           ncopyMin = self.DETOX_NCOPY_MIN
           banInvalid = True
       oneMoreIteration = True
       totalIters = 0
       while oneMoreIteration:
           oneMoreIteration = False
           for site in sorted(self.allSites.keys()):
               if self.allSites[site].getStatus() == 0:
                   continue
               sitePr = self.sitePropers[site]
               if sitePr.space2free > sitePr.deleted:
                   if sitePr.hasMoreToDelete(self.dataPropers,ncopyMin,banInvalid):
                       print " --- Site "+site+" has more to delete"
                       oneMoreIteration = True
                       break
           if oneMoreIteration:
               if totalIters > 10 :
                   oneMoreIteration = False
                   break
               print " Iterating unifying deletion lists"
               self.unifyDeletionLists(phedexGroup)
               totalIters = totalIters + 1

       # now it all done, calculate for each site space taken by last copies
       statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
       today = str(datetime.date.today())
       for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
           sitePr = self.sitePropers[site]
           if phedexGroup == 'DataOps':
               sitePr.lastCopySpace(self.dataPropers,0)
           else:
               sitePr.lastCopySpace(self.dataPropers,self.DETOX_NCOPY_MIN)

           outputFile = open(statusDirectory+'/'+site+'/'+os.environ['DETOX_DATASETS_TO_DELETE'],'w')
           outputFile.write("# -- " + today + "\n\n")
           outputFile.write("#   Rank      Size  DatasetName \n")
           outputFile.write("#[~days]      [GB] \n")
           outputFile.write("#-------------------------------\n")
           dsets = self.phedexHandler.getDatasetsByRank(site)
           for dset in sitePr.allSets():
               dataPr = self.dataPropers[dset]
               rank = sitePr.dsetRank(dset)
               size = sitePr.dsetSize(dset)
               outputFile.write("%8.1f %9.1f %s\n"%(rank,size,dset))
           outputFile.close()

       resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']
       for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
           sitePr = self.sitePropers[site]
           sitedir = resultDirectory + "/" + site
           if not os.path.exists(sitedir):
               os.mkdir(sitedir)

    def unifyDeletionLists(self,phedexGroup):
        if phedexGroup == 'DataOps':
            ncopyMin = 0
            banInvalid = False
        else:
            ncopyMin = self.DETOX_NCOPY_MIN
            banInvalid = True

        for site in self.sitePropers:
            self.sitePropers[site].makeWishList(self.dataPropers,ncopyMin,banInvalid)

        for datasetName in self.dataPropers:
            dataPr = self.dataPropers[datasetName]
            countWishes = 0
            for site in self.sitePropers:
                sitePr = self.sitePropers[site]
                if sitePr.onWishList(datasetName):
                    countWishes = countWishes + 1

            if dataPr.nSites()-dataPr.nBeDeleted() - countWishes > (ncopyMin-1):
                # grant wishes to all sites
                for site in self.sitePropers.keys():
                    sitePr = self.sitePropers[site]
                    if sitePr.onWishList(datasetName):
                        added = sitePr.grantWish(datasetName)
                        if added:
                            dataPr.addDelTarget(site)
            else:
                # here all sites want to delete this set need to pick one to host this set
                # and grant wishes to others pick one with the least space of protected
                # datasets
                for iter in range(0,2):
                    nprotected = 0
                    for site in sorted(dataPr.mySites(), cmp=self.sortByProtected):
                        sitePr = self.sitePropers[site]

                        if sitePr.pinDataset(datasetName):
                            nprotected = nprotected + 1
                            if nprotected >= ncopyMin :
                                break

                    #here could not find last copy site
                    #need to remove this dataset from all sites
                    if nprotected < ncopyMin:
                        for site in dataPr.mySites():
                            sitePr = self.sitePropers[site]
                            sitePr.revokeWish(datasetName)
                            dataPr.removeDelTarget(site)
                    else:
                        break

                for site in self.sitePropers.keys() :
                    sitePr = self.sitePropers[site]
                    if(sitePr.onWishList(datasetName)):
                        added = sitePr.grantWish(datasetName)
                        if added:
                            dataPr.addDelTarget(site)

    def assignToT1s(self):
        self.lowRankSpreader.assignSitePropers(self.sitePropers)
        self.lowRankSpreader.assignPhedexSets(self.phedexHandler.getPhedexDatasets())

        self.lowRankSpreader.assignDatasets()

    def getRequestStats(self,pastWeeks):
        reqTimes = {}
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            if site not in self.siteRequests:
                continue
            for reqid in self.siteRequests[site].getReqIds():
                theRequest = self.delRequests[reqid]
                timeStamp = theRequest.getTimeStamp()
                reqEpoch =  int(time.mktime(timeStamp.timetuple()))
                if (self.epochTime -reqEpoch) > pastWeeks * (60*60*24*7):
                    continue
                reqTimes[reqEpoch] = reqid
        totSets  = 0
        totSpace = 0
        totSites = 0
        for item in sorted(reqTimes):
            reqid = reqTimes[item]
            theRequest = self.delRequests[reqid]
            timeStamp = theRequest.getTimeStamp()
            site = theRequest.siteName()
            nsets = theRequest.getNdsets()
            size = theRequest.getSize()/1000
            totSets += nsets
            totSpace += size
            totSites += 1

        return (totSets,totSpace,totSites)

    def printResults(self, phedexGroup, mode):
        today = str(datetime.date.today())
        ttime = time.strftime("%H:%M")
        
        usedSets = self.popularityHandler.getUsedDatasets()
        totalSpaceTaken = 0
        totalSpaceLcopy = 0
        totalDisk = 0
        totalNotUsed = 0
        totalSpaceTakenT2 = 0
        totalSpaceLcopyT2 = 0
        totalNotUsedT2 = 0
        totalDiskT2 = 0
        t2Sites = 0
        # file with more infortmation on all sites
        outputFile = open(os.environ['DETOX_DB'] + "/SitesInfo.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n#\n")
            outputFile.write("#- S I T E S  I N F O R M A T I O N ----\n#\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("#  Active Quota[TB] Taken[TB] LastCopy[TB] SiteName \n")
        outputFile.write("#------------------------------------------------------\n")
        for site in sorted(self.allSites):
            if 'DataOps' in phedexGroup or 'AnalysisOps' in phedexGroup:
                pass
            else:
                if self.allSites[site].getSize(phedexGroup) == 0:
                    continue
            theSite = self.allSites[site]
            taken = 0
            lcopy = 0
            active = theSite.getStatus()

            if active != 0:
                sitePr = self.sitePropers[site]
                taken = sitePr.spaceTaken()/1000
                lcopy = sitePr.spaceLastCp()/1000
                if 'DataOps' in phedexGroup:
                    lcopy = sitePr.spaceCustodial()/1000
                totalDisk = totalDisk + theSite.getSize(phedexGroup)/1000
                totalSpaceLcopy = totalSpaceLcopy + lcopy
                totalSpaceTaken = totalSpaceTaken + taken
                if site.startswith("T2_"):
                    totalDiskT2 += theSite.getSize(phedexGroup)/1000
                    totalSpaceLcopyT2 += lcopy
                    totalSpaceTakenT2 += taken
                    t2Sites += 1
            
            # summary of all sites
            outputFile.write("   %-6d %-9d %-9d %-12d %-20s \n"\
                                 %(active,theSite.getSize(phedexGroup)/1000,taken,lcopy,site))
        outputFile.write("#------------------------------------------------------\n")
        outputFile.write("#  %-6d %-9d %-9d %-12d %-20s \n"\
                             %(len(self.allSites.keys()),totalDisk,
                               totalSpaceTaken,totalSpaceLcopy,'Total T2s+T1s'))
        percTst = 100; percTslc = 100; percUnused = 100
        if totalDiskT2 > 0:
            percTst = totalSpaceTaken/totalDisk*100
            percTslc = totalSpaceLcopy/totalDisk*100
        outputFile.write("#  %-6s %-9s %-4.1f%%     %-4.1f%%        %-20s \n"\
                             %(' ',' ',percTst,percTslc,' '))
        outputFile.write("# Total Active Quota  = %-9d \n"%(totalDisk))
        
        outputFile.write("#------------------------------------------------------\n")
        outputFile.write("#  %-6d %-9d %-9d %-12d %-20s \n"\
                             %(t2Sites,totalDiskT2,totalSpaceTakenT2,totalSpaceLcopyT2,'Total T2s'))
        percTst = 100; percTslc = 100
        if totalDiskT2 > 0:
            percTst = totalSpaceTakenT2/totalDiskT2*100
            percTslc = totalSpaceLcopyT2/totalDiskT2*100
        outputFile.write("#  %-6s %-9s %-4.1f%%     %-4.1f%%        %-20s \n"\
                             %(' ',' ',percTst,percTslc,' '))
        outputFile.write("# Total Active Quota  = %-9d \n"%(totalDiskT2))
        outputFile.write("#------------------------------------------------------\n")
        outputFile.close()

        siteRankAve = []
        siteRankMdn = []
	outputFile = open(os.environ['DETOX_DB'] + "/SiteRanks.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n#\n")
            outputFile.write("#- S I T E   R A N K S ----\n#\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("#  <Rank>    Mdn(Rank)  SiteName \n")
        outputFile.write("#\n#------------------------------------\n")
        for site in sorted(self.allSites):
            theSite = self.allSites[site]
            active = theSite.getStatus()
            if 'DataOps' in phedexGroup or 'AnalysisOps' in phedexGroup:
                pass
            else:
                if self.allSites[site].getSize(phedexGroup) == 0:
                    continue

            if active != 0:
                sitePr = self.sitePropers[site]
		median = sitePr.medianRank()
                rank =  sitePr.siteRank()
                siteRankAve.append(rank)
                siteRankMdn.append(median)
		outputFile.write("   %-9.1f %-10.1f %-20s \n"%(rank,median,site))
        sm1 = sm2 = rms1 = rms2 = 0
        if len(siteRankAve) > 0:
            sm1 = statistics.mean(siteRankAve)
            sm2 = statistics.mean(siteRankMdn)
        if len(siteRankAve) > 1:   
            rms1 = statistics.stdev(siteRankAve)
            rms2 = statistics.stdev(siteRankMdn)
        outputFile.write("#\n#------------------------------------\n")
        outputFile.write("#  %-9.1f %-10.1f %-20s \n"%(sm1,sm2,"Mean Value"))
        outputFile.write("#  %-9.1f %-10.1f %-20s \n"%(rms1,rms2,"RMS"))
        
        outputFile.close()

        outputFile = open(os.environ['DETOX_DB'] + "/DeletionSummary.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- D E L E T I O N  R E Q U E S T S ----\n\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("# Date   ReqId  NSets Size[TB] SiteName \n")
        outputFile.write("#--------------------------------------------------\n")
        #find all requests for site for the last week
        reqTimes = {}
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            if site not in self.siteRequests:
                continue
            for reqid in self.siteRequests[site].getReqIds():
                theRequest = self.delRequests[reqid]
                timeStamp = theRequest.getTimeStamp()
                reqEpoch =  int(time.mktime(timeStamp.timetuple()))
                if (self.epochTime -reqEpoch) > 2 * (60*60*24*7):
                    continue
                reqTimes[reqEpoch] = reqid
        for item in sorted(reqTimes):
            reqid = reqTimes[item]
            theRequest = self.delRequests[reqid]
            timeStamp = theRequest.getTimeStamp()
            site = theRequest.siteName()
            nsets = theRequest.getNdsets()
            size = theRequest.getSize()/1000
            outputFile.write("  %-5s  %-6d %-5d %-8d %-20s \n"\
                                 %(timeStamp.strftime("%m/%d"),reqid,nsets,size,site))
        outputFile.write("#--------------------------------------------------\n")
        (totSets,totSpace,totSites) =  self.getRequestStats(2)
        outputFile.write("#  %12s %-5d %-8d %-3d %-10s \n"%(' ',totSets,totSpace,totSites,'Total'))
        outputFile.write("#\n#---------------l a s t  m o n t h----------------\n#\n")
        (totSets,totSpace,totSites) =  self.getRequestStats(4)
        outputFile.write("#  %12s %-5d %-8d %-3d \n"%(' ',totSets,totSpace,totSites))
        outputFile.write("#\n#---------------l a s t  y e a r------------------\n#\n")
        (totSets,totSpace,totSites) =  self.getRequestStats(52)
        outputFile.write("#  %12s %-5d %-8d %-3d \n"%(' ',totSets,totSpace,totSites))
        outputFile.close()

        deprecatedSpace = 0
        totalSets = 0
        outputFile = open(os.environ['DETOX_DB'] + "/DeprecatedSummary.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- D E P R E C A T E D  D A T A S E T S ----\n\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("#  NDatasets Size[TB] SiteName \n")
        outputFile.write("#--------------------------------------------\n")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            nsets = sitePr.nsetsDeprecated()
            if nsets < 1:
                continue
            deprecatedSpace = deprecatedSpace + sitePr.spaceDeprecated()/1000
            totalSets = totalSets + nsets

            outputFile.write("   %-9d %-8.2f %-20s \n"\
                                 %(nsets,sitePr.spaceDeprecated()/1000,site))
        outputFile.write("#--------------------------------------------\n")
        outputFile.write("#  %-9d %-8.2f %-20s \n"%(totalSets,deprecatedSpace,'Total'))
        perc = 100
        if totalDisk > 0:
            perc = deprecatedSpace/totalDisk*100
        outputFile.write("#  %-9s %-3.1f%% %-20s \n"%('',perc,''))
        outputFile.write("# Total Active Quota = %-9d \n"%(totalDisk))
        outputFile.close()

        incompleteSpace = 0
        totalSets = 0
        totalTrueSize = 0
        totalDiskSize = 0
        outputFile = open(os.environ['DETOX_DB'] + "/IncompleteSummary.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- I N C O M P L E T E  D A T A S E T S ----\n\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("#  NDatasets TrueSize[TB] Size[TB] SiteName \n")
        outputFile.write("#--------------------------------------------\n")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            trueSize = 0
            diskSize = 0
            nsets = 0
            if 'DataOps' in phedexGroup or 'AnalysisOps' in phedexGroup:
                pass
            else:
                if self.allSites[site].getSize(phedexGroup) == 0:
                    continue
            for dset in sitePr.allSets():
                dataPr = self.dataPropers[dset]
                if sitePr.isPartial(dset):
                    delta = dataPr.getTrueSize() - sitePr.dsetSize(dset)
                    incompleteSpace = incompleteSpace + delta
                    trueSize = trueSize + dataPr.getTrueSize()/1000
                    totalTrueSize = totalTrueSize + dataPr.getTrueSize()/1000
                    diskSize = diskSize + sitePr.dsetSize(dset)/1000
                    totalDiskSize = totalDiskSize + sitePr.dsetSize(dset)/1000
                    nsets = nsets + 1
                    totalSets = totalSets + 1
            if nsets < 1:
                continue
            outputFile.write("   %-9d %-12.2f %-8.2f %-20s \n"%(nsets,trueSize,diskSize,site))
        outputFile.write("#--------------------------------------------\n")
        outputFile.write("#  %-9d %-12.2f %-8.2f %-20s \n"\
                                 %(totalSets,totalTrueSize,totalDiskSize,'Total'))
        delta = totalTrueSize-totalDiskSize
        percTs = 100; percTd = 100; perc = 100
        if totalDisk > 0:
            percTs = totalTrueSize/totalDisk*100
            percTd = totalDiskSize/totalDisk*100
            perc = delta/totalDisk*100
        outputFile.write("#  %-9s %-3.1f%%         %-3.1f%% %-20s \n"%('',percTs,percTd,''))
        outputFile.write("# Missing Space      = %-4d (%-3.1f%%)\n"%(delta,perc))
        outputFile.write("# Total Active Quota = %-9d \n"%(totalDisk))
        outputFile.close()

        outputFile = open(os.environ['DETOX_DB'] + "/LargeDatasets.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- L A R G E  D A T A S E T S ----\n\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        for dset in sorted(self.dataPropers):
            dataPr = self.dataPropers[dset]
            trueSize = dataPr.getTrueSize()
            if trueSize < 20000 :
                continue
            outputFile.write(dset+'\n')
            sites = dataPr.mySites()
            for site in sites:
                sitePr = self.sitePropers[site]
                diskSize = sitePr.dsetSize(dset)
                outputFile.write("   %-12.1f %-8.1f %-20s \n"%(trueSize,diskSize,site))
        outputFile.close()

        outputFile = open(os.environ['DETOX_DB'] + "/TransferStats.txt",mode)
        if mode == 'w':
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- D A T A S E T  D O W N L O A D  S P E E D----\n\n")
        outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
        outputFile.write("#  NStuck LoadSpeed  Volume  SiteName \n")
        outputFile.write("#         [GB/Day]   [TB]    \n")
        outputFile.write("#--------------------------------------------\n")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            (speed,volume,stuck) = sitePr.getDownloadStats()
            outputFile.write("   %-6d %-10.1f %-7.1f %-20s \n"\
                                 %(stuck,speed,volume/1000,site))
        outputFile.write("#--------------------------------------------\n")
        outputFile.close()

        resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            sitedir = resultDirectory + "/" + site

            fileTimest = sitedir + "/Summary.txt"
            fileRemain = sitedir + "/RemainingDatasets.txt"
            fileDelete = sitedir + "/DeleteDatasets.txt"
            fileDeprec = sitedir + "/DeprecatedSets.txt"
            fileIncomp = sitedir + "/IncompleteSets.txt"
            fileWrGroup= sitedir + "/RunAwayGroupSets.txt"
            fileStuck  = sitedir + "/StuckDatasets.txt" 

            outputFile = open(fileTimest,mode)
            if mode == 'w':
                outputFile.write('#- ' + today + " " + ttime + "\n\n")
                outputFile.write("#- D E L E T I O N  P A R A M E T E R S ----\n\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("Upper Threshold     : %8.2f  ->  %4d [TB]\n" \
                                 %(self.DETOX_USAGE_MAX,
                                   self.DETOX_USAGE_MAX*sitePr.siteSizeGb()/1000))
            outputFile.write("Lower Threshold     : %8.2f  ->  %4d [TB]\n" \
                                 %(self.DETOX_USAGE_MIN,
                                   self.DETOX_USAGE_MIN*sitePr.siteSizeGb()/1000))
            outputFile.write("\n")
            outputFile.write("#- S P A C E  S U M M A R Y ----------------\n\n")
            outputFile.write("Total Space     [TB]: %8.2f\n"%(sitePr.siteSizeGb()/1000))
            outputFile.write("Space Used      [TB]: %8.2f\n"%(sitePr.spaceTaken()/1000))
            outputFile.write("Space to delete [TB]: %8.2f\n"%(sitePr.spaceDeleted()/1000))
            outputFile.write("Space last CP   [TB]: %8.2f\n"%(sitePr.spaceLastCp()/1000))
            outputFile.write("Space deprected [TB]: %8.2f\n"%(sitePr.spaceDeprecated()/1000))
            outputFile.write("Incomplete data [TB]: %8.2f\n"%(sitePr.spaceIncomplete()/1000))
            outputFile.close()

            if len(sitePr.delTargets()) > 0:
                print " File: " + fileDelete
            outputFile = open(fileDelete,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n#\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
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

            outputFile = open(fileRemain,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("#   Rank      Size nsites nsites  DatasetName \n")
            outputFile.write("#[~days]      [GB] before after               \n")
            outputFile.write("#---------------------------------------------\n")
            delTargets = sitePr.delTargets()
            for dset in sitePr.allSets():
                if dset in delTargets: 
                    continue
                dataPr = self.dataPropers[dset]
                rank = sitePr.dsetRank(dset)
                size = sitePr.dsetSize(dset)
                nsites = dataPr.nSites()
                ndeletes = dataPr.nBeDeleted()
                outputFile.write("%8.1f %9.1f %6d %6d  %s\n"\
                                 %(rank,size,nsites,nsites-ndeletes,dset))
            outputFile.close()

            outputFile = open(fileDeprec,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("#   Rank    Size nsites DatasetName\n")
            outputFile.write("#[~days]    [GB] \n")
            outputFile.write("#------------------------------------\n")
            for dset in sitePr.allSets():
                if sitePr.isDeprecated(dset):
                    dataPr = self.dataPropers[dset]
                    rank = sitePr.dsetRank(dset)
                    size = sitePr.dsetSize(dset)
                    nsites = dataPr.nSites()
                    ndeletes = dataPr.nBeDeleted()
                    outputFile.write("%6.1f %9.1f %6d  %s\n"\
                                         %(rank,size,nsites-ndeletes,dset))
            outputFile.close()
            
            outputFile = open(fileIncomp,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("# Rank     TrueSize  DiskSize  nsites  DatasetName\n")
            outputFile.write("# [days]   [GB]      [GB]                       \n")
            outputFile.write("#------------------------------------\n")
            for dset in sitePr.allSets():
                if not sitePr.isPartial(dset): 
                    continue
                dataPr = self.dataPropers[dset]
                rank = sitePr.dsetRank(dset)
                size = sitePr.dsetSize(dset)
                trueSize = dataPr.getTrueSize()
                nsites = dataPr.nSites()
                ndeletes = dataPr.nBeDeleted()
		outputFile.write("  %-8.1f %-9.1f %-9.1f %-7d %-s\n"\
                                     %(rank,trueSize,size,nsites-ndeletes,dset))
            outputFile.close()

            outputFile = open(fileWrGroup,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("#------------------------------------\n")
            runAwayGroups =  self.phedexHandler.getRunAwayGroups(site)
            runAwaySets =  self.phedexHandler.getRunAwaySets(site)
            for group in sorted(runAwayGroups):
                outputFile.write("\n"+group+":\n")
                for dset in sorted(runAwaySets):
                    if runAwaySets[dset] == group:
                        outputFile.write(dset+"\n")
            outputFile.close()

            outputFile = open(fileStuck,mode)
            if mode == 'w':
                outputFile.write("# -- " + today + " " + ttime + "\n")
            outputFile.write("#\n#- DDM Partition: " + phedexGroup +" -\n#\n")
            outputFile.write("#------------------------------------\n")
            outputFile.write("# Rank     TrueSize  DiskSize  nsites  DatasetName \n")
            outputFile.write("# [days]   [GB]      [GB]                           \n")
            for dset in sitePr.allSets():
                if not sitePr.dsetIsStuck(dset): 
                    continue
                dataPr = self.dataPropers[dset]
                rank = sitePr.dsetRank(dset)
                size = sitePr.dsetSize(dset)
                trueSize = dataPr.getTrueSize()
                nsites = dataPr.nSites()
                ndeletes = dataPr.nBeDeleted()
                outputFile.write("  %-8.1f %-9.1f %-9.1f %-7d %-s\n"\
                                     %(rank,trueSize,size,nsites-ndeletes,dset))
            outputFile.close()

    def updateSiteStatus(self):
        # find all sites with stuck datasets, calculate mean and rms
        nstuckAtSite = {}
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            theSite = self.allSites[site]
            active = theSite.getStatus()
            if active == 0: 
                continue

            sitePr = self.sitePropers[site]
            (speed,volume,stuck) = sitePr.getDownloadStats()
            nstuckAtSite[site] = int(stuck)
        stmean,strms = self.getMeanValue(nstuckAtSite,3,0.5)
        # set status=2 to all sites that are above 3xrms threshold
        changingStatus = {}
        for site in sorted(self.allSites):
            theSite = self.allSites[site]
            if site.startswith("T1_"):
                continue
            active = theSite.getStatus()
            if active == 0: 
                continue

            shouldBe = 1
            if site in nstuckAtSite and abs(stmean -  nstuckAtSite[site]) > 3*strms:
                if nstuckAtSite[site] > 4:
                    shouldBe = 2
            if shouldBe != active:
                 print (" -- %-16s status changing: %1d --> %1d"%(site,active,shouldBe))
                 changingStatus[site] = shouldBe
                 
        self.dbInfoHandler.updateSiteStatus(changingStatus)


    def requestDeletions(self):
        now_tstamp = datetime.datetime.now()
        numberRequests = 0
        thisRequest = None
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):

            if self.allSites[site].getId() <  1:
                continue
            
            sitePr = self.sitePropers[site]

            datasets2del = sitePr.delTargets()
            if len(datasets2del) < 1:
                continue
            datasets2del = datasets2del[0:500]
            if not site.startswith('T2_'):
                continue

            totalSize = 0
            thisRequest = deletionRequest.DeletionRequest(0,site,now_tstamp)
            for dataset in datasets2del:
                dsetId = self.dbInfoHandler.getDatasetId(dataset)
                totalSize =  totalSize + sitePr.dsetSize(dataset)
                thisRequest.update(dsetId,sitePr.dsetRank(dataset),sitePr.dsetSize(dataset))
            print "Deletion request for site " + site
            
            print " -- Number of datasets       = " + str(len(datasets2del))
            print "%s %0.2f %s" %(" -- Total size to be deleted =",totalSize/1000,"TB")

            proceed = True
            if site in self.siteRequests:
                lastReqIds = self.siteRequests[site].getLastReqId(2)
                #they can look identical
                #resubmit in case it was submitted too long ago
                for lreqid in lastReqIds:
                    lastRequest = self.delRequests[lreqid]
                    if thisRequest.looksIdentical(lastRequest):
                        if thisRequest.deltaTime(lastRequest)/(60*60) < 72 :
                            print " -- Skipping submition, looks like request " + str(lreqid)
                            proceed = False
                            break
            if not proceed:
                continue
            numberRequests = numberRequests + 1

            
            (reqid,rdate) = self.submitDeletionRequest(site,datasets2del)
            #if site.startswith('T2'):
            self.submitUpdateRequest(site,reqid)
            print " -- Request Id =  " + str(reqid)
            
            newdate = datetime.datetime.strptime(rdate[:19],"%Y-%m-%d %H:%M:%S")
            self.delRequests[reqid] = deletionRequest.DeletionRequest(reqid,site,newdate,thisRequest)
            if site not in self.siteRequests:
                self.siteRequests[site] = deletionRequest.SiteDeletions(site)
            self.siteRequests[site].update(reqid,newdate)

            connection = self.dbInfoHandler.getDbConnection()
            for dataset in datasets2del:
                dataPr = self.dataPropers[dataset]
                rank =   sitePr.dsetRank(dataset)
                size =   sitePr.dsetSize(dataset)
                siteId = self.allSites[site].getId()
                dsetId = self.dataPropers[dataset].getId()
                groupID = 1

                cursor = connection.cursor()
                sql = "insert into Requests(RequestId,RequestType,SiteId,DatasetId,Rank,GroupId,Date) " \
                    "values('%d','%d','%d','%d','%d','%d','%s')" % \
                    (reqid, 1, siteId, dsetId, rank, groupID, rdate)
                try:
                    cursor.execute(sql)
                    connection.commit()
                except:
                    print " -- FAILED insert mysql info: %s"%(sql)
                    connection.close()
                    sys.exit(1)
            connection.close()

        if(numberRequests > 0):
            self.sendEmail("report from CacheRelease",\
                               "Submitted deletion requests, check log for details.")

    def extractDataStats(self):
        phedexSets = self.phedexHandler.getPhedexDatasets()

        for dsetName in phedexSets:
            dsetId = self.dbInfoHandler.getDatasetId(dsetName)
            if  self.dbInfoHandler.datasetExists(dsetId):
                phedexSets[dsetName].setTrueSize(self.dbInfoHandler.getDatasetSize(dsetId))
                phedexSets[dsetName].setTrueNfiles(self.dbInfoHandler.getDatasetFiles(dsetId))

    def extractCacheRequests(self):
        self.delRequests.clear()
        self.siteRequests.clear()

        results = self.dbInfoHandler.extractCacheRequests()
        for row in results:
            reqid  = int(row[0])
            dsetId = int(row[2])
            if not self.dbInfoHandler.datasetExists(dsetId):
                continue

            siteId = int(row[3])
            groupId = int(row[4])
            rank = int(row[5])
            tstamp = row[6]

            if self.dbInfoHandler.siteExists(siteId):
                site = self.dbInfoHandler.getSiteName(siteId)
            else:
                continue

            size = self.dbInfoHandler.getDatasetSize(dsetId)
            if reqid not in self.delRequests:
                self.delRequests[reqid] = deletionRequest.DeletionRequest(reqid,site,tstamp)
            if site not in self.siteRequests:
                self.siteRequests[site] = deletionRequest.SiteDeletions(site)
            self.delRequests[reqid].update(dsetId,rank,size)
            self.siteRequests[site].update(reqid,tstamp)

    def showCacheRequests(self):
        for site in sorted(self.allSites.keys()):
             if self.allSites[site].getStatus() != 0:
                 self.showCacheRequestsForSite(site)

    def showCacheRequestsForSite(self,site):
        resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']
        outputFile = open(resultDirectory + '/' + site + '/DeletionHistory.txt','w')
        outputFile.write("# ================================================================\n")
        outputFile.write("#  List of at most 20 last phedex deletion requests at this site. \n")
        outputFile.write("#  ---- note: those requests are not necessarily approved.        \n")
        outputFile.write("# ================================================================\n")

        if site not in self.siteRequests:
            outputFile.write("# no requests found")
            outputFile.close()
            return

        counter = 0
        prevRequest = None
        for reqid in self.siteRequests[site].getReqIds():
            theRequest = self.delRequests[reqid]
            if theRequest.looksIdentical(prevRequest):
                continue
            allSets = theRequest.getDsets()

            prevRequest = theRequest
            outputFile.write("#\n# PhEDEx Request: %s (%10s, %.1f GB)\n"%\
                             (reqid,theRequest.getTimeStamp(),theRequest.getSize()))

            outputFile.write("#\n# Rank   DatasetName\n")
            outputFile.write("# ---------------------------------------\n")

            for dataset in allSets:
                outputFile.write("  %-6d %s\n" %(theRequest.getDsetRank(dataset),dataset))

            outputFile.write("\n")
            counter = counter + 1
            if counter > 20:
                break
    def submitDeletionRequest(self,site,datasets2del):
        if len(datasets2del) < 1:
            return
        phedex = phedexApi.phedexApi(logPath='./')
        # compose data for deletion request
        check,data = phedex.xmlData(datasets=datasets2del,instance='prod',level='block')
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
        del phedex
        return (reqid,rdate)

    def submitUpdateRequest(self,site,reqid):
        # here we brute force deletion to be approved
        phedex = phedexApi.phedexApi(logPath='./')
        check,response = phedex.updateRequest(decision='approve',request=reqid,
                                              node=site,instance='prod')
        if check:
            print " ERROR - phedexApi.updateRequest failed - reqid="+ str(reqid)
            print response
        del phedex

    def changeGroup(self,site,dataset,group):
        # here we brute force deletion to be approved
        phedex = phedexApi.phedexApi(logPath='./')
        check,response = phedex.changeGroup(site,dataset,group)
        if check:
            print " ERROR - phedexApi.updateRequest failed"
            print response
        del phedex

    def sendEmail(self,subject,body):
        emails = os.environ['DETOX_EMAIL_LIST']
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

    def sortByProtected(self,item1,item2):
        r1 = self.sitePropers[item1].spaceFree()
        r2 = self.sitePropers[item2].spaceFree()
        if r1 < r2:
            return 1
        elif r1 > r2:
            return -1
        else:
            return 0

    def getMeanValue(self,aa,rmsW,frac):
        meanPr = 999.0
        mean = 0.0
        rms = 999.9
        loops = 0
        while True:
            mean = 0.0
            meansq = 0.0
            items = 0.0
            for vkey in aa:
                value = aa[vkey]
                if abs(value - meanPr) > rmsW*rms:
                    continue
                mean = mean + value
                meansq = meansq + value*value
                items = items + 1
            mean = mean/items
            rms = math.sqrt(meansq/items - mean*mean)
            if abs(mean - meanPr) < frac*rms:
                break
            else:
                meanPr = mean
            loops = loops + 1
            if loops > 10:
                break
        return (mean, rms)
