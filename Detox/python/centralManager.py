#===================================================================================================
#  C L A S S
#===================================================================================================
import sys, os, subprocess, re, time, datetime, smtplib, MySQLdb, shutil, string, glob
import phedexDataHandler, popularityDataHandler, phedexApi, deprecateDataHandler
import siteProperties, datasetProperties
import siteStatus, deletionRequest

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

        self.delRequests = {}
        self.siteRequests = {}

        self.phedexHandler = phedexDataHandler.PhedexDataHandler(self.allSites)
        self.popularityHandler = popularityDataHandler.PopularityDataHandler(self.allSites)
        self.deprecatedHandler = deprecateDataHandler.DeprecateDataHandler()

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

    def getAllSites(self):
        db = os.environ.get('DETOX_SITESTORAGE_DB')

        print ' Access site storage database to find all sites.'
        connection = self.getDbConnection()
        cursor = connection.cursor()

        # AnalysisOps is GroupId=1
        sql = "select SiteName,SizeTb,Sites.Status,Sites.SiteId from Quotas,Sites "
        sql = sql + "where GroupId=1 and Quotas.SiteId=Sites.SiteId"
        
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print ' Error(%s) -- could not retrieve sites'%(sql)
            print sys.exc_info()
            connection.close()
            sys.exit(1)
        connection.close()

        for row in results:
            siteName = row[0]
            siteSizeGb = float(row[1])*1000
            willBeUsed = int(row[2])
            siteId = int(row[3])
            self.allSites[siteName] = siteStatus.SiteStatus(siteName)
            self.allSites[siteName].setStatus(willBeUsed)
            self.allSites[siteName].setSize(siteSizeGb)
            self.allSites[siteName].setId(siteId)

        for site in sorted(self.allSites):
            if self.allSites[site].getStatus() == 0:
                print ' Site not active, status=%d  - %s'%(self.allSites[site].getStatus(),site)
            else:
                print ' Site --- active, status=%d  - %s'%(self.allSites[site].getStatus(),site)

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
                if site not in self.allSites:
                    continue
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

       #here we go into datasets and find out dataset Ids
       # or assign if they do not exist
       dataSetIds = {}
       connection = self.getDbConnection()
       for datasetName in phedexSets.keys():
           cursor = connection.cursor()
           sql = "select Datasets.DatasetId,NFiles,Size from Datasets,DatasetProperties"
           sql = sql + " where DatasetName='"+datasetName+"'"
           sql = sql + " and Datasets.DatasetId=DatasetProperties.DatasetId"
           try:
               cursor.execute(sql)
               results = cursor.fetchall()
           except MySQLdb.Error,e:
               print e[0],e[1]
               print " -- FAILED extract mysql info: %s"%(sql)
               connection.close()
               sys.exit(1)
           
           dsetId = None
           for row in results:
               dsetId   = row[0]
               trueNfiles = row[1]
               trueSize = row[2]
           dataSetIds[datasetName] = dsetId
           phedexSets[datasetName].setTrueSize(trueSize)
           phedexSets[datasetName].setTrueNfiles(trueNfiles)
           phedexSets[datasetName].findIncomplete()
       connection.close()

       missing = 0
       for datasetName in phedexSets.keys():
           onSites = phedexSets[datasetName].locatedOnSites()
           if len(onSites) < 1:
               continue
           if dataSetIds[datasetName] is None:
               print " -- WARNING -- not in the database " + datasetName

           rank =       phedexSets[datasetName].getGlobalRank()
           trueSize =   phedexSets[datasetName].getTrueSize()
           trueNfiles = phedexSets[datasetName].getTrueNfiles()
           self.dataPropers[datasetName] = datasetProperties.DatasetProperties(datasetName)
           self.dataPropers[datasetName].append(onSites)
           self.dataPropers[datasetName].setId(dataSetIds[datasetName])
           #self.dataPropers[datasetName].setDeprecated(isDeprecated)
           self.dataPropers[datasetName].setTrueSize(trueSize)
           self.dataPropers[datasetName].setTrueNfiles(trueNfiles)
           for site in onSites:
               isDeprecated = self.deprecatedHandler.isDeprecated(datasetName,site)
               size = phedexSets[datasetName].size(site)
               part = phedexSets[datasetName].isPartial(site)
               cust = phedexSets[datasetName].isCustodial(site)
               vali = phedexSets[datasetName].isValid(site)
               #since I cant delete dataset that is not in the datase 
               #I will set it as invalid 
               if dataSetIds[datasetName] is None:
                   vali = False
               self.sitePropers[site].addDataset(datasetName,rank,size,vali,part,
                                                 cust,isDeprecated)

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
           if size2del > 100*1000:
               size2del = 100*1000
           sitePr.setSpaceToFree(size2del)

       #determine if we need to call it again
       #call it if there are sites that should delete more 
       #and have datasets to add to wish list
       oneMoreIteration = True
       totalIters = 0
       while oneMoreIteration:
           oneMoreIteration = False
           for site in sorted(self.allSites.keys()):
               if self.allSites[site].getStatus() == 0:
                   continue
               sitePr = self.sitePropers[site]
               if sitePr.space2free > sitePr.deleted:
                   if sitePr.hasMoreToDelete():
                       print " --- Site "+site+" has more to delete"
                       oneMoreIteration = True
                       break
           if oneMoreIteration:
               if totalIters > 20 :
                   oneMoreIteration = False
                   break
               print " Iterating unifying deletion lists"
               self.unifyDeletionLists()
               totalIters = totalIters + 1

       # now it all done, calculate for each site space taken by last copies
       statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
       today = str(datetime.date.today())
       for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
           sitePr = self.sitePropers[site]
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

       self.printResults()

    def unifyDeletionLists(self):
        for site in self.sitePropers:
            self.sitePropers[site].makeWishList()

        for datasetName in self.dataPropers:
            dataPr = self.dataPropers[datasetName]
            countWishes = 0
            for site in self.sitePropers:
                sitePr = self.sitePropers[site]
                if sitePr.onWishList(datasetName):
                    countWishes = countWishes + 1

            if dataPr.nSites()-dataPr.nBeDeleted() - countWishes > (self.DETOX_NCOPY_MIN-1):
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

                        if sitePr.pinDataset(datasetName):
                            nprotected = nprotected + 1
                            if nprotected >= self.DETOX_NCOPY_MIN:
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
            if(os.path.isdir(subd)):
                shutil.rmtree(subd)
            else:
                os.remove(subd)

        today = str(datetime.date.today())
        ttime = time.strftime("%H:%M")

        usedSets = self.popularityHandler.getUsedDatasets()
        totalSpaceTaken = 0
        totalSpaceLcopy = 0
        totalDisk = 0
        # file with more infortmation on all sites
        outputFile = open(os.environ['DETOX_DB'] + "/SitesInfo.txt",'w')
        outputFile.write('#- ' + today + " " + ttime + "\n#\n")
        outputFile.write("#- S I T E S  I N F O R M A T I O N ----\n#\n")
        outputFile.write("#  Active Quota[TB] Taken[TB] LastCopy[TB] SiteName \n")
        for site in sorted(self.allSites):
            theSite = self.allSites[site]
            taken = 0
            lcopy = 0
            if theSite.getStatus() != 0:
                sitePr = self.sitePropers[site]
                taken = sitePr.spaceTaken()/1000
                lcopy = sitePr.spaceLastCp()/1000
                totalDisk = totalDisk + theSite.getSize()/1000
                totalSpaceLcopy = totalSpaceLcopy + lcopy
                totalSpaceTaken = totalSpaceTaken + taken

            # summary of all sites
            outputFile.write("   %-6d %-9d %-9d %-12d %-20s \n"\
                                 %(theSite.getStatus(),theSite.getSize()/1000,taken,lcopy,site))
        outputFile.write("#------------------------------------------------------\n")
        outputFile.write("#  %-6d %-9d %-9d %-12d %-20s \n"\
                             %(len(self.allSites.keys()),totalDisk,
                               totalSpaceTaken,totalSpaceLcopy,'Total'))
        percTst = totalSpaceTaken/totalDisk*100
        percTslc = totalSpaceLcopy/totalDisk*100
        outputFile.write("#  %-6s %-9s %-4.1f%%     %-4.1f%% %-20s \n"%(' ',' ',percTst,percTslc,' '))
        outputFile.write("# Total Active Quota  = %-9d \n"%(totalDisk))
        outputFile.close()

        outputFile = open(os.environ['DETOX_DB'] + "/DeletionSummary.txt",'w')
        outputFile.write('#- ' + today + " " + ttime + "\n\n")
        outputFile.write("#- D E L E T I O N  R E Q U E S T S ----\n\n")
        outputFile.write("#  NDatasets Size[TB] SiteName \n")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            datasets2del = sitePr.delTargets()
            nsets =  len(datasets2del)
            if nsets < 1:
                continue

            totalSize = 0
            for dataset in datasets2del:
                totalSize =  totalSize + sitePr.dsetSize(dataset)
            outputFile.write("   %-9d %-8d %-20s \n"\
                                 %(nsets,totalSize/1000,site))
        outputFile.close()

        deprecatedSpace = 0
        totalSets = 0
        outputFile = open(os.environ['DETOX_DB'] + "/DeprecatedSummary.txt",'w')
        outputFile.write('#- ' + today + " " + ttime + "\n\n")
        outputFile.write("#- D E P R E C A T E D  D A T A S E T S ----\n\n")
        outputFile.write("#  NDatasets Size[TB] SiteName \n")
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
        perc = deprecatedSpace/totalDisk*100
        outputFile.write("#  %-9s %-3.1f%% %-20s \n"%('',perc,''))
        outputFile.write("# Total Active Quota = %-9d \n"%(totalDisk))
        outputFile.close()

        incompleteSpace = 0
        totalSets = 0
        totalTrueSize = 0
        totalDiskSize = 0
        outputFile = open(os.environ['DETOX_DB'] + "/IncompleteSummary.txt",'w')
        outputFile.write('#- ' + today + " " + ttime + "\n\n")
        outputFile.write("#- I N C O M P L E T E  D A T A S E T S ----\n\n")
        outputFile.write("#  NDatasets TrueSize[TB] Size[TB] SiteName \n")
        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            trueSize = 0
            diskSize = 0
            nsets = 0
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
        percTs = totalTrueSize/totalDisk*100
        percTd = totalDiskSize/totalDisk*100
        outputFile.write("#  %-9s %-3.1f%%         %-3.1f%% %-20s \n"%('',percTs,percTd,''))
        delta = totalTrueSize-totalDiskSize
        perc = delta/totalDisk*100
        outputFile.write("# Missing Space      = %-4d (%-3.1f%%)\n"%(delta,perc))
        outputFile.write("# Total Active Quota = %-9d \n"%(totalDisk))
        outputFile.close()


        for site in sorted(self.sitePropers.keys(), key=str.lower, reverse=False):
            sitePr = self.sitePropers[site]
            sitedir = resultDirectory + "/" + site
            if not os.path.exists(sitedir):
                os.mkdir(sitedir)

            fileTimest = sitedir + "/Summary.txt"
            fileRemain = sitedir + "/RemainingDatasets.txt"
            fileDelete = sitedir + "/DeleteDatasets.txt"
            fileDeprec = sitedir + "/DeprecatedSets.txt"
            fileIncomp = sitedir + "/IncompleteSets.txt"
            fileWrGroup= sitedir + "/RunAwayGroupSets.txt"

            outputFile = open(fileTimest,'w')
            outputFile.write('#- ' + today + " " + ttime + "\n\n")
            outputFile.write("#- D E L E T I O N  P A R A M E T E R S ----\n\n")
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
            outputFile = open(fileDelete,'w')
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
                print '  -> ' + dset
            outputFile.close()

            outputFile = open(fileRemain,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n\n")
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

            outputFile = open(fileDeprec,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n\n")
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
            
            outputFile = open(fileIncomp,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n\n")
            outputFile.write("#   Rank    TrueSize DiskSize nsites DatasetName\n")
            outputFile.write("#[~days]    [GB]     [GB]\n")
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
                outputFile.write("%8.1f %11.1f %8.1f %6d  %s\n"\
                                     %(rank,trueSize,size,nsites-ndeletes,dset))
            outputFile.close()

            outputFile = open(fileWrGroup,'w')
            outputFile.write("# -- " + today + " " + ttime + "\n")
            outputFile.write("#------------------------------------\n")
            runAwayGroups =  self.phedexHandler.getRunAwayGroups(site)
            runAwaySets =  self.phedexHandler.getRunAwaySets(site)
            for group in sorted(runAwayGroups):
                outputFile.write("\n"+group+":\n")
                for dset in sorted(runAwaySets):
                    if runAwaySets[dset] == group:
                        outputFile.write(dset+"\n")
            outputFile.close()


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

            totalSize = 0
            thisRequest = deletionRequest.DeletionRequest(0,site,now_tstamp)
            for dataset in datasets2del:
                totalSize =  totalSize + sitePr.dsetSize(dataset)
                thisRequest.update(dataset,sitePr.dsetRank(dataset),sitePr.dsetSize(dataset))
            print "Deletion request for site " + site
            
            print " -- Number of datasets       = " + str(len(datasets2del))
            print "%s %0.2f %s" %(" -- Total size to be deleted =",totalSize/1000,"TB")

            if site in self.siteRequests:
                lastReqId = self.siteRequests[site].getLastReqId()
                lastRequest = self.delRequests[lastReqId]
                #they can look identical
                #resubmit in case it was submitted too long ago
                if thisRequest.looksIdentical(lastRequest):
                    #if thisRequest.deltaTime(lastRequest)/(60*60) < 72 :
                        print " -- Skipping submition, looks like a request " + str(lastReqId)
                        continue
            numberRequests = numberRequests + 1

            (reqid,rdate) = self.submitDeletionRequest(site,datasets2del)
            if site.startswith('T2'):
                self.submitUpdateRequest(site,reqid)
            print " -- Request Id =  " + str(reqid)

            thisRequest.reqId = reqid
            thisRequest.tstamp = rdate
            self.delRequests[reqid] = deletionRequest.DeletionRequest(reqid,site,rdate,thisRequest)
            if site not in self.siteRequests:
                self.siteRequests[site] = deletionRequest.SiteDeletions(site)
            self.siteRequests[site].update(reqid,rdate)

            connection = self.getDbConnection()
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

    def extractCacheRequests(self):
        connection = self.getDbConnection()
        cursor = connection.cursor()
        sql = "select RequestId,DatasetName,Size,Rank,Date,SiteName " +\
            "from Requests,Sites,Datasets,DatasetProperties " +\
            "where Requests.SiteId=Sites.SiteId " +\
            "and Requests.DatasetId=Datasets.DatasetId and DatasetProperties.DatasetId=Datasets.DatasetId "+\
            "and RequestType=1 order by RequestId DESC LIMIT 10000"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except MySQLdb.Error,e:
            print e[0],e[1]
            print " -- FAILED extract mysql info: %s"%(sql)
            connection.close()
            sys.exit(1)
        connection.close()

        for row in results:
            reqid  = row[0]
            tstamp = row[4]
            site   = row[5]
            if reqid not in self.delRequests:
                self.delRequests[reqid] = deletionRequest.DeletionRequest(reqid,site,tstamp)
            if site not in self.siteRequests:
                self.siteRequests[site] = deletionRequest.SiteDeletions(site)
            dataset = row[1]
            size    = row[2]
            rank    = row[3]

            self.delRequests[reqid].update(dataset,rank,size)
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
            prevRequest = theRequest

            outputFile.write("#\n# PhEDEx Request: %s (%10s, %.1f GB)\n"%\
                             (reqid,theRequest.getTimeStamp(),theRequest.getSize()))

            outputFile.write("#\n# Rank   DatasetName\n")
            outputFile.write("# ---------------------------------------\n")

            for dataset in theRequest.getDsets():
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
        del phedex
        return (reqid,rdate)

    def submitUpdateRequest(self,site,reqid):
        # here we brute force deletion to be approved
        phedex = phedexApi.phedexApi(logPath='./')
        check,response = phedex.updateRequest(decision='approve',request=reqid,node=site,instance='prod')
        if check:
            print " ERROR - phedexApi.updateRequest failed - reqid="+ str(reqid)
            print response
        del phedex

    def getDbConnection(self,db=os.environ.get('DETOX_SITESTORAGE_DB')):
        # configuration
        server = os.environ.get('DETOX_SITESTORAGE_SERVER')
        user = os.environ.get('DETOX_SITESTORAGE_USER')
        pw = os.environ.get('DETOX_SITESTORAGE_PW')
        # open database connection
        connection = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
        # prepare a cursor object using cursor() method
        return connection

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
