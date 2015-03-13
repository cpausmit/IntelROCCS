#===================================================================================================
#  C L A S S
#===================================================================================================
import os, re, sys, MySQLdb
import datetime
import siteStatus

class DbInfoHandler:
    def __init__(self):
        self.allSites = {}
        self.datasetIds = {}
        self.datasetSizes = {}
        self.datasetRanks = {}
        
        self.extractAllSites()

    def setDatasetRanks(self,dsetRanks):
        for dset in dsetRanks:
            self.datasetRanks[dset] = dsetRanks[dset][0]

    def extractAllSites(self):
        print ' Access site storage database to find all sites.'
        
        # AnalysisOps is GroupId=1
        sql = "select SiteName,SizeTb,Sites.Status,Sites.SiteId from Quotas,Sites "
        sql = sql + "where GroupId=1 and Quotas.SiteId=Sites.SiteId"
        
        results = self.dbExecSql(sql)
        for row in results:
            siteName = row[0]
            if "T1_" in siteName:
                continue
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
  
    def enableSite(self,site,targetQuota):
        if site == 'T2_TW_Taiwan':
            return

        siteId = self.allSites[site].getId()
        
        print "\n !! Activating site " + site + " !!" 
        
            # Activate site
        if targetQuota  > self.allSites[site].getSize():
            print "  - set quota to " + str(targetQuota) + " -" 
            sql = 'update Quotas set SizeTb=' + str(targetQuota)
            sql = sql + ' where SiteId=' + str(siteId) + ' limit 1'
            self.dbExecSql(sql)

        sql = 'update Sites set Status=1 where SiteId=' + str(siteId) 
        sql = sql + ' limit 1'
        self.dbExecSql(sql)

    def disableSite(self,site):
        siteId = self.allSites[site].getId()
        
        print "\n !! Disabling site " + site + " !!" 

        sql = 'update Sites set Status=0 where SiteId=' + str(siteId) + ' limit 1'
        self.dbExecSql(sql)
     

    def logRequest(self,site,datasets,reqid,rdate,reqtype):
        self.extractDatasetIds(datasets)

        connection = self.getDbConnection()
        for dataset in datasets:
            siteId = self.allSites[site].getId()
            dsetId = self.datasetIds[dataset]
            groupId = 1
            rank = self.datasetRanks[dataset]
            
            cursor = connection.cursor()
            sql = "insert into Requests(RequestId,RequestType,SiteId,DatasetId,Rank,GroupId,Date) " \
                "values('%d','%d','%d','%d','%d','%d','%s')" % \
                (reqid, reqtype, siteId, dsetId, rank, groupId, rdate)
            try:
                cursor.execute(sql)
                connection.commit()
            except:
                print " -- FAILED insert mysql info: %s"%(sql)
                connection.close()
                sys.exit(1)
        connection.close()
   
    def extractDatasetIds(self,datasets):
        connection = self.getDbConnection()
        for dataset in datasets:
            if dataset in self.datasetIds:
                continue
            
            cursor = connection.cursor()
            sql = "select DatasetId from Datasets where "
            sql = sql + "DatasetName='" + dataset + "'"
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
            except:
                print " -- FAILED insert mysql info: %s"%(sql)
                connection.close()
                sys.exit(1)

            for row in results:
                dsetId = row[0]
            self.datasetIds[dataset] = dsetId
        connection.close()

    def extractDatasetSizes(self,datasets):
        self.extractDatasetIds(datasets)
        dsetSizes = {}

        connection = self.getDbConnection()
        for dataset in datasets:
            cursor = connection.cursor()
            if dataset in self.datasetSizes:
                dsetSizes[dataset] = self.datasetSizes[dataset]
                continue

            sql = "select Size from DatasetProperties where "
            sql = sql + "DatasetId=" + str(self.datasetIds[dataset])
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
            except:
                print " -- FAILED insert mysql info: %s"%(sql)
                connection.close()
                sys.exit(1)

            for row in results:
                dsetSize = row[0]
            self.datasetSizes[dataset] = dsetSize
            dsetSizes[dataset] = dsetSize
        connection.close()
        return dsetSizes
     
    def getAllSites(self):
        return self.allSites
            

    def dbExecSql(self,sql):
        print sql
        connection = self.getDbConnection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print ' Error(%s) -- could not execute sql '%(sql)
            print sys.exc_info()
            connection.close()
            sys.exit(1)
        connection.close()
        return results

    def getDbConnection(self,db=os.environ.get('CARETAKER_SITESTORAGE_DB')):
        # configuration
        server = os.environ.get('CARETAKER_SITESTORAGE_SERVER')
        user = os.environ.get('CARETAKER_SITESTORAGE_USER')
        pw = os.environ.get('CARETAKER_SITESTORAGE_PW')
        # open database connection
        connection = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
        # prepare a cursor object using cursor() method
        return connection
