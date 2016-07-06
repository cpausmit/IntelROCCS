#===================================================================================================
#  C L A S S
#===================================================================================================
import os, re, sys, MySQLdb
import datetime
import siteStatus

class DbInfoHandler:
    def __init__(self):
        self.allSites = {}
        self.idToSite = {}
        self.datasetNames = {}
        self.datasetIds = {}
        self.datasetSizes = {}
        self.datasetFiles = {}
        self.phedexGroups = (os.environ['DETOX_GROUP']).split(',')
        self.phgroupIds = {}
        self.extractGroupIds()
        self.extractAllSites()
        self.extractDatasetIds()
        self.extractDatasetSizes()

    def setDatasetRanks(self,dsetRanks):
        for dset in dsetRanks:
            self.datasetRanks[dset] = dsetRanks[dset][0]

    def extractGroupIds(self):
        connection = self.getDbConnection()
        cursor = connection.cursor()

        sql = 'select GroupId,GroupName from Groups' 
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
            groupId = int(row[0])
            groupName = row[1]
            self.phgroupIds[groupName] = groupId

    def extractAllSites(self):
        phedGroups = []
        for group in self.phedexGroups:
            subgroups = group.split('+')
            phedGroups.extend(subgroups)

        print ' Access site storage database to find all sites.'
        connection = self.getDbConnection()
        cursor = connection.cursor()

        sql = 'select SiteName,SiteId,Status from Sites' 
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print ' Error(%s) -- could not retrieve sites'%(sql)
            print sys.exc_info()
            connection.close()
            sys.exit(1)

        for row in results:
            siteName = row[0]
            siteId = int(row[1])
            status = int(row[2])
            if status == -1:
                continue
            self.allSites[siteName] = siteStatus.SiteStatus(siteName)
            self.allSites[siteName].setId(siteId)
            self.allSites[siteName].setStatus(status)
            self.idToSite[siteId] = siteName

            for group in phedGroups:
                siteSizeGb = self.getGroupQuota(cursor,group,siteId)
                self.allSites[siteName].setSize(siteSizeGb,group)
        connection.close()

    def getGroupQuota(self,cursor,group,siteId):

        sql = 'select SizeTb from Quotas,Groups'
        sql = sql + ' where  Groups.GroupName=\'' + group + '\''
        sql = sql + ' and SiteId=' + str(siteId) + ' and Quotas.GroupId=Groups.GroupId'         
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print ' Error(%s) -- could not retrieve quotas'%(sql)
            print sys.exc_info()
            connection.close()
            sys.exit(1)

        siteSizeGb = 0.0
        for row in results:
            siteSizeGb = float(row[0])*1000
        return siteSizeGb 

    def logRequest(self,site,datasets,reqid,rdate,reqtype):
        self.extractDatasetIds(datasets)

        connection = self.getDbConnection()
        for dataset in datasets:
            siteId = self.allSites[site].getId()
            if dataset not in self.datasetIds:
                continue
            if dataset not in self.datasetRanks:
                continue
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
   
    def extractDatasetIds(self):
        connection = self.getDbConnection()
        cursor = connection.cursor()
        sql = "select * from Datasets"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print " -- FAILED extract mysql info: %s"%(sql)
            connection.close()
            sys.exit(1)
        connection.close()

        for row in results:
            dsetId = int(row[0])
            dsetName = row[1]
            self.datasetNames[dsetId] = dsetName
            self.datasetIds[dsetName] = dsetId

    def extractDatasetSizes(self):
        connection = self.getDbConnection()
        cursor = connection.cursor()
        sql = "select * from DatasetProperties"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print " -- FAILED insert mysql info: %s"%(sql)
            connection.close()
            sys.exit(1)
        connection.close()

        for row in results:
            dsetId = int(row[0])
            self.datasetFiles[dsetId] = int(row[1])
            self.datasetSizes[dsetId] = float(row[2])
     
    def getAllSites(self):
        return self.allSites

    def siteExists(self,siteId):
        if siteId in self.idToSite:
            return True
        else:
            return False

    def getSiteName(self,siteId):
        if siteId in self.idToSite:
            return self.idToSite[siteId]
        
    def datasetExists(self,dsetId):
        if dsetId in self.datasetSizes:
            return True
        else:
            return False

    def getDatasetName(self,dsetId):
        return self.datasetNames[dsetId]
    def getDatasetSize(self,dsetId):
        return self.datasetSizes[dsetId]
    def getDatasetFiles(self,dsetId):
        return self.datasetFiles[dsetId]

    def getDatasetId(self,dsetName):
        if dsetName in self.datasetIds:
            return self.datasetIds[dsetName]
        else:
            return -1

    def getGroupId(self,group):
        return self.phgroupIds[group]

    def dbExecSql(self,sql):
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

    def updateSiteStatus(self, changingStatus):
        connection = self.getDbConnection()
        for site in changingStatus: 
            siteId = str(self.allSites[site].getId())
            newStatus = str(changingStatus[site])
            sql = 'update Sites set Status=' +  newStatus + ' where SiteId=' + siteId 
            sql = sql + ' limit 1'
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
            except:
                print ' Error(%s) -- could not execute sql '%(sql)
                print sys.exc_info()
        connection.close()

    def extractCacheRequests(self):
        connection = self.getDbConnection()
        cursor = connection.cursor()
        sql = "select * from Requests where RequestType=1"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except MySQLdb.Error,e:
            print e[0],e[1]
            print " -- FAILED extract mysql info: %s"%(sql)
            connection.close()
            sys.exit(1)
        connection.close()
        return results

    def getDbConnection(self,db=os.environ.get('DETOX_SITESTORAGE_DB')):
        # configuration
        #server = os.environ.get('DETOX_SITESTORAGE_SERVER')
        #user = os.environ.get('DETOX_SITESTORAGE_USER')
        #pw = os.environ.get('DETOX_SITESTORAGE_PW')
        # open database connection
        #connection = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
   	connection = MySQLdb.connect(read_default_file="/etc/my.cnf",read_default_group="mysql-ddm",db="IntelROCCS")
        # prepare a cursor object using cursor() method
        return connection

    
