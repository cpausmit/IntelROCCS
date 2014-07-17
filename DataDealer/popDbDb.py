#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# 
#---------------------------------------------------------------------------------------------------
import sys, os, json, sqlite3, datetime
sys.path.append(os.path.dirname(os.environ['INTELROCCS_BASE']))
import sites
import IntelROCCS.Api.popDb.popDbData as popDbData
import IntelROCCS.Api.popDb.popDbApi as popDbApi

class popDbDb():
    def __init__(self, dbPath, oldestAllowedHours):
        dbFile = "%s/DSStatInTimeWindow.db" % (dbPath)
        update = 0
        if not os.path.exists(dbPath):
            os.makedirs(dbPath)
        timeNow = datetime.datetime.now()
        deltaNhours = datetime.timedelta(seconds = 60*60*(oldestAllowedHours))
        if os.path.isfile(dbFile):
            modTime = datetime.datetime.fromtimestamp(os.path.getmtime(dbFile))
            if os.path.getsize(dbFile) == 0:
                os.remove(dbFile)
                update = 1
        else:
            update = 1
        self.dbCon = sqlite3.connect(dbFile)
        cur = self.dbCon.cursor()
        cur.execute('PRAGMA foreign_keys = ON')
        cur.close()
        if update == 1:
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute('CREATE TABLE DatasetData (Day TEXT, DatasetName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
                cur.execute('CREATE TABLE SiteData (Day TEXT, SiteName TEXT, NumberAccesses INTEGER, NumberCpus INTEGER)')
        self.popDbData = popDbData.popDbData(dbPath, oldestAllowedHours)
        self.popDbApi = popDbApi.popDbApi()
        self.sites = sites.sites()
        allSites = self.sites.getAllSites()
        with self.dbCon:
            cur.execute('SELECT Day FROM SiteData ORDER BY Day DESC LIMIT 1')
            day = cur.fetchone()
            lastDate = datetime.date.today() - datetime.timedelta(days=22)
            if day:
                d = time.strptime(day[0], '%Y-%m-%d')
                lastDate = datetime.date(d.tm_year, d.tm_mon, d.tm_mday)
            td = datetime.timedelta(days=1)
            date = datetime.date.today() - td
            while date > lastDate:
                popDbJsonData = self.popDbData.getPopDbData("DSStatInTimeWindow", date.strftime('%Y-%m-%d'))
                #self.buildPopDbDb(popDbJsonData)
                #for site in allSites:
                #    popDbJsonData = self.popDbApi.DSStatInTimeWindow(tstart=date.strftime('%Y-%m-%d'), tstop=date.strftime('%Y-%m-%d'), sitename=site)
                date = date - td

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def buildPopDbDb(self, popDbJsonData):
        datasets = phedexJsonData.get('phedex').get('dataset')
        for dataset in datasets:
            datasetName = dataset.get('name')
            sizeGb = 0
            size = dataset.get('bytes')
            if not size:
                for block in dataset.get('block'):
                    sizeGb += int(block.get('bytes')/10**9)
            else:
                sizeGb = int(size/10**9)
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute('INSERT INTO Datasets(DatasetName, SizeGb) VALUES(?, ?)', (datasetName, sizeGb))
                datasetId = cur.lastrowid
                for replica in dataset.get('block')[0].get('replica'):
                    siteName = replica.get('node')
                    groupName = replica.get('group')
                    cur.execute('INSERT INTO Replicas(SiteName, DatasetId, GroupName) VALUES(?, ?, ?)', (siteName, datasetId, groupName))

    def getDatasetSize(self, datasetName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SizeGb FROM Datasets WHERE DatasetName=?', (datasetName,))
            sizeGb = cur.fetchone()[0] # TODO : Check that something is returned
            return sizeGb

    def getNumberReplicas(self, datasetName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SiteName, DatasetId FROM Replicas NATURAL JOIN Datasets WHERE Datasets.DatasetName=?', (datasetName,))
            replicas = 0
            for row in cur:
                replicas += 1
            return replicas

    def getSiteReplicas(self, datasetName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SiteName FROM Replicas NATURAL JOIN Datasets WHERE Datasets.DatasetName=? AND Replicas.GroupName=?', (datasetName, 'AnalysisOps'))
            sites = []
            for row in cur:
                sites.append(row[0])
            return sites

    def getSiteStorage(self, siteName):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT SizeGb FROM Datasets NATURAL JOIN Replicas WHERE Replicas.SiteName=? ANd Replicas.GroupName=?', (siteName, 'AnalysisOps'))
            storageGb = 0
            for row in cur:
                storageGb += row[0]
            return storageGb

if __name__ == '__main__':
    popDb = popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)
    sys.exit(0)
