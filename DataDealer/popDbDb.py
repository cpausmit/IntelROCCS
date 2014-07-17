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
                self.buildDatasetPopDb(popDbJsonData, date.strftime('%Y-%m-%d'))
                for site in allSites:
                    popDbJsonData = self.popDbApi.DSStatInTimeWindow(tstart=date.strftime('%Y-%m-%d'), tstop=date.strftime('%Y-%m-%d'), sitename=site)
                    self.buildSitePopDb(popDbJsonData, date.strftime('%Y-%m-%d'))
                date = date - td

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def buildDatasetPopDb(self, popDbJsonData, date):
        datasets = popDbJsonData.get('DATA')
        for dataset in datasets:
            datasetName = dataset.get('COLLNAME')
            numberAccesses = dataset.get('NACC')
            numberCpus = dataset.get('TOTCPU')
            with self.dbCon:
                cur = self.dbCon.cursor()
                cur.execute('INSERT INTO DatasetData(Day, DatasetName, NumberAccesses, NumberCpus) VALUES(?, ?, ?, ?)', (date, datasetName, numberAccesses, numberCpus))

    def buildSitePopDb(self, popDbJsonData, date):
        siteName = popDbJsonData.get('SITENAME')
        datasets = popDbJsonData.get('DATA')
        numberAccesses = 0
        numberCpus = 0
        for dataset in datasets:
            numberAccesses += dataset.get('NACC')
            numberCpus += dataset.get('TOTCPU')
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('INSERT INTO SiteData(Day, SiteName, NumberAccesses, NumberCpus) VALUES(?, ?, ?, ?)', (date, siteName, numberAccesses, numberCpus))

    def getDatasetAccesses(self, datasetName, date):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT NumberAccesses FROM DatasetData WHERE DatasetName=? AND Day=?', (datasetName, date))
            numberAccesses = cur.fetchone()[0] # TODO : Check that something is returned
            return numberAccesses

    def getDatasetCpus(self, datasetName, date):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT NumberCpus FROM DatasetData WHERE DatasetName=? AND Day=?', (datasetName, date))
            numberCpus = cur.fetchone()[0] # TODO : Check that something is returned
            return numberCpus

    def getSiteAccesses(self, siteName, date):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT NumberAccesses FROM SiteData WHERE SiteName=? AND Day=?', (siteName, date))
            numberAccesses = cur.fetchone()[0] # TODO : Check that something is returned
            return numberAccesses

    def getSiteCpus(self, siteName, date):
        with self.dbCon:
            cur = self.dbCon.cursor()
            cur.execute('SELECT NumberCpus FROM SiteData WHERE SiteName=? AND Day=?', (siteName, date))
            numberCpus = cur.fetchone()[0] # TODO : Check that something is returned
            return numberCpus

if __name__ == '__main__':
    popDb = popDbDb("%s/Cache/PopDbCache" % (os.environ['INTELROCCS_BASE']), 12)
    sys.exit(0)
