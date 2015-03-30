#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# getPhedexData.py
#---------------------------------------------------------------------------------------------------
import os, sqlite3, ConfigParser, datetime, time
import popDbApi

class popDbData:
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'api.cfg'))
        self.popDbCache = config.get('pop_db', 'cache')
        self.cacheDeadline = config.getint('pop_db', 'expiration_timer')
        self.popDbApi = popDbApi.popDbApi()

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def totimestamp(self, year=1970, month=1, day=1, epoch=datetime.datetime(1970,1,1)):
        dt = datetime.datetime(year, month, day)
        td = dt - epoch
        return td.seconds + td.days*86400

    def buildDSStatInTimeWindowCache(self, siteNames):
        if not os.path.exists(self.popDbCache):
            os.makedirs(self.popDbCache)
        cacheFile = "%s/%s.db" % (self.popDbCache, 'DSStatInTimeWindow')
        timeNow = time.time()
        deltaNSeconds = 60*60*28
        if os.path.isfile(cacheFile):
            modTime = os.path.getmtime(cacheFile)
            if (os.path.getsize(cacheFile)) == 0 or ((timeNow-deltaNSeconds) > modTime):
                os.remove(cacheFile)
        deltaNSeconds = 60*60*self.cacheDeadline
        if os.path.isfile(cacheFile):
            modTime = os.path.getmtime(cacheFile)
            if (timeNow-deltaNSeconds) > modTime:
                days = [1]
            else:
                return 0
        else:
            days = range(1, 15)
        popDbCache = sqlite3.connect(cacheFile)
        utcNow = datetime.datetime.utcnow()
        today = datetime.date(utcNow.year, utcNow.month, utcNow.day)
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS DSStatInTimeWindow(Date TEXT, SiteName TEXT, DatasetName TEXT, Accesses INTEGER, Cpus INTEGER, Users INTEGER)')
            for siteName in siteNames:
                for i in days:
                    date = today - datetime.timedelta(days=i)
                    tstart = date.strftime('%Y-%m-%d')
                    tstop = tstart
                    jsonData = self.popDbApi.DSStatInTimeWindow(tstart=tstop, tstop=tstop, sitename=siteName)
                    siteName = jsonData.get('SITENAME')
                    datasets = jsonData.get('DATA')
                    for dataset in datasets:
                        datasetName = dataset.get('COLLNAME')
                        accesses = dataset.get('NACC')
                        cpus = dataset.get('TOTCPU')
                        users = dataset.get('NUSERS')
                        cur.execute('INSERT INTO DSStatInTimeWindow(Date, SiteName, DatasetName, Accesses, Cpus, Users) VALUES(?, ?, ?, ?, ?, ?)', (tstart, siteName, datasetName, accesses, cpus, users))
        return 0

    def buildGetSingleDSstatCache(self, datasets):
        if not os.path.exists(self.popDbCache):
            os.makedirs(self.popDbCache)
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'getSingleDSstat'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS getSingleDSstat(Timestamp INTEGER, DatasetName TEXT, Accesses TEXT)')
        for datasetName in datasets:
            jsonData = self.popDbApi.getSingleDSstat(name=datasetName, aggr='day', orderby='naccess')
            days = jsonData.get('DATA')
            with popDbCache:
                cur = popDbCache.cursor()
                for day in days:
                    timestamp = int(day[0]/10**3)
                    accesses = day[1]
                    cur.execute('INSERT INTO getSingleDSstat(Timestamp, DatasetName, Accesses) VALUES(?, ?, ?)', (timestamp, datasetName, accesses))

    def getDatasetAccesses(self, date, datasetName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Accesses) FROM DSStatInTimeWindow WHERE DatasetName=? AND Date=?', (datasetName, date))
            accesses = cur.fetchone()[0]
            if not accesses:
                accesses = 0
        return accesses

    def getDatasetCpus(self, date, datasetName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Cpus) FROM DSStatInTimeWindow WHERE DatasetName=? AND Date=?', (datasetName, date))
            cpus = cur.fetchone()[0]
            if not cpus:
                cpus = 0
        return cpus

    def getDatasetUsers(self, date, datasetName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Users) FROM DSStatInTimeWindow WHERE DatasetName=? AND Date=?', (datasetName, date))
            users = cur.fetchone()[0]
            if not users:
                users = 0
        return users

    def getSiteAccesses(self, date, siteName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Accesses) FROM DSStatInTimeWindow WHERE SiteName=? AND Date=?', (siteName, date))
            accesses = cur.fetchone()[0]
            if not accesses:
                accesses = 0
        return accesses

    def getSiteCpus(self, date, siteName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Cpus) FROM DSStatInTimeWindow WHERE SiteName=? AND Date=?', (siteName, date))
            cpus = cur.fetchone()[0]
            if not cpus:
                cpus = 0
        return cpus

    def getSiteUsers(self, date, siteName):
        # access cache
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT sum(Users) FROM DSStatInTimeWindow WHERE SiteName=? AND Date=?', (siteName, date))
            users = cur.fetchone()[0]
            if not users:
                users = 0
        return users

    def getHistoricalData(self, datasetName):
        popDbCache = sqlite3.connect("%s/%s.db" % (self.popDbCache, 'popDbCache'))
        historicalData = []
        with popDbCache:
            cur = popDbCache.cursor()
            cur.execute('SELECT Timestamp, max(Accesses) FROM getSingleDSstat WHERE DatasetName=?', (datasetName))
            maximum = cur.fetchone()[0]
            for i in range(-7,8):
                timestamp = maximum + i*86400
                cur.execute('SELECT Accesses FROM getSingleDSstat WHERE DatasetName=? ANd Timestamp=?', (datasetName, timestamp))
                # if no data, set to 0
                row = cur.fetchone()
                accesses = 0
                if row:
                    accesses = row[0]
                historicalData.append(accesses)
        return historicalData
