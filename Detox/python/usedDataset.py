#----------------------------------------------------------------------------------------------------
#
# This class provides support for storing datasets information extracted from Popularity service
#
#----------------------------------------------------------------------------------------------------
import time, datetime, math, os

class TimeSequence:
    def __init__(self):
        self.timeStamps = {}

    def update(self,utime,nacc):
        self.timeStamps[utime] = nacc

    def timesUsed(self,tstamp):
        timeLine = int(time.mktime(tstamp.timetuple()))

        nacc = 0
        for stamp in self.timeStamps:
            thisTime = int(time.mktime(time.strptime(stamp, "%Y-%m-%d")))
            if thisTime > timeLine:
                nacc = nacc + self.timeStamps[stamp]
        return nacc

class UsedDataset:

    def __init__(self, dataset, timenow):
        self.dataset = dataset
        self.siteNames = {}
        self.siteSequence = {}
        self.lastUsedAtSite = {}
        self.timesUsedAtSite = {}
	self.timeNow = int(time.mktime(timenow.timetuple()))

    def updateForSite(self,site,utime,nacc):
        if site.startswith("T1_"):
            site += "_Disk"

        if site not in self.siteNames:
            self.siteNames[site] = 1

        if site not in self.lastUsedAtSite:
            self.lastUsedAtSite[site] = utime
        else:
            prepoch = int(time.mktime(time.strptime(self.lastUsedAtSite[site], "%Y-%m-%d")))
            now = int(time.mktime(time.strptime(utime, "%Y-%m-%d")))
            if now > prepoch:
                self.lastUsedAtSite[site] = utime

	#introduce exponential decay with time
	#unite is day, reduce by 10 in 180 days
        koeff = 1.0
        if int(os.environ['DETOX_APP_TDECAY']) == 1:
            secSinceUsed = int(time.mktime(time.strptime(utime, "%Y-%m-%d")))
            deltaDays = (self.timeNow - secSinceUsed)/(60*60*24)
            e1 = "%.1f" % (deltaDays/60.0)
            e2 = float(e1)
            koeff = 10.0**(e2)
	nacc = nacc/koeff

        if site not in self.timesUsedAtSite:
            self.timesUsedAtSite[site] = nacc
            self.siteSequence[site] = TimeSequence()
        else:
            self.timesUsedAtSite[site] =  self.timesUsedAtSite[site] + nacc

        self.siteSequence[site].update(utime,nacc)

    def timesUsedSince(self,tstamp,site):
        if site not in self.siteSequence:
            return 0
        else:
            return self.siteSequence[site].timesUsed(tstamp)

    def locatedOnSites(self):
        return self.siteNames.keys()

    def isOnSite(self,site):
        if site in self.siteNames:
            return True
        return False

    def lastUsed(self,site):
        return self.lastUsedAtSite[site]

    def timesUsed(self,site):
        return self.timesUsedAtSite[site]
