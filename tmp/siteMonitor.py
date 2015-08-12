#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the SiteMonitor. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, datetime, ConfigParser, time, sqlite3
import sites
import crabApi

# initialize
startingTime = datetime.datetime.now()
print " ----  Start time : " + startingTime.strftime('%Hh %Mm %Ss') + "  ---- "
print ""

config = ConfigParser.RawConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
siteCache = config.get('data_dealer', 'site_cache')
if not os.path.exists(siteCache):
    os.makedirs(siteCache)
cacheFile = "%s/%s.db" % (siteCache, "siteCache")
timeNow = int(time.time())
deltaNSeconds = 60*60*1
if os.path.isfile(cacheFile):
    modTime = os.path.getmtime(cacheFile)
    if (os.path.getsize(cacheFile)) == 0 or ((timeNow-deltaNSeconds) > modTime):
        os.remove(cacheFile)

#===================================================================================================
#  M A I N
#===================================================================================================
# get sites
print " ----  Get Sites  ---- "
startTime = datetime.datetime.now()
sites_ = sites.sites()
availableSites = sites_.getAvailableSites()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# get cpus for sites
print " ----  Update CPUs  ---- "
startTime = datetime.datetime.now()
crabApi_ = crabApi.crabApi()
siteCache = sqlite3.connect(cacheFile)
timestamp = timeNow - 60*60*24*60
with siteCache:
    cur = siteCache.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS SiteCpu(SiteName TEXT, Cpus INTEGER, Timestamp INTEGER)')
    cur.execute('DELETE FROM SiteCpu WHERE Timestamp<?', (timestamp,))

for site in availableSites:
    cpus = crabApi_.getCpus(site)
    with siteCache:
        cur = siteCache.cursor()
        cur.execute('INSERT INTO SiteCpu(SiteName, Cpus) VALUES(?, ?)', (site, cpus))

totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

#===================================================================================================
#  E X I T
#===================================================================================================
print " ----  Done  ---- "
endingTime = datetime.datetime.now()
totalTime = endingTime - startingTime
print " ----  Complete run took " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""
print " ----  End time : " + endingTime.strftime('%Hh %Mm %Ss') + "  ---- "

sys.exit(0)
