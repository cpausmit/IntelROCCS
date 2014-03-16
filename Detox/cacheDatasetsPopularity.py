#!/usr/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script talks to the popularity service and extracts usage information for a given site
# (specified as an input parameter). The popularity service requires to specify a time window, hence
# the script starts with 1 months time windows (snapshots) 8 months in the past and then switches to
# weekly time intervals. The assumption is made that the last time the dataset was used was on the
# last day of the time interval.
#
# The script reuses previously extracted information and does not recreate a snapshot if it already
# exists (except for snapshots for this and previous week).  The snapshots that become old are
# deleted.  No formatting of the ouput is performed, it will be done later when the popularity
# information is combined with what is extracted from PhEDEx database.
#
# Issues:
#
# - important to clarify the password issue!! (there is a temporary but not too safe fix)
#
#----------------------------------------------------------------------------------------------------
import subprocess, sys, os, re, glob, time
import datetime
from   datetime import date, timedelta

if not os.environ.get('DETOX_DB'):
	print '\n ERROR - DETOX environment not defined: source setup.sh\n'
	sys.exit(0)

if len(sys.argv) < 2:
    print 'not enough arguments\n'
    sys.exit()
else:
    msite = str(sys.argv[1])

dates = [];
getPopularityData = True

#====================================================================================================
#  H E L P E R S
#====================================================================================================

def getDatasetsPopularity():

    dirname = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
              msite + '/' + os.environ['DETOX_SNAPSHOTS']

    if not os.path.exists(dirname):
        os.makedirs(dirname)

    items = len(dates)
    for i in range(0,items-1):
        outputFile = dirname + '/' + str(dates[i])
        # if snapshot exists move on (dangerous as this script might run several times per day
        # --> last snapshot has to be repeated
        if os.path.exists(outputFile):
            continue

        tEnd = str(dates[i])
        tStart = str(dates[i+1])

        append = '\?\&sitename=' + msite + '\&tstart=' + tStart + '\&tstop=' + tEnd;
        cmd = 'python popularityClient.py  /popularity/DSStatInTimeWindow/' + append;

        if getPopularityData:

	    # launch the shell command
	    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
				       shell=True)
	    strout, error = process.communicate()

            #child = pexpect.spawn(cmd,timeout=600)
            ## the trick is that you have to create a key file without password
            ## better would be to get a cern sso cookie, but that so far fails
            #child.expect (pexpect.EOF)
            #strout = child.before
            
            fileHandle = open(outputFile, "w")
            fileHandle.write(strout)
            fileHandle.close()

    files = glob.glob(dirname + '/??-??-??')
    for file in files:
         yearm = (re.search(r"(\d+)-(\d+)-(\d+)", file)).group(0)
         found = 0
         for i in range(0,items-1):
             dateTmp = str(dates[i])
             if dateTmp == yearm:
                 found = 1
                 break
         if found == 0:
             print " removing file " + file 
             os.remove(file)

def weekStartDate(year, week):
    d = date(year, 1, 1)    
    delta_days = d.isoweekday() - 1
    delta_weeks = week
    if year == d.isocalendar()[0]:
        delta_weeks -= 1
    delta = timedelta(days=-delta_days, weeks=delta_weeks)
    return d + delta

def firstDayOfMonth(d):
    return date(d.year, d.month, 1)

#====================================================================================================
#  M A I N
#====================================================================================================

# set the starting point
now = datetime.datetime.now()
thisYear,thisWeekNumber,DOW = now.isocalendar()

# generate the relevant intervals for the popularity information
for i in range(0,9):
    weekNumber = thisWeekNumber - i
    beginningOfWeek = weekStartDate(thisYear,weekNumber)
    dates.append(beginningOfWeek)

beginningOfWeek = firstDayOfMonth(beginningOfWeek)
dates.append(beginningOfWeek)

for i in range(0,9):
    delta = timedelta(days=-2)
    beginningOfWeek = firstDayOfMonth(beginningOfWeek+delta)
    dates.append(beginningOfWeek)

# time the popularity access
timeStart = time.time()

# here we loop through the required time intervals for the given site to extract popularity info
getDatasetsPopularity()

# timing result
timeNow = time.time()
print '   - Popularity queries for %s took: %d seconds'%(msite,timeNow-timeStart) 
