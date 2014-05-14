#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
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
# -------
# - important to clarify the password issue!! (there is a temporary but not too safe fix)
# - we need to cleanly separate main and helpers, please, no global variables
#
#---------------------------------------------------------------------------------------------------
import subprocess, sys, os, re, glob, time, datetime

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setupDetox.sh\n'
    sys.exit(0)

if len(sys.argv) < 2:
    print 'not enough arguments\n'
    sys.exit()
else:
    site = str(sys.argv[1])

dates = [];

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def getDatasetsPopularity():
 
    getPopularityData = True
    
    dirname = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
              site + '/' + os.environ['DETOX_SNAPSHOTS']

    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # 

    items = len(dates)
    for i in range(items-1):
        outputFile = dirname + '/' + str(dates[i])

        if os.path.exists(outputFile):
            if i == items-2:
                # last snapshot is always remade if the file is older than one day
                if time.time() - os.path.getmtime(outputFile) < (24 * 60 * 60):
                    continue
            else:
                # always attempt to remake empty snapshots
                if os.path.getsize(outputFile) > 0:
                    continue

        if getPopularityData:
            tEnd = str(dates[i])
            tStart = str(dates[i+1])
            cmd = os.environ['DETOX_BASE'] + '/' + \
                  'popularityClient.py  /popularity/DSStatInTimeWindow/' + \
                  '\?\&sitename=' + site + '\&tstart=' + tStart + '\&tstop=' + tEnd
            process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
            strout, error = process.communicate()
            if process.returncode != 0:
                print " Received exit status " + str(process.returncode)
                raise Exception(" FATAL - Failed to extract popularity for site " + site)

            fileHandle = open(outputFile, "w")
            fileHandle.write(strout)
            fileHandle.close()

            # make sure file is clean
            isFileCorrupt = False
            inputFile = open(outputFile,'r')
            for line in inputFile.xreadlines():
                if "Traceback" in line:
                    isFileCorrupt = True
                if "Error" in line:
                    isFileCorrupt = True
            inputFile.close();

            # deal with file corruption
            if isFileCorrupt:
                raise Exception(" FATAL - Found corrupted snapshot for site " + site)

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
    d = datetime.date(year, 1, 1)
    delta_days = d.isoweekday() - 1
    delta_weeks = week
    if year == d.isocalendar()[0]:
        delta_weeks -= 1
    delta = datetime.timedelta(days=-delta_days, weeks=delta_weeks)
    return d + delta

def firstDayOfMonth(d):
    return datetime.date(d.year, d.month, 1)

#===================================================================================================
#  M A I N
#===================================================================================================
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
    delta = datetime.timedelta(days=-2)
    beginningOfWeek = firstDayOfMonth(beginningOfWeek+delta)
    dates.append(beginningOfWeek)

# time the popularity access
timeStart = time.time()

# here we loop through the required time intervals for the given site to extract popularity info
getDatasetsPopularity()

# timing result
timeNow = time.time()
print '   - Popularity queries for %s took: %d seconds'%(site,timeNow-timeStart)
