#====================================================================================================
#  C L A S S
#====================================================================================================
import subprocess, sys, os, re, glob, time, datetime, signal, json
import usedDataset


class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class PopularityDataHandler:
    def __init__(self,allSites):

        self.datasets = {}
        self.allSites = allSites

        #fill in dates object that will determine time intervals
        self.dates = []
        self.timeNow = datetime.datetime.now()
        thisYear,thisWeekNumber,DOW = self.timeNow.isocalendar()

        today = datetime.date.today()
        timeDelta = datetime.timedelta(int(os.environ['DETOX_TIME_WIND'])*30)

        # generate the relevant intervals for the popularity information
        for i in range(0,9):
            weekNumber = thisWeekNumber - i
            beginningOfWeek = self.weekStartDate(thisYear,weekNumber)
            if today-timeDelta < beginningOfWeek:
                self.dates.append(beginningOfWeek)

        beginningOfWeek = self.firstDayOfMonth(beginningOfWeek)
        self.dates.append(beginningOfWeek)

        monthsBack =  int(os.environ['DETOX_TIME_WIND'])
        for i in range(0,monthsBack):
            beginningOfWeek = self.firstDayOfMonth(beginningOfWeek+datetime.timedelta(days=-2))
            self.dates.append(beginningOfWeek)

    def extractPopularityData(self):
        notValidSites = 0

        for site in sorted(self.allSites):
            self.accessPopularityData(site)
            if self.allSites[site].getValid() == 0:
                notValidSites = notValidSites + 1
            if notValidSites > 20:
                raise Exception(" FATAL - Popularity service seems to be down")

    def accessPopularityData(self,site):

        timeStart = time.time()
        dirname = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
                  site + '/' + os.environ['DETOX_SNAPSHOTS']
        self.cleanOutdatedSnapshots(dirname)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        siteAdjust = site.replace('_Disk', '')
        badsnapshots = 0
        items = len(self.dates)
        for i in range(items-1):
            if badsnapshots > 4 :
                self.allSites[site].setValid(0)
                break

            outputFile = dirname + '/' + str(self.dates[i])
            if os.path.exists(outputFile):
                if i == items-2:
                    # last snapshot is always remade if the file is older than one day
                    if time.time() - os.path.getmtime(outputFile) < (24* 60 * 60):
                        continue
                else:
                    # always attempt to remake empty snapshots
                    if os.path.getsize(outputFile) > 0:
                        continue

            tEnd = str(self.dates[i])
            tStart = str(self.dates[i+1])
            cmd = os.environ['DETOX_BASE'] + '/' + \
                  'popularityClient.py  /popularity/DSStatInTimeWindow/' + \
                  '\?\&sitename=' + siteAdjust + '\&tstart=' + tStart + '\&tstop=' + tEnd
            process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)


            signal.signal(signal.SIGALRM, alarm_handler)
            signal.alarm(2*60)  # 2 minutes
            try:
                strout, error = process.communicate()
                signal.alarm(0)
            except Alarm:
                print " Oops, taking too long!"
                raise Exception(" FATAL -- Popularity query timed out, stopping")

            if process.returncode != 0:
                print "    - Popularity queries got bad status " + str(process.returncode) + " for " + site
                badsnapshots = badsnapshots + 1
                continue

            fileHandle = open(outputFile, "w")
            fileHandle.write(strout)
            fileHandle.close()

        for i in range(items-1):
            # make sure file is clean, then read what is in it and disect information
            rawinp = None
            isFileCorrupt = False
            outputFile = dirname + '/' + str(self.dates[i])
            if not os.path.exists(outputFile):
                continue

            inputFile = open(outputFile,'r')
            for line in inputFile.xreadlines():
                if "Traceback" in line:
                    isFileCorrupt = True
                if "Error" in line:
                    isFileCorrupt = True
                if "SITENAME" in line:
                    rawinp = line
            inputFile.close();

            # deal with file corruption
            if isFileCorrupt:
                print " Bad snapshot detected for site " + site
                badsnapshots = badsnapshots + 1
                os.remove(outputFile)
                continue

            #use goodsnapshot
            date = (re.search(r"(\d+)-(\d+)-(\d+)", outputFile)).group(0)
            self.processRawInput(siteAdjust,date,rawinp)

        timeNow = time.time()
        print '   - Popularity queries for %s took: %d seconds'%(site,timeNow-timeStart)

    def processRawInput(self,site,date,rawinp):

        #print ' #### RAWINPUT #### ' + rawinp

        dataJason = json.loads(rawinp)
        thisSite = dataJason["SITENAME"]
        if thisSite != site:
            print " !!!!!! Popularity site screw up !!!!!!"
            print thisSite
            print site
        for entry in dataJason["DATA"]:
            dataset = entry["COLLNAME"]
            nAccessed = entry["NACC"]

            if dataset not in self.datasets :
                self.datasets[dataset] = usedDataset.UsedDataset(dataset,self.timeNow)

            self.datasets[dataset].updateForSite(site,date,nAccessed)

    def cleanOutdatedSnapshots(self,dirname):

        today = datetime.date.today()
        twindow = int(os.environ['DETOX_TIME_WIND']) + 1
        pastDate = self.subtractDate(today, twindow)

        files = glob.glob(dirname + '/????-??-??')
        for file in files:
            ma = re.search(r"(\d+)-(\d+)-(\d+)", file)
            dt = datetime.date(int(ma.group(1)),int(ma.group(2)),int(ma.group(3)))
            if dt not in self.dates:
                print "     - Cache cleanup: remove " + ma.group()
                os.remove(file)
            #if dt < pastDate:
            #   print "     - Cache cleanup: remove " + ma.group()
            #  os.remove(file)

    def weekStartDate(self,year, week):
        d = datetime.date(year, 1, 1)
        delta_days = d.isoweekday() - 1
        delta_weeks = week
        if year == d.isocalendar()[0]:
            delta_weeks -= 1
        delta = datetime.timedelta(days=-delta_days, weeks=delta_weeks)
        return d + delta

    def firstDayOfMonth(self,d):
        return datetime.date(d.year, d.month, 1)

    def getUsedDatasets(self):
        return self.datasets

    def subtractDate(self, date, month = 0, year = 0):
        year, month = divmod(year*12 + month, 12)
        if date.month <= month:
            year = date.year - year - 1
            month = date.month - month + 12
        else:
            year = date.year - year
            month = date.month - month
        return self.firstDayOfMonth(date.replace(year = year, month = month, day = 10))
