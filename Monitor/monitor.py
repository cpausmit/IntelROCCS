#!/usr/bin/python
'''
Wrapper script that reads the binaries and makes the standard plots
'''

import os
import sys
import time

genesis=1378008000
nowish = time.time()

# remove old log files
logPeriod = int((nowish/1000000)%10)
logBlock = int((nowish/10000000))
#os.system('rm $MONITOR_DB/monitor-%i[^%i%i]*'%(logBlock,logPeriod,logPeriod-1))

def addTime(timeStruct,addTuple):
    '''
        take timeStruct and add (y,m) in addTuple
        return properly formatted timeStruct
        will add day functionality later
    '''
    _year = timeStruct.tm_year
    _month = timeStruct.tm_mon
    _year=_year+addTuple[0]
    if addTuple[1] < 0 and _month < -addTuple[1]:
        _year -= 1
    elif addTuple[1] > 0 and month + addTuple[1] > 12:
        _year += 1
    _month = (_month + addTuple[1])%12
    return time.strptime("%i-%.2i-%.2i %.2i:%.2i:%.2i"%(_year,
                                                       _month,
                                                       timeStruct.tm_mday,
                                                       timeStruct.tm_hour,
                                                       timeStruct.tm_min,
                                                       timeStruct.tm_sec),
                            "%Y-%m-%d %H:%M:%S")
print " STARTING "
daysInMonth = [-1,31,28,31,30,31,30,31,31,30,31,30,31]
currentDate = time.gmtime()

currentYear = currentDate.tm_year
currentMonth = currentDate.tm_mon
currentDay = currentDate.tm_mday

DDMPattern = '(?!/_/_/USER_)'
DDMGroup = 'AnalysisOps'
DDMTimeStamps = [
                (genesis,nowish),
                (time.mktime(time.strptime('%i-01-01'%(currentYear-1),'%Y-%m-%d')), time.mktime(time.strptime('%i-12-31'%(currentYear-1),'%Y-%m-%d'))),
                (time.mktime(time.strptime('%i-01-01'%(currentYear),'%Y-%m-%d')), time.time()),
                (time.mktime(addTime(time.gmtime(),(0,-3))), time.time())
                ]
DDMLabels = ["SummaryAll", "SummaryLastYear", "SummaryThisYear","Last3Months"]

os.environ['MONITOR_PATTERN'] = DDMPattern
os.environ['MONITOR_GROUP'] = DDMGroup

print DDMGroup, DDMPattern

os.system('./readJsonSnapshotPickle.py T2*')

os.system('./plotFromPickle.py T2* %s'%('${MONITOR_DB}/monitorCache${MONITOR_GROUP}.pkl'))

for i in range(len(DDMTimeStamps)):
    os.environ['MONITOR_PLOTTEXT'] = DDMLabels[i]
    timeStamp = DDMTimeStamps[i]
    os.system('./plotFromPickle.py T2* %i %i %s'%( timeStamp[0], timeStamp[1], '${MONITOR_DB}/monitorCache${MONITOR_GROUP}.pkl' ))
### DataOps


DataOpsGroup = 'DataOps'

os.environ['MONITOR_PATTERN'] = DDMPattern
os.environ['MONITOR_GROUP'] = DataOpsGroup
os.system('./readJsonSnapshotPickle.py T2*')

for i in range(len(DDMTimeStamps)):
    os.environ['MONITOR_PLOTTEXT'] = DDMLabels[i]
    timeStamp = DDMTimeStamps[i]
    os.system('./plotFromPickle.py T2* %i %i %s'%( timeStamp[0], timeStamp[1], '${MONITOR_DB}/monitorCache${MONITOR_GROUP}.pkl' ))



### ''' see our bee '''

CRBPattern = '/_/_/_AOD_|/_/_/RECO$'
CRBPatterns = ['/_/_/_AOD$', '/_/_/_AODSIM', '/_/_/_AOD_','/_/_/MINIAOD_','/_/_/REC0$']
CRBPatternLabels = ['AOD', 'AODSIM', 'AllAOD','MINIAOD','RECO']
CRBGroup = '_'
CRBTimeStamps = []
CRBLabels = ["CRBSummary12Months","CRBSummary6Months","CRBSummary3Months"]
for period in [12,6,3]:
    startTime = nowish - period*86400*30 # approximately 'period' number of months ago
    CRBTimeStamps.append((startTime,nowish))

os.environ['MONITOR_PATTERN'] = CRBPattern
os.environ['MONITOR_GROUP'] = CRBGroup
os.system('./readJsonSnapshotPickle.py T[12]*')

for i in range(len(CRBTimeStamps)):
    timeStamp = CRBTimeStamps[i]
    for j in range(len(CRBPatterns)):
        os.environ['MONITOR_PATTERN'] = CRBPatterns[j]
        os.environ['MONITOR_PLOTTEXT'] = CRBLabels[i]+'_'+CRBPatternLabels[j]
        os.system('./plotFromPickle.py T[12]* %i %i %s'%( timeStamp[0], timeStamp[1], '${MONITOR_DB}/monitorCacheAll.pkl' ))
