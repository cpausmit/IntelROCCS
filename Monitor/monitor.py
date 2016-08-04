#!/usr/bin/python
'''
Wrapper script that reads the binaries and makes the standard plots
'''

import os
import sys
import time
import plotFromPickle
import cPickle as pickle
genesis=1378008000
nowish = time.time()

# remove old log files
logPeriod = int((nowish/1000000)%10)
logBlock = int((nowish/10000000))
monitordb = os.environ['MONITOR_DB']
os.system('rm -f $MONITOR_DB/monitor-%i[^%i%i]*'%(logBlock,logPeriod,logPeriod-1))

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
    if _month==0:
      _month=12
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


''' CRB-style plots '''

CRBPattern = '/_/_/_AOD_|/_/_/RECO$'
CRBPatterns = ['/_/_/RECO$', '/_/_/_AOD$', '/_/_/_AODSIM', '/_/_/_AOD_','/_/_/MINIAOD_']
CRBPatternLabels = ['RECO', 'AOD', 'AODSIM', 'AllAOD','MINIAOD']
#CRBPattern = '/_/_/_AOD_'
#CRBPatterns = ['/_/_/_AOD$', '/_/_/_AODSIM', '/_/_/_AOD_','/_/_/MINIAOD_']
#CRBPatternLabels = ['AOD', 'AODSIM', 'AllAOD','MINIAOD']
CRBGroup = '_'
CRBTimeStamps = []
CRBLabels = ["CRBSummary12Months","CRBSummary6Months","CRBSummary3Months"]
os.environ['MONITOR_PATTERN'] = CRBPattern
os.environ['MONITOR_GROUP'] = CRBGroup
os.system('./readJsonSnapshotPickle.py T[12]*')

def stampToString(stamp):
    return time.strftime('%Y-%m-%d',time.gmtime(stamp))

with open(monitordb+'/monitorCacheAll.pkl','rb') as cachefile:
    crbcache = pickle.load(cachefile)

def makeCRBPlots(endStamp,crbLabel):
    CRBTimeStamps = []
    for period in [12,6,3]:
        startTime = endStamp - period*86400*30 # approximately 'period' number of months ago
        CRBTimeStamps.append((startTime,endStamp))
    os.system('rm -f ${MONITOR_DB}/monitorCacheAll_T12_%s.root'%(crbLabel))
    os.system('rm -f ${MONITOR_DB}/monitorCacheAll_T2_%s.root'%(crbLabel))
    os.system('rm -f ${MONITOR_DB}/monitorCacheAll_T1_%s.root'%(crbLabel))
    for i in range(len(CRBTimeStamps)):
        timeStamp = CRBTimeStamps[i]
        for j in range(len(CRBPatterns)):
            print '\n=========================\n',stampToString(timeStamp[0]),'-',stampToString(timeStamp[1]),'\n========================='
            os.environ['MONITOR_PATTERN'] = CRBPatterns[j]
            os.environ['MONITOR_PLOTTEXT'] = CRBLabels[i]+'_'+CRBPatternLabels[j]+'_'+crbLabel
            print 'T1'; plotFromPickle.makeActualPlots('T1*',timeStamp[0],timeStamp[1],crbcache,'T1_'+crbLabel,'${MONITOR_DB}/monitorCacheAll_T1_%s.root'%(crbLabel)) 
            print 'T2'; plotFromPickle.makeActualPlots('T2*',timeStamp[0],timeStamp[1],crbcache,'T2_'+crbLabel,'${MONITOR_DB}/monitorCacheAll_T2_%s.root'%(crbLabel)) 
            print 'T[12]'; plotFromPickle.makeActualPlots('T[12]*',timeStamp[0],timeStamp[1],crbcache,'T12_'+crbLabel,'${MONITOR_DB}/monitorCacheAll_T12_%s.root'%(crbLabel)) 
    os.system('./generateXls.py T12_%s ${MONITOR_DB}/monitorCacheAll_T12_%s.root'%(crbLabel,crbLabel))
    os.system('./generateXls.py T2_%s ${MONITOR_DB}/monitorCacheAll_T2_%s.root'%(crbLabel,crbLabel))
    os.system('./generateXls.py T1_%s ${MONITOR_DB}/monitorCacheAll_T1_%s.root'%(crbLabel,crbLabel))

makeCRBPlots(nowish,'current')
iY=0
notDone=True
with open(os.environ['MONITOR_BASE']+'/html/xls.html','r') as inhtml:
    oldlines = list(inhtml)
while notDone:
    year = 2015+iY
    for month in [3,6,9,12]:
        endStamp = time.mktime(time.strptime('%i-%i-%i'%(year,month,daysInMonth[month]),'%Y-%m-%d'))
        if endStamp>nowish:
            notDone = False
            break
        crblabel = 'ending_%4i-%.2i'%(year,month)
        makeCRBPlots(endStamp,crblabel)
        newlines = []
        for line in oldlines:
            newlines.append(line)
            if '!--' in line:
                newlines.append('  <tr>  <td> Ending %4i-%.2i </td> <td> <a href="xls/T1_%s.xlsx">T1_%s.xlsx</a> </td> <td> <a href="xls/T2_%s.xlsx">T2_%s.xlsx</a> </td> <td> <a href="xls/T12_%s.xlsx">T12_%s.xlsx</a></td> </tr>'%(year,month,crblabel,crblabel,crblabel,crblabel,crblabel,crblabel))
        oldlines = newlines
    iY += 1
with open(os.environ['MONITOR_WEB']+'/xls.html','w') as outhtml:
    for line in oldlines:
        outhtml.write(line)
with open(os.environ['MONITOR_DB']+'/xls.html','w') as outhtml:
    for line in oldlines:
        outhtml.write(line)
system('cp %s/xls.html %s/xls.html'%(os.environ['MONITOR_WEB'],os.environ['MONITOR_DB']))

