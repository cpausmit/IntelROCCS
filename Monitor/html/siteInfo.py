#!/usr/bin/python

import os,sys,time,cgi,cgitb
from string import replace
cgitb.enable()

data = cgi.FieldStorage()
startdate = data.getvalue("startdate")
enddate = data.getvalue("enddate")

datasetPattern = '(?!/_/_/USER_)'
groupPattern = 'AnalysisOps'

start = int(time.mktime(time.strptime("%i-%.2i-01"%(startyear,startmonth),"%Y-%m-%d")))
end = int(time.mktime(time.strptime("%i-%.2i-%.2i"%(endyear,endmonth,lastday[endmonth]),"%Y-%m-%d")))

label = "siteinfo"+str(start)+"_"+str(end)+"_"+sitePattern
label = label.replace("/","")

monitorDB = '/local/IntelROCCSplots/'
os.environ['MIT_ROOT_STYLE'] = '/local/IntelROCCSplots/MitRootStyle.C'
os.environ['MONITOR_DB'] =  monitorDB 
os.environ['MONITOR_PATTERN'] = datasetPattern
os.environ['MONITOR_GROUP'] = groupPattern
os.environ['MONITOR_PLOTTEXT'] = label

print "Content-type: text/html  "
print



rc = os.system('/var/www/cgi-bin/webPlotFromPickle.py %s %i %i %s/monitorCache%s.pkl >& /dev/null'%( sitePattern, start, end, monitorDB, groupPattern ))

#start making html

plotString = label
plotDir = 'http://t3serv012.mit.edu/IntelROCCSplots/'
dateString = str(int(time.time()))
print '''
<br>
API URL: http://t3serv012.mit.edu/cgi-bin/interactiveMonitor.cgi?startyear=%i&startmonth=%i&endyear=%i&endmonth=%i&pattern=%s&group=%s&site=%s
'''%(startyear,startmonth,endyear,endmonth,rawDatasetPAttern,groupPattern,sitePattern)
print '''
<img src="%sUsage_%s.png?%s" alt="DatasetSummary" width="400">
<img src="%sCRBUsage_%s.png?%s" alt="DatasetSummary" width="400">
<br>
<img src="%sCRB01_%s.png?%s" alt="DatasetSummary" width="400">
<img src="%sCRBTime_%s.png?%s" alt="DatasetSummary" width="400">
'''%(plotDir,plotString,dateString,plotDir,plotString,dateString,plotDir,plotString,dateString,plotDir,plotString,dateString,)
