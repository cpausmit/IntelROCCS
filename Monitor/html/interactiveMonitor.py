#!/usr/bin/python

import os,sys,time,cgi,cgitb,pwd    
from getpass import getuser
cgitb.enable()

data = cgi.FieldStorage()
startyear = int(data.getvalue("startyear"))
startmonth = int(data.getvalue("startmonth"))
endyear = int(data.getvalue("endyear"))
endmonth = int(data.getvalue("endmonth"))

'''datasetPattern = sys.argv[3]
groupPattern = sys.argv[4]
sitePattern = sys.argv[5]'''
sitePattern = "T2_US_MIT"
groupPattern = "AnalysisOps"
datasetPattern = '/_/_/_AOD_'

lastday = [-1, 31,28,31,30,31,30,31,31,30,31,30,31]
start = int(time.mktime(time.strptime("%i-%.2i-01"%(startyear,startmonth),"%Y-%m-%d")))
end = int(time.mktime(time.strptime("%i-%.2i-%.2i"%(endyear,endmonth,lastday[endmonth]),"%Y-%m-%d")))

label = "interactive_"+str(start)+"_"+str(end)+"_"+datasetPattern+"_"+sitePattern
label = label.replace("/","")
label = "interactive"

monitorDB = '/local/IntelROCCSplots/'
os.environ['MIT_ROOT_STYLE'] = '/local/IntelROCCSplots/MitRootStyle.C'
os.environ['MONITOR_DB'] =  monitorDB # make this more robust later
os.environ['MONITOR_PATTERN'] = datasetPattern
os.environ['MONITOR_GROUP'] = groupPattern
os.environ['MONITOR_PLOTTEXT'] = "interactive"

print "Content-type: text/html  "
print

if groupPattern=="_":
    groupPattern = "All"

# with open('/local/IntelROCCSplots/test.txt','w') as f:
#     f.write("hello\n")

rc = os.system('/var/www/cgi-bin/webPlotFromPickle.py %s %i %i %s/monitorCache%s.pkl >& /dev/null'%( sitePattern, start, end, monitorDB, groupPattern ))

#start making html

plotString = label
plotDir = 'http://t3serv012.mit.edu/IntelROCCSplots/'
dateString = str(int(time.time()))
print '''
<br>
<img src="%sUsage_%s.png?%s" alt="DatasetSummary" width="400">
<img src="%sCRBUsage_%s.png?%s" alt="DatasetSummary" width="400">
<br>
<img src="%sCRB01_%s.png?%s" alt="DatasetSummary" width="400">
<img src="%sCRBTime_%s.png?%s" alt="DatasetSummary" width="400">

'''%(plotDir,plotString,dateString,plotDir,plotString,dateString,plotDir,plotString,dateString,plotDir,plotString,dateString,)
