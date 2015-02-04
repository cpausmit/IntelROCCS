#!/usr/bin/env python

import os, sys, re, glob, subprocess, time, json, pprint, MySQLdb

def epochTime(s):
  #returns epoch time given string format in file
  return time.mktime(time.strptime(s,'%Y-%m-%dT%H:%M:%S'))

def processFile(fileName,debug=0):
  # process the contents of a simple file into a dict of dicts:
  # waitTimes[dataset][site] is the mean wait time of jobs on 'site' looking for 'dataset'
  # print fileName
  data_file = open(fileName,'r')
  waitTimes={}
  maxTuple = (0,None,None)
  nJobs={}
  returnList=[]
  for line in data_file.readlines():
    vals=line.split()
    jID = int(vals[0])
    datasetName = vals[1]
    siteName = vals[2]
    if not re.search(r'/.*/.*/.*AOD.*',datasetName):
      #not AOD or AODSIM
      continue
    if not re.search(r'T2.*',siteName):
      # not on a tier 2
      continue
    start = int(vals[3])
    submitted = int(vals[4])
    wait = max(start - submitted,0)
    if wait > 10**7:
      sys.stderr.write("%s\n"%(str(vals)))
      continue
    if maxTuple[0] < wait:
      maxTuple = (wait,datasetName,siteName)
    returnList.append(wait)
    # maxWait = max(maxWait,wait)
    # if wait > 86400:
    #   print "Warning, job %i waited for %i = %i - %i\n\t%s\n\t%s\n"%(jID, wait, start, submitted, datasetName, siteName)
    if not datasetName in waitTimes:
      waitTimes[datasetName]={}
      nJobs[datasetName]={}
    if siteName in waitTimes[datasetName]:
      wTmp = waitTimes[datasetName][siteName] 
      n = nJobs[datasetName][siteName]
      waitTimes[datasetName][siteName] = float(n*wTmp + wait)/(n+1)
      nJobs[datasetName][siteName]+=1
    else:
      waitTimes[datasetName][siteName] = wait
      nJobs[datasetName][siteName] = 1
  return waitTimes, returnList, maxTuple

def processFileJson(fileName,debug=0):
  # process the contents of a simple json file into a dict of dicts:
  # waitTimes[dataset][site] is the mean wait time of jobs on 'site' looking for 'dataset'
  # also returns list of lists with structure:
  # [jobID, datasetName,SiteName, started, submitted, finished, infoTimeStamp]
  # print fileName
  with open(fileName) as data_file:
    data = json.load(data_file)
  maxTuple = (0,None,None)
  waitTimes={}
  nJobs={}
  jobs = data["jobs"]
  returnList = []
  for job in jobs:
    datasetName = job["InputCollection"]
    if not re.search(r'/.*/.*/.*AOD.*',datasetName):
      #not AOD or AODSIM
      continue
    siteName = job["SiteName"]
    if not re.search(r'T2.*',siteName):
      # not on a tier 2
      continue
    start = epochTime(job["StartedRunningTimeStamp"])
    finish = epochTime(job["FinishedTimeStamp"])
    submitted = epochTime(job["SubmittedTimeStamp"])
    latestInfo = epochTime(job["DboardLatestInfoTimeStamp"])
    jID = job["JobId"]
    returnList.append([jID, datasetName, siteName, start, submitted, finish, latestInfo])
    wait = max(start - submitted,0)
    if wait > 10**7:
      print returnList[-1]
    if maxTuple[0] < wait:
      maxTuple = (wait,datasetName,siteName)
    # if wait > 86400:
      # print "Warning, job %i waited for %i = %i - %i\n\t%s\n\t%s"%(jID, wait, start, submitted, datasetName, siteName)
    if not datasetName in waitTimes:
      waitTimes[datasetName]={}
      nJobs[datasetName]={}
    if siteName in waitTimes[datasetName]:
      wTmp = waitTimes[datasetName][siteName] 
      n = nJobs[datasetName][siteName]
      waitTimes[datasetName][siteName] = float(n*wTmp + wait)/(n+1)
      nJobs[datasetName][siteName]+=1
    else:
      waitTimes[datasetName][siteName] = wait
      nJobs[datasetName][siteName] = 1

  return waitTimes, returnList, maxTuple

'''
#### MAIN ####
'''

timeStart=None
timeEnd=None

if len(sys.argv) == 1:
    print "Using default times"
    print "If desired, enter start end epoch times"
    timeStart = list(time.gmtime(time.time()-600))[:6] 
    timeEnd = list(time.gmtime())[:6] 
else:
    timeStart = time.gmtime(int(sys.argv[1]))
    timeEnd = time.gmtime(int(sys.argv[2]))

# timeStart = [2014, 10, 13, 00, 00, 00] # testing
# timeEnd = [2014, 10, 13, 00, 59, 00]
timeStartString = epochTime('%.2i-%.2i-%.2iT%.2i:%.2i:%.2i'%(
  timeStart[0],
  timeStart[1],
  timeStart[2],
  timeStart[3],
  timeStart[4],
  timeStart[5]))
timeEndString = epochTime('%.2i-%.2i-%.2iT%.2i:%.2i:%.2i'%(
  timeEnd[0],
  timeEnd[1],
  timeEnd[2],
  timeEnd[3],
  timeEnd[4],
  timeEnd[5]))

outFilePath = os.environ.get('MONITOR_DB') + "/waitTimes/%i_%i.txt"%(timeStartString,timeEndString)
logFilePath = os.environ.get('MONITOR_DB') + "/waitTimes/%i_%i.log"%(timeStartString,timeEndString)

pid = os.getpid()
jsonExists = os.path.isfile(outFilePath)
if jsonExists:
    waitTimes, compressedList, maxWait = processFile(outFilePath)
else:
    os.system("mkdir -p %s/waitTimes"%(os.environ.get("MONITOR_DB")))
    curlPath = 'http://dashb-cms-datapop.cern.ch/dashboard/request.py/cms-wait-time-api?start=%.2i-%.2i-%.2i@20%.2i:%.2i:%.2i&end=%.2i-%.2i-%.2i@20%.2i:%.2i:%.2i'%(timeStart[0],
      timeStart[1],
      timeStart[2],
      timeStart[3],
      timeStart[4],
      timeStart[5],
      timeEnd[0],
      timeEnd[1],
      timeEnd[2],
      timeEnd[3],
      timeEnd[4],
      timeEnd[5])
    curlPath = curlPath.replace("@","%")
    # print "curl -H 'Accept: application/json' '%s' "%(curlPath)
    os.system("mkdir -p tmp")
    print "curl -H 'Accept: application/json' '%s' > tmp/%i.json"%(curlPath,pid)
    os.system("curl -H 'Accept: application/json' '%s' > tmp/%i.json"%(curlPath,pid))
    # os.system("curl -H 'Accept: application/json' '%s'| python -mjson.tool > tmp/%i.json"%(curlPath,pid))
    waitTimes, compressedList, maxWait = processFileJson("tmp/%i.json"%(pid))
    # os.system("rm tmp/%i.json"%(pid))

## waitTimes are available

formatString = ["%i ", "%s ", "%s ", "%i ", "%i ", "%i ", "%i "]

# sys.stdout.write(str(maxWait[0])+"\n")
# print compressedList
if not jsonExists:
  outFile = open(outFilePath,'w')
  for l in compressedList:
    # sys.stdout.write("%.3f\n"%(max(l[3]-l[4],0)))
    for i in xrange(len(formatString)):
      outFile.write(formatString[i]%(l[i]))
    outFile.write("\n")
  outFile.close()
else:
  pass
  # for l in compressedList:
  #   sys.stdout.write("%.3f\n"%(l))

logFile = open(logFilePath,'w')
nTotal = 0
totalWait = 0
for d in waitTimes:
  logFile.write(d+"\n")
  for s in waitTimes[d]:
    if s=="unknown":
      continue
    if waitTimes[d][s]:
      logFile.write("\t%20s"%(s)+"\t%.3f\n"%(waitTimes[d][s]))
    totalWait += waitTimes[d][s]
    nTotal += 1
sys.stdout.write("%.3f\n"%(float(totalWait)/nTotal))
logFile.close()
