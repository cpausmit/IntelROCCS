#!/usr/bin/env python

import os, sys, re, subprocess, MySQLdb, time, json
from xml.etree import ElementTree

# This code is slow and inefficient. It has been 
# hobbled together using Monitor code that was intended
# for completely different use. Not deleting the 
# json files it creates will speed up subsequent
# runs. I will eventually fix it. --Sid

#===================================================================================================
#  H E L P E R S
#===================================================================================================

def findDatasetHistoryAll(start=-1,debug=False):
    if start==-1:
        start = int(time.time()) - 86400 # 24 hours ago if not specified
    getJsonFile("del",start)
    getJsonFile("xfer",start)

def getJsonFile(requestType,start,debug=False):
    if requestType=="del":
        fileName = "delRequests_%i.json"%(start)
        requestType="deleterequests"
    elif requestType=="xfer":
        fileName = "xferRequests_%i.json"%(start)
        requestType="transferrequests"
    else:
        sys.stderr.write("unknown request type: %s\n"%(requestType))
        # sys.exit(1)
    # make a reasonable file name
    # fileName = os.environ.get('MONITOR_DB') + '/datasets/' + fileName

    # test whether the file exists and it was just created
    if os.path.exists(fileName) and abs(os.path.getmtime(fileName) - time.time()) < 24*60*60 and not(os.stat(fileName).st_size==0):
        sys.stderr.write("getJsonFile(%s,%i): file already exists!\n"%(requestType,start))
        return
    else:        # check failed so need to go to the source
        if os.path.exists(fileName):
            os.remove(fileName) # in case the last download was corrupted and wget can't overwrite it
        cmd = 'wget --no-check-certificate -O ' + fileName + \
              ' https://cmsweb.cern.ch/phedex/datasvc/json/prod/%s?create_since=%i'%(requestType,int(start))
        print ' CMD: ' + cmd
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
            print line
        return

def parseRequestJsonSingle(fileName,start,isXfer,targetDatasetNames,datasetHistory):
    print "Parsing ",fileName
    # isXfer = True if xfer history, False if deletions
    with open(fileName) as dataFile:
        try:
            data = json.load(dataFile)
        except ValueError:
            # json is not loadable
            if isXfer:
                getJsonFile("xfer",start)
            else:
                getJsonFile("del",start)
    requests = data["phedex"]["request"]
    if isXfer:
        for request in requests:
            for node in request["destinations"]["node"]:
                for dataset in request["data"]["dbs"]["dataset"]: 
                    datasetName = dataset["name"]
                    # if not re.match(datasetPattern,datasetName):
                    if not datasetName in targetDatasetNames:
                        #not the one we want
                        continue
                    siteName = node["name"]
                    if not re.search(r'T2.*',siteName):
                        #not a tier 2
                        continue
                    try:
                        if node["decided_by"]["decision"]=="n":
                            # transfer was not approved
                            continue
                    except KeyError:
                        # missing decision info?
                        continue
                        # pass
                    try:
                        xferTime = node["decided_by"]["time_decided"]
                    except KeyError:
                        xferTime = request["time_create"]
                    if not datasetName in datasetHistory:
                        datasetHistory[datasetName]={}
                    if not siteName in datasetHistory[datasetName]:
                        datasetHistory[datasetName][siteName] = [[],[]]
                    datasetHistory[datasetName][siteName][0].append(xferTime)
    else:
        for request in requests:
            for node in request["nodes"]["node"]:
                for dataset in request["data"]["dbs"]["dataset"]:
                    datasetName = dataset["name"]
                    if not datasetName in targetDatasetNames:
                        #not the one we want
                        continue
                    siteName = node["name"]
                    if not re.search(r'T2.*',siteName):
                        #not a tier 2
                        continue
                    try:
                        if node["decided_by"]["decision"]=="n":
                            # transfer was not approved
                            continue
                    except KeyError:
                        # missing decision info?
                        continue
                        # pass
                    try:
                        delTime = node["decided_by"]["time_decided"]
                    except KeyError:
                        delTime = request["time_create"]
                    if not datasetName in datasetHistory:
                        datasetHistory[datasetName]={}
                    if not siteName in datasetHistory[datasetName]:
                        datasetHistory[datasetName][siteName] = [[],[]]
                    datasetHistory[datasetName][siteName][1].append(delTime)


def cleanHistories(xfers,dels,start,end):
    # used to clean up messy histories
    # print "history: ", xfers,dels
    i=0   
    if (len(dels)==0 or dels[0] < start) and (len(xfers)==0 or xfers[0] > end):
        return [],[]
    elif len(dels)==0 and xfers[0] < end:
        return [xfers[0]],[end]
    elif len(xfers)==0 and dels[0] > start:
        return [start],[dels[0]]

    if xfers[0] > dels[0]:
        xfers.insert(0,start)

    while True:
        # print xfers,dels
        nx=len(xfers)
        nd=len(dels)
        if i+1==nx:
            return xfers,dels[:nx]
        elif i+1==nd:
            if xfers[i+1] > dels[i]:
                return xfers[:nd+1],dels
            else:
                return xfers[:nd],dels
        if xfers[i+1] < dels[i+1]:
            xfers.pop(i+1)
        elif xfers[i+1] > dels[i+1]:
            dels.pop(i+1)
        else:
            i+=1
    return xfers,dels

def findDatasetCreationTimeSingle(dataset,debug=0):
    cmd = './das_client.py --format=plain --limit=0 --query="dataset=' + dataset + ' | grep dataset.creation_time " '
    # print cmd
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        try:
            cTime = time.mktime(time.strptime(line,'%Y-%m-%d %H:%M:%S\n'))
        except ValueError:
            # bad response; assume it was always there
            print line
            return 0
    return cTime

if len(sys.argv) < 2:
    sys.stderr.write("Usage: %s <dataset name 1> [<dataset name 2> ... ]\n"%(sys.argv[0]))
    sys.exit(1)

datasetNames = sys.argv[1:]
start = 1378008000 # beginning of phedex xfer/del histories
print "Finding dataset transfer and deletion histories. This will take a few minutes..."
findDatasetHistoryAll(start)
datasetMovement={}
parseRequestJsonSingle('delRequests_%i.json'%(int(start)),start,False,datasetNames,datasetMovement)
parseRequestJsonSingle('xferRequests_%i.json'%(int(start)),start,True,datasetNames,datasetMovement)
print "Done finding dataset histories!"
for datasetName in datasetNames:
    print "Analyzing %s"%(datasetName)
    if datasetName not in datasetMovement:
        print "Warning: %s not found in Phedex history"%(datasetName)
        continue
    cTime = findDatasetCreationTimeSingle(datasetName)
    for siteName in datasetMovement[datasetName]:
        xfers = datasetMovement[datasetName][siteName][0]
        dels =  datasetMovement[datasetName][siteName][1]
        end = time.time()
        xfers,dels = cleanHistories(xfers,dels,cTime,end)
        for x,d in zip(xfers,dels):
            print "On %s from %i to %i"%(siteName,x,d)