#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# common tools for requesting, parsing, and sanitizing Phedex histories
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb, time, json
from xml.etree import ElementTree

# setup definitions
if not os.environ.get('MONITOR_DB'):
    print '\n ERROR - MONITOR environment not defined: source setupMonitor.sh\n'
    sys.exit(0)

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
    fileName = os.environ.get('MONITOR_DB') + '/datasets/' + fileName

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

def getAnalysisOpsDeletions(start,end,datasetPattern):
    delFileName = os.environ.get('MONITOR_DB') + '/datasets/delRequests_1378008000.json'
    # returns all datasets in the requests which match the pattern and are in AnalysisOps
    print "Parsing ",delFileName
    # isXfer = True if xfer history, False if deletions
    datasetSet=set([])
    with open(delFileName) as dataFile:
        try:
            data = json.load(dataFile)
        except ValueError:
            sys.exit(-1)
    requests = data["phedex"]["request"]
    for request in requests:
        for dataset in request["data"]["dbs"]["dataset"]:
            datasetName = dataset["name"]
            requestedBy = request["requested_by"]["name"]
            try:
                if re.match(datasetPattern,datasetName) and (requestedBy=="Maxim Goncharov" or requestedBy=="Christoph Paus"):
                # if re.match(datasetPattern,datasetName):
                    datasetSet.add(datasetName)
                    if re.match(r".*BUNN.*",datasetName):
                        print "added superbunny dataset %s because it was requested by %s"%(datasetName,requestedBy)
            except TypeError:
                print "weird"
                pass
    return datasetSet

def getAllDeletions(start,end,datasetPattern):
    delFileName = os.environ.get('MONITOR_DB') + '/datasets/delRequests_1378008000.json'
    # returns all datasets in the requests which match the pattern and are in AnalysisOps
    print "Parsing ",delFileName
    # isXfer = True if xfer history, False if deletions
    datasetSet=set([])
    with open(delFileName) as dataFile:
        try:
            data = json.load(dataFile)
        except ValueError:
            sys.exit(-1)
    requests = data["phedex"]["request"]
    for request in requests:
        for dataset in request["data"]["dbs"]["dataset"]:
            datasetName = dataset["name"]
            requestedBy = request["requested_by"]["name"]
            try:
                # if re.match(datasetPattern,datasetName) and (requestedBy=="Maxim Goncharov" or requestedBy=="Christoph Paus"):
                if re.match(datasetPattern,datasetName):
                    datasetSet.add(datasetName)
            except TypeError:
                print "weird"
                pass
    return datasetSet


def parseRequestJson(fileName,start,end,isXfer,datasetHistory,datasetPattern,datasetSet):
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
                    if not datasetName in datasetSet:
                        #not AOD or AODSIM or whatever
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
                        # continue
                        pass
                    try:
                        xferTime = node["decided_by"]["time_decided"]
                    except KeyError:
                        xferTime = request["time_create"]
                    if xferTime > end:
                        # too late
                        continue
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
                    # if not re.match(datasetPattern,datasetName):
                    if not datasetName in datasetSet:
                        #not AOD or AODSIM
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
                        # continue
                        pass
                    try:
                        delTime = node["decided_by"]["time_decided"]
                    except KeyError:
                        delTime = request["time_create"]
                    if delTime > end:
                        # too late
                        continue
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


#===================================================================================================
#  M A I N
#===================================================================================================

# findDatasetHistoryAll()
# sys.exit(0)
