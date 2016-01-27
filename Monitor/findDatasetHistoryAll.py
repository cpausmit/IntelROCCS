#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# common tools for requesting, parsing, and sanitizing Phedex histories
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb, time, json
from Dataset import *
import glob

# setup definitions
if not os.environ.get('MONITOR_DB'):
    print '\n ERROR - MONITOR environment not defined: source setupMonitor.sh\n'
    sys.exit(0)

# genesis=int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
genesis=1378008000

#===================================================================================================
#  H E L P E R S
#===================================================================================================

def findDatasetHistoryAll(start=-1,debug=False,sites=None):
    if start==-1:
        start = int(time.time()) - 86400 # 24 hours ago if not specified
    getJsonFile("del",start,debug,sites)
    getJsonFile("xfer",start,debug,sites)

def getJsonFile(rawRequestType,start,debug=False,sites=None):
  certPath = os.environ['USERCERT']
  keyPath = os.environ['USERKEY']
  if sites==None:
    sites = ['T2_BE_IIHE','T2_ES_IFCA','T2_IT_Pisa','T2_RU_PNPI','T2_US_Caltech','T2_BE_UCL','T2_FI_HIP',
        'T2_IT_Rome','T2_RU_RRC_KI','T2_US_Florida','T2_BR_SPRACE','T2_FR_CCIN2P3','T2_KR_KNU','T2_RU_SINP',
        'T2_US_MIT','T2_BR_UERJ','T2_FR_GRIF_IRFU','T2_PK_NCP','T2_TH_CUNSTDA','T2_US_Nebraska','T2_CH_CERN',
        'T2_FR_GRIF_LLR','T2_PL_Swierk','T2_TR_METU','T2_US_Purdue','T2_CH_CSCS','T2_FR_IPHC','T2_PL_Warsaw',
        'T2_TW_Taiwan','T2_US_UCSD','T2_CN_Beijing','T2_GR_Ioannina','T2_PT_NCG_Lisbon','T2_UA_KIPT','T2_US_Wisconsin',
        'T2_DE_DESY','T2_HU_Budapest','T2_RU_IHEP','T2_UK_London_Brunel','T2_DE_RWTH','T2_IN_TIFR','T2_RU_INR','T2_UK_London_IC',
        'T2_EE_Estonia','T2_IT_Bari','T2_RU_ITEP','T2_UK_SGrid_Bristol','T2_AT_Vienna','T2_ES_CIEMAT','T2_IT_Legnaro',
        'T2_RU_JINR','T2_UK_SGrid_RALPP']
    sites += ["T1_UK_RAL_Disk", "T1_US_FNAL_Disk", "T1_IT_CNAF_Disk", "T1_DE_KIT_Disk", "T1_RU_JINR_Disk", "T1_FR_CCIN2P3_Disk", "T1_ES_PIC_Disk"]
  for site in sites:
      fileName=""
      if rawRequestType=="del":
          fileName = "delRequests_%i_%s.json"%(start,site)
          requestType="deleterequests"
      elif rawRequestType=="xfer":
          fileName = "xferRequests_%i_%s.json"%(start,site)
          requestType="transferrequests"
      else:
          sys.stderr.write("unknown request type: %s\n"%(rawRequestType))
          # sys.exit(1)
      # make a reasonable file name
      fileName = os.environ.get('MONITOR_DB') + '/datasets/' + fileName

      # test whether the file exists and it was just created
      if os.path.exists(fileName) and abs(os.path.getmtime(fileName) - time.time()) < 24*60*60 and not(os.stat(fileName).st_size==0):
          sys.stderr.write("getJsonFile(%s,%i): file already exists!\n"%(requestType,start))
          continue
      else:        # check failed so need to go to the source
          if os.path.exists(fileName):
              os.remove(fileName) # in case the last download was corrupted and wget can't overwrite it
          urlpath = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/%s?create_since=%i&node=%s'%(requestType,int(start),site)

          flags = '--ca-directory=/home/snarayan/certs --certificate=%s --private-key=%s'%(certPath,keyPath)
          cmd = "wget -O %s %s '%s'"%(fileName,flags,urlpath)
      #    cmd = 'wget --no-check-certificate -O ' + fileName + \
      #          ' https://cmsweb.cern.ch/phedex/datasvc/json/prod/%s?create_since=%i&node=%s'%(requestType,int(start),site)
          print ' CMD: ' + cmd
          for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
              print line
  return

def getFileTime(s):
  return int(s.split('/')[-1].split('_')[1].split('.')[0])

def parseRequestJson(start,end,isXfer,datasetPattern,datasetSet):
  if isXfer:
    allJsons = glob.glob(os.environ.get('MONITOR_DB')+'/datasets/xferRequests_*.json')
  else:
    allJsons = glob.glob(os.environ.get('MONITOR_DB')+'/datasets/delRequests_*.json')
  goodJsons = []
  for fileName in allJsons:
    timestamp = getFileTime(fileName)
    if timestamp<end and timestamp>start:
      goodJsons.append(fileName)
  for fileName in goodJsons:
      print "Parsing ",fileName
      # isXfer = True if xfer history, False if deletions
      with open(fileName) as dataFile:
          try:
              data = json.load(dataFile)
          except ValueError:
            print "Warning, JSON was corrupted - reloading"
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
                      try:
                          datasetObject = datasetSet[datasetName]
                      except KeyError:
                          # not one of the datasets we're considering
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
                      datasetObject.addTransfer(siteName,xferTime)
      else:
          for request in requests:
              for node in request["nodes"]["node"]:
                  for dataset in request["data"]["dbs"]["dataset"]:
                      datasetName = dataset["name"]
                      try:
                          datasetObject = datasetSet[datasetName]
                      except KeyError:
                          # not one of the datasets we're considering
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
                      datasetObject.addDeletion(siteName,delTime)

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
            return (xfers,dels[:nx])
        elif i+1==nd:
            if xfers[i+1] > dels[i]:
                return (xfers[:nd+1],dels)
            else:
                return (xfers[:nd],dels)
        if xfers[i+1] < dels[i+1]:
            xfers.pop(i+1)
        elif xfers[i+1] > dels[i+1]:
            dels.pop(i+1)
        else:
            i+=1
    return (xfers,dels)
