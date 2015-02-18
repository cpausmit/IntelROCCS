#!/usr/bin/python

'''==============================================================================
    This script reads information from the pickle caches and directly
    makes plots
   =============================================================================='''

import os, sys
import re, glob, subprocess, time, json, pprint, MySQLdb
import datasetProperties
from findDatasetHistoryAll import *
import findDatasetProperties as fDP
import cPickle as pickle
import ROOT
from Dataset import *

genesis=1378008000
nowish = time.time()

# get the dataset pattern to consider (careful the pattern will be translated, better implementation
# should be done at some point)
try:
    detoxDB = os.environ['DETOX_DB']
    monitorDB = os.environ['MONITOR_DB']
    datasetPattern = os.environ['MONITOR_PATTERN']
    groupPattern = os.environ['MONITOR_GROUP']
except KeyError:
    sys.stderr.write('\n ERROR - a environment variable is not defined\n\n')
    sys.exit(2)
datasetPattern = datasetPattern.replace("_",".*")
groupPattern = groupPattern.replace("_",".*")

'''==============================================================================
      H E L P E R S
   =============================================================================='''

def addData(nAllAccessed,nAccessed,debug=0):
    # adding a hash array (nAccessed) to the mother of all hash arrays (nAllAccessed)

    # loop through the hash array
    for key in nAccessed:
        # add the entries to our all access hash array
        if key in nAllAccessed:
            nAllAccessed[key] += nAccessed[key]
        else:
            nAllAccessed[key] = nAccessed[key]

    # return the updated all hash array
    return nAllAccessed


def addSites(nSites,nAccessed,debug=0):
    # adding up the number of sites for each dataset

    # loop through the hash array
    for key in nAccessed:
        # add the entries to our all access hash array

        if key in nSites:
            nSites[key] += 1
        else:
            nSites[key] = 1
    # return the updated all hash array
    return nSites

def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    # this is the text including the size units, that need to be converted
    sizeGb  = float(sizeTxt[0:-2])
    units   = sizeTxt[-2:]
    # decide what to do for the given unit
    if   units == 'MB':
        sizeGb = sizeGb/1000.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1000.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(1)

    # return the size in GB as a float
    return sizeGb

def calculateAverageNumberOfSites(sitePattern,datasetSet,fullStart,end):
    # calculate the average number of replicas (sites) for a dataset in a given time interval

    # print ' Relevant time interval: %s %s  --> %d'%(time.strftime("%Y-%m-%d",time.gmtime(fullStart))\
    #     ,time.strftime("%Y-%m-%d",time.gmtime(end)),end-fullStart)
    print ' Relevant time interval: %s %s  --> %d'%(fullStart,end,end-fullStart)

    # convert it into floats and take care of possible rounding issues
    fullInterval = end - fullStart
    predictedDatasetsOnSites={}
    for datasetName in datasetSet:
        predictedDatasetsOnSites[datasetName]=set([])
    nSites = {}
    timeOnSites = {} #timeOnSites[dataset][site] = time

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for datasetName,datasetObject in datasetSet.iteritems():

        if not re.match(datasetPattern,datasetName):
            continue
            
        # don't penalize a dataset for not existing
        cTime = datasetObject.cTime
        start = max(fullStart,cTime)

        interval = end - start
        if not datasetName in nSites:
            nSites[datasetName] = 0
            timeOnSites[datasetName] = {}
        for siteName,movement in datasetObject.movement.iteritems():
            if not re.match(sitePattern,siteName):                      # only requested sites
                continue
            if not siteName in timeOnSites[datasetName]:
                timeOnSites[datasetName][siteName] = 0

            xfers = movement[0]
            dels = movement[1]
            lenXfer = len(xfers)
            lenDel  = len(dels)
            if lenDel == lenXfer - 1:
                dels.append(nowish)
            # find this site's fraction for nSites
            if not datasetName in nSites:
                nSites[datasetName] = 0

            siteSum = 0
            i       = 0
            while i < lenXfer:
                try:
                    tXfer = xfers[i]
                    tDel  = dels[i]
                except IndexError:
                    break
                i = i + 1                   # iterate before all continue statements
                # four ways to be in interval                 
                # (though this prevents you from having the same
                #  start and end date)
                if tXfer <= start <= end <= tDel:
                    siteSum += 1                                 # should happen at most once (?)
                elif tXfer <= start < tDel < end:
                    siteSum += float(tDel - start)/float(interval)
                elif start < tXfer < end <= tDel:
                    siteSum += float(end - tXfer)/float(interval)
                elif start < tXfer < tDel <= end:
                    siteSum += float(tDel - tXfer)/float(interval)
                else:                                            # have ensured tXfer > tDel
                    continue
            timeOnSites[datasetName][siteName] += siteSum
            nSites[datasetName] += siteSum

    n     = 0
    nSkip =  0
    Sum   = float(0)
    for datasetName in nSites:
        if nSites[datasetName] == 0:                            # dataset not on sites in interval
            nSkip += 1
            continue
        Sum += nSites[datasetName]
        n   += 1

    return nSites,timeOnSites

'''==============================================================================
      M A I N
   =============================================================================='''

debug = 0
sizeAnalysis = True
addNotAccessedDatasets = True

usage  = "\n"
usage += " plottingWrapper.py  <sitePattern> <startDate> <endDate> <pickleJar>\n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   startDate  - epoch time of starting date\n"
usage += "   endDate    - epoch time of ending date\n"
usage += "   pickleJar  - *.pkl file containing the relevant aggregated data\n"


# decode command line parameters
if len(sys.argv)==5:
    sitePattern = str(sys.argv[1])
    start = max(genesis,int(sys.argv[2]))
    end = min(nowish,int(sys.argv[3]))
    jarFileName = str(sys.argv[4])
else:
    sys.stderr.write(' ERROR - wrong number of arguments\n')
    sys.stderr.write(usage)
    sys.exit(2)

interval = float(end - start)/float(86400*30) # time interval in months, used for plotting
sitePattern=re.sub("\*",".*",sitePattern) # to deal with stuff like T2* --> T2.*
print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Dataset pattern:      %s"%(datasetPattern)
print " Group pattern:      %s"%(groupPattern)
print " Site pattern: %s"%(sitePattern)

with open(jarFileName,"rb") as pickleJar:
    pickleDict = pickle.load(pickleJar)
datasetSet = pickleDict["datasetSet"]
nSiteAccess = pickleDict["nSiteAccess"]

print "Computing average number of sites"
nAverageSites,timeOnSites = calculateAverageNumberOfSites(sitePattern,datasetSet,start,end)

# last step: produce plots!

stylePath = os.environ.get("MIT_ROOT_STYLE")
print stylePath
rc = ROOT.gROOT.LoadMacro(stylePath) # figure out using so later
if rc:
    print "Warning, MitRootStyle could not be loaded from %s"%(stylePath)
else:
    ROOT.MitRootStyle.Init()

'''==============================================================================
                                 our usage plots
   =============================================================================='''
cUsage = ROOT.TCanvas("c1","c1",800,600)
maxBin = 8
nBins = 60
delta = float(maxBin)/(2*(nBins-1)) # so that bins are centered
hUsage = ROOT.TH1F("dataUsage","Data Usage",nBins,-delta,maxBin+delta)
kBlack = 1
if not rc:
    ROOT.MitRootStyle.InitHist(hUsage,"","",kBlack) 
titles = "; Accesses/month; Fraction of total data volume"
hUsage.SetTitle(titles)
nEntries = 0
totalSize = 0
for datasetName,datasetObject in datasetSet.iteritems():
    if not re.match(datasetPattern,datasetName):
        # print "did not match pattern"
        continue
    if datasetObject.nFiles==0:
        # what
        continue
    nSitesAv = nAverageSites[datasetName]
    if nSitesAv == 0:
      # it was nowhere
      continue
    nEntries += 1
    nAccess = 0
    for siteName in datasetObject.nAccesses:
        if not re.match(sitePattern,siteName): 
            # maybe we should get rid of this functionality to speed things up
            continue
        for utime,n in datasetObject.nAccesses[siteName].iteritems():
            if utime >= start and utime <= end:
                nAccess += n
    value = float(nAccess)/float(datasetObject.nFiles*nSitesAv*interval)
    weight = float(nSitesAv) * float(datasetObject.sizeGB)/1000.
    totalSize += weight
    hUsage.Fill(min(maxBin,value),weight)
#totalSize = hUsage.Integral()
print "Found %i datasets, corresponding to an average volume of %3f PB"%(nEntries,float(totalSize)/1000.)
hUsage.Scale(1./float(totalSize))
maxy = hUsage.GetMaximum()
hUsage.SetMaximum(maxy*10.)
ROOT.gPad.SetLogy(1) # big zero bins
cUsage.cd()
try:
    histColor = os.environ['MONITOR_COLOR']
    hUsage.SetLineColor(histColor)
except KeyError:
    pass
hUsage.Draw("hist")
ROOT.MitRootStyle.OverlayFrame()
ROOT.MitRootStyle.AddText("Overflow in last bin. nAccess=0 in first bin.")
integralTexts = ["Period: [%s, %s]\n"%( strftime("%Y-%m-%d",gmtime(start)) , strftime("%Y-%m-%d",gmtime(end)) )]
integralTexts.append( "Average data managed: %.3f PB\n"%(totalSize/1000.) )
integralTexts.append( "Mean: %.3f accesses/month\n"%(hUsage.GetMean()) )
positions = [0.8, 0.75, 0.7]
plotTText = [None,None,None]
for i in range(3):
  plotTText[i] = ROOT.TText(.5,positions[i],integralTexts[i])
  plotTText[i].SetTextSize(0.04)
  plotTText[i].SetTextColor(2)
  plotTText[i].Draw()
if groupPattern == ".*":
    groupPattern = "All"
try:
    cUsage.SaveAs(monitorDB+"/Usage_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
except KeyError:
    cUsage.SaveAs(monitorDB+"/Usage_%s_%i_%i.png"%(groupPattern,start,end))


'''==============================================================================
                                 CRB usage plots
   =============================================================================='''

ROOT.gPad.SetLogy(0) 
hCRB = ROOT.TH1F("CRBUsage","Data Usage",17,-1.5,15.5)
hZeroOne = ROOT.TH1F("CRBZeroOne","Zero and One Bin",100,0,1.);
hTime = ROOT.TH1F("time","time",100,0,1.);
if not rc:
    ROOT.MitRootStyle.InitHist(hCRB,"","",kBlack) 
    ROOT.MitRootStyle.InitHist(hZeroOne,"","",kBlack) 
    ROOT.MitRootStyle.InitHist(hTime,"","",kBlack) 
titles = "; <n_{accesses}>; Prorated data volume [TB]"
hCRB.SetTitle(titles)
hZeroOne.SetTitle(titles)
titles = "; Prorated Time Fraction; Data volume [TB]"
hTime.SetTitle(titles)
cCRB = ROOT.TCanvas("c2","c2",800,600)
cZeroOne = ROOT.TCanvas("c3","c3",800,600)
cTime = ROOT.TCanvas("c4","c4",800,600)
for datasetName,datasetObject in datasetSet.iteritems():
    if not re.match(datasetPattern,datasetName):
        continue
    if datasetObject.nFiles==0:
        # what
        continue
    sizeGB = datasetObject.sizeGB
    for siteName in datasetObject.movement:
        if not re.match(sitePattern,siteName): 
            # maybe we should get rid of this functionality to speed things up
            continue
        timeOnSite = timeOnSites[datasetName][siteName]
        value = 0
        if siteName in datasetObject.nAccesses:
            for utime,n in datasetObject.nAccesses[siteName].iteritems():
                if utime <= end and utime >= start:
                    value += min(float(n)/datasetObject.nFiles , 14.5)
            fillValue = max(1,value)
            # if value < 1:
            #     print value,fillValue
        if value == 0:
            if datasetObject.cTime > start:
                fillValue = 0
            else:
                fillValue = -1
        weight = float(sizeGB * timeOnSite)/1000.
        hCRB.Fill(fillValue,weight)
        if (fillValue == 0) or (fillValue == 1):
            hZeroOne.Fill(value,weight)
        if timeOnSite:
            hTime.Fill(timeOnSite,sizeGB/1000.)
try:
    histColor = os.environ['MONITOR_COLOR']
    hCRB.SetLineColor(histColor)
    hTime.SetLineColor(histColor)
    hZeroOne.SetLineColor(histColor)
except KeyError:
    pass

xaxis = hCRB.GetXaxis()
xaxis.SetBinLabel(1,"0 old")
xaxis.SetBinLabel(2,"0 new")
xaxis.SetBinLabel(3,"< 1")
xaxis.SetBinLabel(4,"2")
xaxis.SetBinLabel(5,"3")
xaxis.SetBinLabel(6,"4")
xaxis.SetBinLabel(7,"5")
xaxis.SetBinLabel(8,"6")
xaxis.SetBinLabel(9,"7")
xaxis.SetBinLabel(10,"8")
xaxis.SetBinLabel(11,"9")
xaxis.SetBinLabel(12,"10")
xaxis.SetBinLabel(13,"11")
xaxis.SetBinLabel(14,"12")
xaxis.SetBinLabel(15,"13")
xaxis.SetBinLabel(16,"14")
xaxis.SetBinLabel(17,">14")
cCRB.cd()
hCRB.Draw("hist")
ROOT.MitRootStyle.OverlayFrame()
ROOT.MitRootStyle.AddText("Overflow in last bin.")
totalSize = hCRB.Integral()
integralTexts = ["Period: [%s, %s]\n"%( strftime("%Y-%m-%d",gmtime(start)) , strftime("%Y-%m-%d",gmtime(end)) )]
integralTexts.append( "Average data on disk: %.3f PB\n"%(totalSize/1000.) )
positions = [0.8,0.75]
plotTText = [None,None]
for i in range(2):
  plotTText[i] = ROOT.TText(.5,positions[i],integralTexts[i])
  plotTText[i].SetTextSize(0.04)
  plotTText[i].SetTextColor(2)
  plotTText[i].Draw()
if groupPattern == ".*":
    groupPattern = "All"
try:
    cCRB.SaveAs(monitorDB+"/CRBUsage_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
except KeyError:
    cCRB.SaveAs(monitorDB+"/CRBUsage_%s_%i_%i.png"%(groupPattern,start,end))
    
cZeroOne.cd()
hZeroOne.Draw("hist")
ROOT.MitRootStyle.OverlayFrame()
plotTText = [None,None]
for i in range(1):
  plotTText[i] = ROOT.TText(.5,positions[i],integralTexts[i])
  plotTText[i].SetTextSize(0.04)
  plotTText[i].SetTextColor(2)
  plotTText[i].Draw()
try:
    cZeroOne.SaveAs(monitorDB+"/CRB01_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
except KeyError:
    cZeroOne.SaveAs(monitorDB+"/CRB01_%s_%i_%i.png"%(groupPattern,start,end))
    
cTime.cd()
hTime.Draw("hist")
ROOT.MitRootStyle.OverlayFrame()
plotTText = [None,None]
for i in range(1):
  plotTText[i] = ROOT.TText(.5,positions[i],integralTexts[i])
  plotTText[i].SetTextSize(0.04)
  plotTText[i].SetTextColor(2)
  plotTText[i].Draw()
try:
    cTime.SaveAs(monitorDB+"/CRBTime_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
except KeyError:
    cTime.SaveAs(monitorDB+"/CRBTime_%s_%i_%i.png"%(groupPattern,start,end))

sys.exit(0)
