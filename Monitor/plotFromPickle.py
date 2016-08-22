#!/usr/bin/python

'''==============================================================================
    This script reads information from the pickle caches and directly
    makes plots
   =============================================================================='''

import os, sys
import re, glob, subprocess, time
from findDatasetHistoryAll import *
import findDatasetProperties as fDP
import cPickle as pickle
import ROOT
from array import array
from Dataset import *
from operator import itemgetter

genesis=1378008000
nowish = time.time()
loadedStyle=False
rc=None
try:
    monitorDB = os.environ['MONITOR_DB']
except KeyError:
    sys.stderr.write('\n ERROR - a environment variable is not defined\n\n')
    sys.exit(2)

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

def calculateAverageNumberOfSites(sitePattern,datasetSet,fullStart,end,datasetPattern):
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
    timeOnSites = {} #timeOnSites[dataset][site] = timef

    # match the intervals from the phedex history to the requested time interval
    #===========================================================================
    for datasetName,datasetObject in datasetSet.iteritems():
        verb = (datasetName=='/GluGluZH_HToWW_M120_13TeV_powheg_pythia8/RunIIFall15MiniAODv1-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM')

        if not re.match(datasetPattern,datasetName):
            continue
            
        # don't penalize a dataset for not existing
        cTime = datasetObject.cTime
        #start = max(fullStart,cTime)
        start = fullStart

        interval = end - start

        if verb:
          print fullStart,end,start,cTime,interval
          
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
            if verb:
              print siteName
              print '\t',xfers
              print '\t',dels
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
            if verb:
              print '\t',siteSum
            if siteSum>0:
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


def makeActualPlots(sitePattern,start,end,jarFile,crbLabel='',rootFile='',makeSummaryPlots=False):
    if crbLabel!='' and rootFile=='':
        sys.stderr.write('ERROR [plotFromPickle.makeActualPlots]: If crbLabel is defined, rootFile must be defined')
        return
    groupPattern = os.environ['MONITOR_GROUP']
    datasetPattern = os.environ['MONITOR_PATTERN']
    datasetPattern = datasetPattern.replace("_",".*")
    groupPattern = groupPattern.replace("_",".*")
    interval = float(end - start)/float(86400*30) # time interval in months, used for plotting
    sitePattern=re.sub("\*",".*",sitePattern) # to deal with stuff like T2* --> T2.*

    print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
    print " Dataset pattern:      %s"%(datasetPattern)
    print " Group pattern:      %s"%(groupPattern)
    print " Site pattern: %s"%(sitePattern)

    pickleJar = None
    if type(jarFile)==type(''):
        pickleJar = open(jarFile,"rb")
        pickleDict = pickle.load(pickleJar)
    else:
        pickleDict = jarFile
    datasetSet = pickleDict["datasetSet"]
    nSiteAccess = pickleDict["nSiteAccess"]

    # last step: produce plots!

    global loadedStyle,rc
    if not loadedStyle:
        stylePath = os.environ.get("MIT_ROOT_STYLE")
        print stylePath
        rc = ROOT.gROOT.LoadMacro(stylePath) # figure out using so later
        if rc:
            print "Warning, MitRootStyle could not be loaded from %s"%(stylePath)
        else:
            ROOT.MitRootStyle.Init()
        loadedStyle=True

    print 'MAKING SUMMARY:',makeSummaryPlots
    if makeSummaryPlots:
        c11 = ROOT.TCanvas("c11","c11",800,800)
        nTiers=7
        hVolumeFrac = ROOT.TH1F("hVolumeFrac","hVolumeFrac",nTiers,-0.5,nTiers-.5)
        hUsageFrac = ROOT.TH1F("hUsageFrac","hUsageFrac",nTiers,-0.5,nTiers-.5)
        tiers = {'AODSIM':0, 'AOD':1, 'MINIAODSIM':2,'MINIAOD':3,'GEN-SIM-RAW':4,'GEN-SIM-RECO':5,'Other':6}
        for hist in [hUsageFrac,hVolumeFrac]:
            xaxis = hist.GetXaxis()
            for tier,nBin in tiers.iteritems():
                xaxis.SetBinLabel(nBin+1,tier)
        totalVolume=0
        totalUsage=0
        siteAccessDict = {}
        miniaodSizeNoRepl=0
        miniaodSizeRepl=0
        for datasetName,datasetObject in datasetSet.iteritems():
            tier = datasetName.split('/')[-1]
            datasetVolume = max(0,len(datasetObject.currentSites)*datasetObject.sizeGB)
            if tier.find('MINIAOD')>=0 and len(datasetObject.currentSites)>0:
    #          print datasetName,datasetObject.sizeGB
              miniaodSizeNoRepl += datasetObject.sizeGB
              miniaodSizeRepl += datasetVolume
            datasetUsage = 0
            for s,a in datasetObject.nAccesses.iteritems():
                if not re.match(sitePattern,s):
                    continue
                if s not in siteAccessDict:
                    siteAccessDict[s] = [0,0]
                for t,n in a.iteritems():
                    if (nowish-t)<(86400*30):
                        datasetUsage+=n
            totalVolume += datasetVolume
            totalUsage += datasetUsage
            if tier not in tiers:
              tier = 'Other'
            if tier in tiers:
                val = tiers[tier]
                hVolumeFrac.Fill(val,datasetVolume)
                hUsageFrac.Fill(val,datasetUsage)
        hVolumeFrac.Scale(1./totalVolume)
        hUsageFrac.Scale(1./totalUsage)
        for hist in [hUsageFrac,hVolumeFrac]:
            ROOT.MitRootStyle.InitHist(hist,"","",1)
        hVolumeFrac.GetYaxis().SetTitle('current volume fraction')
        hUsageFrac.GetYaxis().SetTitle('usage fraction (30 days)')
    #    hUsageFrac.SetNormFactor()
        for hist in [hUsageFrac,hVolumeFrac]:
            hist.SetFillColor(8)
            hist.SetLineColor(8)
            hist.SetFillStyle(1001)
            hist.SetMinimum(0.)
            hist.SetTitle('')
            c11.Clear()
            c11.cd()
            c11.SetBottomMargin(.2)
            c11.SetRightMargin(.2)
            hist.Draw("hist")
            if hist==hVolumeFrac:
                c11.SaveAs(monitorDB+'/FractionVolume_%s.png'%(groupPattern))
            else:
                c11.SaveAs(monitorDB+'/FractionUsage_%s.png'%(groupPattern))
        print "no replication  ",miniaodSizeNoRepl
        print "with replication",miniaodSizeRepl
        c21 = ROOT.TCanvas("c21","c21",1000,600)
        for h in [hVolumeFrac,hUsageFrac]:
            h.Delete()
        return


    print "Computing average number of sites"
    nAverageSites,timeOnSites = calculateAverageNumberOfSites(sitePattern,datasetSet,start,end,datasetPattern)

    '''==============================================================================
                                     our usage plots
       =============================================================================='''
    cUsage = ROOT.TCanvas("c1","c1",800,800)
    maxBin = 8
    nBins = 60.
    l = []
    low = 0
    i = 0
    while low < maxBin:
        l.append(low)
        low += (maxBin/nBins) * (1.1)**(i)
        i += 1
    l.append(maxBin)
    hUsage = ROOT.TH1F("dataUsage","Data Usage",len(l)-1,array('f',l))
    # delta = float(maxBin)/(2*(nBins-1)) # so that bins are centered
    # hUsage = ROOT.TH1F("dataUsage","Data Usage",nBins,-delta,maxBin+delta)
    kBlack = 1
    if not rc:
        ROOT.MitRootStyle.InitHist(hUsage,"","",kBlack) 
    titles = "; Accesses/month; Fraction of total data volume"
    hUsage.SetTitle(titles)
    meanVal = 0.
    sumWeight = 0.
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
        meanVal += value
        sumWeight += weight
        nEntries += 1
        hUsage.Fill(min(maxBin,value),weight)
    #totalSize = hUsage.Integral()
    print "Found %i datasets, corresponding to an average volume of %3f PB"%(nEntries,float(totalSize)/1000.)
    if (sumWeight==0):
      sumWeight=1;
      totalSize=1;
    meanVal = meanVal/sumWeight
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
    ROOT.MitRootStyle.AddText("Overflow added to last bin.")
    if groupPattern == ".*":
        groupPattern = "All"
    integralTexts = []
    integralTexts.append( "Group: %s"%(groupPattern) ) 
    integralTexts.append( "Period: [%s, %s]\n"%( strftime("%Y-%m-%d",gmtime(start)) , strftime("%Y-%m-%d",gmtime(end)) ) )
    integralTexts.append( "Average data managed: %.3f PB\n"%(totalSize/1000.) )
    # integralTexts.append( "Mean: %.3f accesses/month\n"%( meanVal ) )
    integralTexts.append( "Mean: %.3f accesses/month\n"%(hUsage.GetMean()) )
    positions = [0.85,0.8, 0.75, 0.7]
    plotTText = [None,None,None,None]
    for i in range(4):
      plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
      plotTText[i].SetTextSize(0.04)
      plotTText[i].SetTextColor(2)
      plotTText[i].Draw()
    try:
        cUsage.SaveAs(monitorDB+"/Usage_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
    except KeyError:
        cUsage.SaveAs(monitorDB+"/Usage_%s_%i_%i.png"%(groupPattern,start,end))

    # houtFile = ROOT.TFile(monitorDB+"/outfile.root","UPDATE")
    # houtFile.cd()
    # hUsage.Write("%s_%s"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
    # houtFile.Close()


    '''==============================================================================
                                     CRB usage plots
       =============================================================================='''

    ROOT.gPad.SetLogy(0) 
    hCRB = ROOT.TH1F("CRBUsage","Data Usage",17,-1.5,15.5)
    hZeroOne = ROOT.TH1F("CRBZeroOne","Zero and One Bin",100,0,1.);
    hTime = ROOT.TH1F("time","time",100,-0.1,1.1);
    if not rc:
        ROOT.MitRootStyle.InitHist(hCRB,"","",kBlack) 
        ROOT.MitRootStyle.InitHist(hZeroOne,"","",kBlack) 
        ROOT.MitRootStyle.InitHist(hTime,"","",kBlack) 
    titles = "; <n_{accesses}>; Prorated data volume [TB]"
    hCRB.SetTitle(titles)
    hZeroOne.SetTitle(titles)
    titles = "; Prorated Time Fraction; Data volume [TB]"
    hTime.SetTitle(titles)
    cCRB = ROOT.TCanvas("c2","c2",800,800)
    cZeroOne = ROOT.TCanvas("c3","c3",800,800)
    cTime = ROOT.TCanvas("c4","c4",800,800)
    txtfile = open(os.environ['MONITOR_PLOTTEXT']+'.txt','w')
    for datasetName,datasetObject in datasetSet.iteritems():
        if datasetObject.cTime>end:
          continue
        if not re.match(datasetPattern,datasetName):
            continue
        if datasetObject.nFiles==0:
            # what
            continue
        sizeGB = datasetObject.sizeGB
        for siteName in datasetObject.movement:
            if not re.match(sitePattern,siteName): 
                continue
            timeOnSite = timeOnSites[datasetName][siteName]
            # print timeOnSite
            value = 0
            if siteName in datasetObject.nAccesses:
                for utime,n in datasetObject.nAccesses[siteName].iteritems():
                    if utime <= end and utime >= start:
                        value += float(n)/datasetObject.nFiles
                fillValue = min(max(1,value), 14.5)
                # if value < 1:
                #     print value,fillValue
            if value == 0:
                if datasetObject.cTime > start:
                    fillValue = 0
                else:
                    fillValue = -1
            txtfile.write('%10f %20s %s\n'%(timeOnSite,siteName,datasetName))
            weight = float(sizeGB * timeOnSite)/1000.
    #        print datasetObject
    #        print fillValue,weight
    #        sys.exit(-1)
            hCRB.Fill(fillValue,weight)
            if (fillValue == 0) or (fillValue == 1):
                hZeroOne.Fill(value,weight)
            hTime.Fill(timeOnSite,sizeGB/1000.)
    try:
        histColor = os.environ['MONITOR_COLOR']
        hCRB.SetLineColor(histColor)
        hTime.SetLineColor(histColor)
        hZeroOne.SetLineColor(histColor)
    except KeyError:
        pass

    txtfile.close()
    if crbLabel!='':
        print 'Updating',rootFile
        fSave = ROOT.TFile(rootFile,'UPDATE')
        histName = 'h_'+os.environ['MONITOR_PLOTTEXT']
        fSave.WriteTObject(hCRB,histName,"Overwrite")
        fSave.WriteTObject(hTime,histName+'_time','Overwrite')
        fSave.Close()

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
      plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
      plotTText[i].SetTextSize(0.04)
      plotTText[i].SetTextColor(2)
      plotTText[i].Draw()
    if groupPattern == ".*":
        groupPattern = "All"
    try:
        hCRB.SaveAs(monitorDB+"/CRBUsage_%s_%s.C"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
        cCRB.SaveAs(monitorDB+"/CRBUsage_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
    except KeyError:
        cCRB.SaveAs(monitorDB+"/CRBUsage_%s_%i_%i.png"%(groupPattern,start,end))
        
    cZeroOne.cd()
    hZeroOne.Draw("hist")
    ROOT.MitRootStyle.OverlayFrame()
    plotTText = [None,None]
    for i in range(1):
      plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
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
      plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
      plotTText[i].SetTextSize(0.04)
      plotTText[i].SetTextColor(2)
      plotTText[i].Draw()
    try:
        cTime.SaveAs(monitorDB+"/CRBTime_%s_%s.png"%(groupPattern,os.environ['MONITOR_PLOTTEXT']))
    except KeyError:
        cTime.SaveAs(monitorDB+"/CRBTime_%s_%i_%i.png"%(groupPattern,start,end))

    if pickleJar:
        pickleJar.close()
    for h in [hUsage,hCRB,hZeroOne,hTime]:
        h.Delete()

if __name__ == '__main__':

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
    crbLabel = ''
    rootFile = ''
    if len(sys.argv)>=5:
        sitePattern = str(sys.argv[1])
        start = max(genesis,int(sys.argv[2]))
        end = min(nowish,int(sys.argv[3]))
        jarFileName = str(sys.argv[4])
        if len(sys.argv)>=6:
            crbLabel = str(sys.argv[5])
            rootFile = str(sys.argv[6])
        makeSummaryPlots = False
    elif len(sys.argv)==3:
        sitePattern = sys.argv[1]
        start = genesis
        end = nowish
        jarFileName = sys.argv[2]
        makeSummaryPlots = True
    else:
        sys.stderr.write(' ERROR - wrong number of arguments\n')
        sys.stderr.write(usage)
        sys.exit(2)

    makeActualPlots(sitePattern,start,end,jarFileName,crbLabel,rootFile,makeSummaryPlots)
