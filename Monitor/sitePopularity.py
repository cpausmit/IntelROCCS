#!/usr/bin/python

from Dataset import *
import ROOT
from sys import argv,stdout,stderr,maxint
import cPickle as pickle
import time
import re
from os import environ

def addDict(d1,d2):
    # add d2 to d1
    for k,v in d2.iteritems():
        if k in d1:
            d1[k] += v
        else:
            d1[k] = v

monitorDB = environ['MONITOR_DB']

fileName = 'monitorCacheAnalysisOps.pkl'
pklJar = open(fileName,"rb")
pklDict = pickle.load(pklJar) # open the pickle jar
datasetSet = pklDict["datasetSet"] # eat the pickle
nSiteAccess = pklDict["nSiteAccess"]
sitePattern = argv[1]

# startTime = 1393632000
startTime = int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
endTime = int(time.time())
# startTime = 1420243200
endTime = 1428292800
interval = endTime - startTime
sPerDay = 86400
# times = [28]
times = [1,2,3,7,14,21,28]

accessesBySite = {}
for k in nSiteAccess:
    if re.match(sitePattern,k):
        accessesBySite[k] = {}


for datasetName,datasetObject in datasetSet.iteritems():
    accesses = datasetObject.nAccesses
    for siteName in accessesBySite:
        try:
            accessTimes = accesses[siteName]
            addDict(accessesBySite[siteName],accessTimes)
        except KeyError:
            pass

fout = ROOT.TFile(monitorDB+'/sitePlots/plots.root','RECREATE')

popularityHist = ROOT.TH1F("pH","pH",100,0,.3)
combinedPopularityHist = ROOT.TH1F("cpH","cpH",100,0,.3)
c1 = ROOT.TCanvas("c1","c1",800,600)
c2 = ROOT.TCanvas("c2","c2",800,600)

graphs = {}
for siteName in accessesBySite:
    graphs[siteName] = ROOT.TGraph()

# print accessesBySite
for days in times:
    combinedPopularityHist.Reset()
    period = days*sPerDay
    for siteName in accessesBySite:
        graph = graphs[siteName]
        graph.Set(int(interval/period)+1)
        popularityHist.Reset()
        for j in range(int(interval/period)+1):
            accesses=0
            for accessTime,n in accessesBySite[siteName].iteritems():
                if startTime+j*period <= accessTime < startTime+(j+1)*period:
                    if n==0:
                        print siteName,startTime+j*period
                    # print "found accesses",accessTimes
                    accesses+=n
            graph.SetPoint(j,startTime+j*period,accesses)
            accesses = float(accesses)/period
            # print "filling %.2f for %i %i: %i - %i"%(accesses,days ,j,  startTime+j*period , startTime+(j+1)*period)
            popularityHist.Fill(accesses)
            combinedPopularityHist.Fill(accesses)
        c1.cd()
        c1.Clear()
        popularityHist.Draw()
        popularityHist.Write(siteName+'_'+str(days))
        c1.SaveAs(monitorDB + '/sitePlots/'+siteName+'_'+str(days)+'.png')
        c2.cd()
        c2.Clear()
        graph.Draw()
        graph.Write(siteName+'_'+str(days)+'_time')
        c2.SaveAs(monitorDB + '/sitePlots/'+siteName+'_'+str(days)+'_time.png')
    c1.cd()
    c1.Clear()
    combinedPopularityHist.Draw()
    combinedPopularityHist.Write("combined_"+str(days))
    c1.SaveAs(monitorDB + '/sitePlots/'+str(days)+'.png')
    c2.cd()
    c2.Clear()
    first=True
    maxAccess=0
    minAccess=maxint
    for siteName in accessesBySite:
        scale=graphs[siteName].GetMean(2)
        if scale > 0:
            y = graphs[siteName].GetY()
            for i in range(graphs[siteName].GetN()):
                y[i] *= 1./scale
                maxAccess = max(maxAccess,y[i])
                minAccess = min(minAccess,y[i])
    k=0
    for siteName in accessesBySite:
        k+=1
        graphs[siteName].SetLineColor(k)
        if first:
            graphs[siteName].SetMaximum(maxAccess)
            graphs[siteName].SetMinimum(minAccess)
            graphs[siteName].Draw()
            first=False
        else:
            graphs[siteName].Draw("same")
    c2.SaveAs(monitorDB + '/sitePlots/'+str(days)+'_time.png')
    c2.Write("time_"+str(days))

fout.Write()
fout.Close()

