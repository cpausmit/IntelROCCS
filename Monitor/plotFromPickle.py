#!/usr/bin/python

'''==============================================================================
    This script reads information from the pickle caches and directly
    makes plots
     =============================================================================='''

import os, sys
import re, glob, subprocess, time
import cPickle as pickle
import ROOT
from array import array
from Dataset import *
from operator import itemgetter

ROOT.gStyle.SetOptStat(0)

genesis=1378008000
nowish = time.time()


def makeActualPlots(site_pattern,dataset_pattern,start,end,jar_file,crb_label,hist_label,root_file,outdir):
    end = min(nowish,end)

    hCRB = ROOT.TH1F("CRB","Data Usage",17,-1.5,15.5)
    hZeroOne = ROOT.TH1F("CRBZeroOne","Zero and One Bin",100,0,1.);
    hTime = ROOT.TH1F("time","time",100,-0.1,1.1);
    titles = "; <n_{accesses}>; Prorated data volume [TB]"
    hCRB.SetTitle(titles)
    hZeroOne.SetTitle(titles)
    titles = "; Prorated Time Fraction; Data volume [TB]"
    hTime.SetTitle(titles)

    cCRB = ROOT.TCanvas("c2","c2",800,800)
    cZeroOne = ROOT.TCanvas("c3","c3",800,800)
    cTime = ROOT.TCanvas("c4","c4",800,800)

    if type(jar_file)==str:
        with open(jar_file,'rb') as f:
            datasets = pickle.load(f)
    else:
        datasets = jar_file

    for name,dataset in datasets.iteritems():
            if dataset.cTime>end:
                continue
            if not re.match(dataset_pattern,name.split('/')[-1]):
                    continue
            if dataset.nFiles==0:
                    # what
                    continue
            sizeGB = dataset.sizeGB
            time_on_sites = dataset.getTimeOnSites(start,end,site_pattern,True)
            for node,fraction in time_on_sites.iteritems():
                    if not re.match(site_pattern,node): 
                            continue
                    value = 0
                    if node in dataset.nAccesses:
                        value = dataset.getTotalAccesses(start,end,'^'+node+'$',True)
                        value /= float(dataset.nFiles)
                    fillValue = min(max(1,value),14.5)
                    if value == 0:
                            if dataset.cTime > start:
                                    fillValue = 0
                            else:
                                    fillValue = -1
                    weight = float(sizeGB * fraction)/1000.
                    hCRB.Fill(fillValue,weight)
                    if (fillValue == 0) or (fillValue == 1):
                            hZeroOne.Fill(value,weight)
                    hTime.Fill(fraction,sizeGB/1000.)

    print 'Updating',root_file
    fSave = ROOT.TFile(root_file,'UPDATE')
    histName = 'h_'+hist_label
    fSave.WriteTObject(hCRB,histName,"Overwrite")
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
    totalSize = hCRB.Integral()
    integralTexts = ["Period: [%s, %s]\n"%( strftime("%Y-%m-%d",gmtime(start)) , strftime("%Y-%m-%d",gmtime(end)) )]
    integralTexts.append( "Average data on disk: %.3f PB\n"%(totalSize/1000.) )
    positions = [0.8,0.75]
    plotTText = [None,None]
    for i in range(2):
        plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
        plotTText[i].SetNDC()
        plotTText[i].SetTextSize(0.04)
        plotTText[i].SetTextColor(2)
        plotTText[i].Draw()
    cCRB.SaveAs(outdir+"/CRB_%s.png"%(crb_label))
            
    cZeroOne.cd()
    hZeroOne.Draw("hist")
    plotTText = [None,None]
    for i in range(1):
        plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
        plotTText[i].SetTextSize(0.04)
        plotTText[i].SetTextColor(2)
        plotTText[i].Draw()
    cZeroOne.SaveAs(outdir+"/01_%s.png"%(crb_label))
            
    cTime.cd()
    hTime.Draw("hist")
    plotTText = [None,None]
    for i in range(1):
        plotTText[i] = ROOT.TText(.3,positions[i],integralTexts[i])
        plotTText[i].SetTextSize(0.04)
        plotTText[i].SetTextColor(2)
        plotTText[i].Draw()
    cTime.SaveAs(outdir+"/Time_%s.png"%(crb_label))

    for h in [hCRB,hZeroOne,hTime]:
            h.Delete()

if __name__ == '__main__':

    intervals = [3,6,12]
    end_dates = ['2016-12-31']
    repl = { 'X' : '.*' } # used for messing with regexes
    def fix_regex(s):
        for k,v in repl.iteritems():
            s = s.replace(k,v)
        return s


    for end_date in end_dates:
        end = time.mktime(time.strptime(end_date,'%Y-%m-%d'))
        for interval in intervals:
            label = '%s_%iMonths'%(config.plot_name,interval)
            start = end - interval*86400*30
            crb_label = '%s_ending_%s'%(label,end_date)

            makeActualPlots(config.plot_site_pattern,
                            config.plot_dataset_pattern,
                            start,
                            end,
                            'monitorCache_'+config.name+'.pkl',
                            crb_label,
                            label,
                            config.root_name+'.root',
                            os.getenv('MONITOR_DB')+'/')

