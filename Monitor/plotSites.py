#!/usr/bin/env python

import os
import ROOT as root

class site(object):
  def __init__(self,l):
    self.name=l[0]
    self.quota=float(l[1])
    self.used=float(l[2])
    self.toDelete=float(l[3])
    self.lastCp=float(l[4])

fileName = os.environ.get('SITE_MONITOR_FILE')
monitorDB = os.environ.get('MONITOR_DB')

siteInfos=[]

with open(fileName) as siteMonitorFile:
  for line in siteMonitorFile:
    l = line.strip().split(' ')
    siteInfos.append(site(l))

nSites = len(siteInfos)

# last cp fraction
hLow = root.TH1F("hLow","hLow",nSites+1,-1.5,nSites-0.5)
hMed = root.TH1F("hMed","hMed",nSites+1,-1.5,nSites-0.5)
hHigh = root.TH1F("hHigh","hHigh",nSites+1,-1.5,nSites-0.5)
medThreshold=0.5
highThreshold=0.7
xaxis = hHigh.GetXaxis()
hHigh.GetYaxis().SetTitle('last copy fraction')
hHigh.SetTitle('')
xaxis.SetBinLabel(1,' ')
hHigh.SetMaximum(1)

for i in range(nSites):
  s = siteInfos[i]
  if s.quota==0:
    # misread line
    continue
  xaxis.SetBinLabel(i+2,s.name)
  cpFr = s.lastCp/s.quota
  if cpFr<highThreshold:
    if cpFr<medThreshold:
      hLow.Fill(i,cpFr)
    else:
      hMed.Fill(i,cpFr)
  else:
    hHigh.Fill(i,cpFr)

c = root.TCanvas('c','c',1000,600)
c.SetBottomMargin(.3)
for hist,color in zip([hLow,hMed,hHigh],[8,5,2]):
  hist.SetFillColor(color)
  hist.SetLineColor(color)
hHigh.SetStats(0)
hHigh.Draw("hist")
hMed.Draw("hist same")
hLow.Draw("hist same")

c.SaveAs(monitorDB+'/lastCpFractionSites.png')


hLow.Reset()
hMed.Reset()
hHigh.Reset()
medThreshold=0.5
highThreshold=0.7
superHighThreshold=0.9
hHigh.GetYaxis().SetTitle('used fraction')
hHigh.SetMaximum(1)

for i in range(nSites):
  s = siteInfos[i]
  usedFr = s.used/s.quota
  if usedFr>superHighThreshold:
    hLow.Fill(i,usedFr)
  else:
    if usedFr<highThreshold:
      if usedFr<medThreshold:
        hLow.Fill(i,usedFr)
      else:
        hMed.Fill(i,usedFr)
    else:
      hHigh.Fill(i,usedFr)

c.Clear()
c.SetBottomMargin(.3)
for hist,color in zip([hLow,hMed,hHigh],[2,5,8]):
  hist.SetFillColor(color)
  hist.SetLineColor(color)
hHigh.SetStats(0)
hHigh.Draw("hist")
hMed.Draw("hist same")
hLow.Draw("hist same")

c.SaveAs(monitorDB+'/usedSites.png')
