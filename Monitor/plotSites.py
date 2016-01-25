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
    self.status=1
  def str(self):
    return '%s %i %i %i %i'%(self.name,int(self.quota),int(self.used),int(self.toDelete),int(self,lastCp))

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
hHigh = root.TH1F("hHigh","hHigh",nSites+1,-1.5,nSites-0.5)
hAverage = root.TH1F("hAve","hAve",nSites+1,-1.5,nSites-0.5)
legend = root.TLegend(0.6,0.8,.9,.9)
medThreshold=0.5
highThreshold=0.7
xaxis = hHigh.GetXaxis()
hHigh.GetYaxis().SetTitle('last copy fraction')
hHigh.SetTitle('')
xaxis.SetBinLabel(1,' ')
hHigh.SetMaximum(1.5)

with open(os.getenv('DETOX_DB')+'/SitesInfo.txt') as fSitesInfo:
  started=False
  for line in fSitesInfo:
    if line.find('#')>=0:
      if started:
        break
      else:
        continue
    ll = line.strip().split()
    for site in siteInfos:
      if site.name==ll[-1]:
        site.status=int(ll[0])

num=0.
denom=0
for i in xrange(nSites):
  s = siteInfos[i]
  xaxis.SetBinLabel(i+2,s.name)
  if s.quota==0 or s.status==0:
    continue
  cpFr = s.lastCp/s.quota
  num+=s.lastCp
  denom+=s.quota
  if cpFr<highThreshold:
    hLow.Fill(i,cpFr)
  else:
    hHigh.Fill(i,cpFr)
  i+=1
average=num/denom
for iB in xrange(1,nSites+2):
  hAverage.SetBinContent(iB,average)

c = root.TCanvas('c','c',1500,900)
c.SetBottomMargin(.3)
for hist,color in zip([hLow,hHigh],[8,2]):
  hist.SetFillColor(color)
  hist.SetLineColor(color)
hHigh.SetStats(0)
hHigh.Draw("hist")
hLow.Draw("hist same")
hAverage.Draw("hist same")
legend.AddEntry(hHigh,"last copy > 0.7","f")
legend.AddEntry(hLow,"last copy < 0.7","f")
legend.AddEntry(hAverage,"weighted average","l")
legend.Draw()
c.SaveAs(monitorDB+'/lastCpFractionSites.png')


hLow.Reset()
hHigh.Reset()
legend = root.TLegend(0.6,0.8,.9,.9)
hAverage.Reset()
medThreshold=0.5
highThreshold=0.7
superHighThreshold=0.9
hHigh.GetYaxis().SetTitle('used fraction')
hHigh.SetMaximum(1.5)

num=0.
denom=0
for i in xrange(nSites):
  s = siteInfos[i]
  if s.quota==0 or s.status==0:
    continue
  usedFr = s.used/s.quota
  num+=s.used
  denom+=s.quota
  if usedFr>superHighThreshold:
    hLow.Fill(i,usedFr)
  else:
    if usedFr<highThreshold:
      hLow.Fill(i,usedFr)
    else:
      hHigh.Fill(i,usedFr)
average=num/denom
for iB in xrange(1,nSites+2):
  hAverage.SetBinContent(iB,average)

c.Clear()
c.SetBottomMargin(.3)
for hist,color in zip([hLow,hHigh],[2,8]):
  hist.SetFillColor(color)
  hist.SetLineColor(color)
hHigh.SetStats(0)
hHigh.Draw("hist")
hLow.Draw("hist same")
hAverage.Draw("hist same")
legend.AddEntry(hLow,"used>0.9 or <0.8","f")
legend.AddEntry(hHigh,"0.8<used<0.9","f")
legend.AddEntry(hAverage,"weighted average","l")
legend.Draw()

c.SaveAs(monitorDB+'/usedSites.png')
