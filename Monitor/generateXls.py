#!/usr/bin/env python

from sys import argv
from ROOT import TH1F, TFile
import os
import xml.etree.ElementTree as ET
from string import ascii_uppercase

label = argv[1]

monitordb = os.environ['MONITOR_DB']+'/'
monitorbase = os.environ['MONITOR_BASE']+'/'

fIn = TFile(monitordb+'monitorCacheAll.root')

def getRow(histName):
  rows = {
    'AllAOD' : [3,4,5],
    'AOD' : [8,9,10],
    'AODSIM' : [13,14,15],
    'MINIAOD' : [18,19,20],
    'RECO' : [23,24,25]
  }
  ll = histName.split('_')
  tier = ll[1]
  period = ll[2]
  if '3Months' in period:
    iM = 0
  elif '6Months' in period:
    iM = 1
  else:
    iM = 2
  return rows[tier][iM]

os.system('rm -rf /tmp/xlstempl')
os.system('cp -r %s/templ/ /tmp/xlstempl')

tree = ET.parse('/tmp/xlstempl/xl/worksheets/sheet2.xml')
root = tree.getroot()

cells = {}

for child in root.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheetData'):
  for grandchild in child:
      cells[grandchild.attrib['r']] = grandchild.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')

keys = fIn.GetListOfKeys()
for k in keys:
  if not('TH1' in k.GetClassName()):
    continue
  name = key.GetName()
  hist = fIn.Get(name)
  row = getRow(name)
  for iB in xrange(1,hist.GetNbinsX()):
    column = ascii_uppercase[iB]
    val = hist.GetBinContent(iB)
    cell = cells['%s%i'%(column,row)]
    cell.text = str(val)
    cell.set('updated','yes')

tree.write('/tmp/xlstempl/xl/worksheets/sheet2.xml')

os.system('cd /tmp/xlstempl/; find . -type f | xargs zip new.xlsx; cd -')
os.system('mv /tmp/xlstempl/new.xlsx %s/xls/%s.xlsx'%(monitordb,label))