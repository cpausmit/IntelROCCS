#!/usr/bin/env python

from sys import argv
from ROOT import TH1F, TFile
import os
import xml.etree.ElementTree as ET
from string import ascii_uppercase

def getRow(histName):
    rows = {
        'XAODX' : [3,4,5],
        'XAOD' : [8,9,10],
        'XAODSIM' : [13,14,15],
        'MINIAODX' : [18,19,20],
        'RECO' : [23,24,25]
    }
    ll = histName.split('_')
    tier = ll[2]
    period = ll[3]
    if '3Months' in period:
        iM = 0
    elif '6Months' in period:
        iM = 1
    else:
        iM = 2
    return rows[tier][iM]

def write_xls(label,outdir,templdir)
    fIn = TFile(label+'.root')

    user = os.environ['USER']

    os.system('mkdir -p /tmp/%s/'%user)
    os.system('rm -rf /tmp/%s/xlstempl/'%(user))
    os.system('cp -r %s /tmp/%s/xlstempl'%(templdir,user))

    tree = ET.parse('/tmp/%s/xlstempl/xl/worksheets/sheet2.xml'%(user))
    root = tree.getroot()

    cells = {}

    for child in root.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheetData'):
        for grandchild in child:
                cells[grandchild.attrib['r']] = grandchild.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')

    keys = fIn.GetListOfKeys()
    for k in keys:
        if not('TH1' in k.GetClassName()):
            continue
        name = k.GetName()
        hist = fIn.Get(name)
        try:
            row = getRow(name)
        except KeyError:
            print name
            continue
        for iB in xrange(1,hist.GetNbinsX()+1):
            column = ascii_uppercase[iB]
            val = hist.GetBinContent(iB)
            # print iB,column,val,name,row
            cell = cells['%s%i'%(column,row)]
            cell.text = str(val)
            cell.set('updated','yes')

    tree.write('/tmp/%s/xlstempl/xl/worksheets/sheet2.xml'%user)

    os.system('cd /tmp/%s/xlstempl/; find . -type f | xargs zip new.xlsx; cd -'%user)

    mvcmd = 'mv /tmp/%s/xlstempl/new.xlsx %s/xls/%s.xlsx'%(user,outdir,label)
    print mvcmd
    os.system(mvcmd)

