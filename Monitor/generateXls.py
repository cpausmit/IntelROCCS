#!/usr/bin/env python

from sys import argv
from ROOT import TH1F, TFile
import os
import xml.etree.ElementTree as ET
from string import ascii_uppercase
import time

def getRow(histName):
    rows = {
        'XAODX' : [3,4,5],
        'XAOD' : [8,9,10],
        'XAODSIM' : [13,14,15],
        'MINIAODX' : [18,19,20],
        'RECO' : [23,24,25]
    }
    ll = histName.split('_')
    if 'T12' in histName:
        tier = ll[2]
        period = ll[3]
    else:
        tier = ll[3]
        period = ll[4]
    if '3Months' in period:
        iM = 0
    elif '6Months' in period:
        iM = 1
    else:
        iM = 2
    return rows[tier][iM]

def write_xls(label,outdir,templdir):
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
            print 'could not find',name
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


xls_site_patterns = ['T1_X','T2_X','T12X']
monitor_db = os.getenv('MONITOR_DB')
monitor_base = os.getenv('MONITOR_BASE')


last_day = [-1,31,28,31,30,31,30,31,31,30,31,30,31]
now = time.time()
date = time.gmtime(now)
end_dates = [date]

year = 2015
month = 3
new_date = time.strptime('%i-%i-%i'%(year,month,last_day[month]),'%Y-%m-%d')
while new_date < date:
    end_dates.append(new_date)
    if month==12:
        year += 1
        month = 3
    else:
        month += 3
    new_date = time.strptime('%i-%i-%i'%(year,month,last_day[month]),'%Y-%m-%d')

newlines = []
for end_date in end_dates:
    if end_date==end_dates[0]:
        str_end_time = 'today'
    else:
        str_end_time = time.strftime('%Y-%m-%d',end_date)
    newlines.append('  <tr>  <td> Ending {0} </td> <td> <a href="xls/T1_{0}.xlsx">T1_X_{0}.xlsx</a> </td> <td> <a href="xls/T2_X_{0}.xlsx">T2_X_{0}.xlsx</a> </td> <td> <a href="xls/T12_X_{0}.xlsx">T12_X_{0}.xlsx</a></td> </tr> \n'.format(str_end_time))
    for xls_site_pattern in xls_site_patterns:
        write_xls(xls_site_pattern+'_'+str_end_time,monitor_db,monitor_base+'/templ/')

with open(monitor_base+'/html/xls.html','r') as inhtml:
    with open(monitor_db+'/xls.html','w') as outhtml:
        for line in inhtml:
            outhtml.write(line)
            if '!--' in line:
                for newline in newlines:
                    outhtml.write(newline)

