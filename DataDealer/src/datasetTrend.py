#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
from operator import itemgetter
import phedexData, popDbData, phedexApi, popDbApi

phedexData = phedexData.phedexData()
#datasets = phedexData.getAllDatasets()
datasets = ['/QCD_Pt-15to3000_Tune4C_Flat_14TeV_pythia8/GEM2019Upg14DR-final_phase1_PU50bx25_DES19_62_V8-v1/AODSIM']
phedexApi_ = phedexApi.phedexApi()
popDbApi_ = popDbApi.popDbApi()
startDates = []
dates = dict()
for dataset in datasets:
    jsonDataCPU = popDbApi_.getSingleDSstat(aggr='week', orderby='totcpu', name=dataset)
    jsonData = jsonDataCPU.get('data')[0]
    datasetName = jsonData.get('name')
    print datasetName
    cpuHours = jsonData.get('data')
    norm = float(max(cpuHours, key=itemgetter(1))[1])
    print norm
    for week in cpuHours:
        date = datetime.datetime.fromtimestamp(float(week[0])/10**3).strftime('%Y-%m-%d')
        cpuH = float(week[1])
        print date + "\t" + '*' * int((cpuH/norm)*100)
