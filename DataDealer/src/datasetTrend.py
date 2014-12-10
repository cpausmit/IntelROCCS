#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
from operator import itemgetter
import phedexData, popDbData, phedexApi, popDbApi

phedexData = phedexData.phedexData()
#datasets = phedexData.getAllDatasets()
datasets = ['/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12-PU_S7_START52_V9-v2/AODSIM']
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
    norm = max(cpuHours, key=itemgetter(1))[1]/100.0
    print norm
    for week in cpuHours:
        date = datetime.datetime.fromtimestamp(float(week[0])/10**3).strftime('%Y-%m-%d')
        cpuH = week[1]
        print date + "\t" + '*' * cpuH
