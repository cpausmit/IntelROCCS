#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
from operator import itemgetter
import phedexData, popDbData, phedexApi, popDbApi

fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/datasetTrend.csv', 'w')
fs.write("dataset,maxCPU,deltaCPU,maxAcc,deltaAcc,popularityTime,dataTier,size,age\n")
fs.close()
phedexData = phedexData.phedexData()
#datasets = phedexData.getAllDatasets()
datasets = ['/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12-PU_S7_START52_V9-v2/AODSIM']
phedexApi_ = phedexApi.phedexApi()
popDbApi_ = popDbApi.popDbApi()
startDates = []
dates = dict()
for dataset in datasets:
    weeksBefore = 0
    weeksAfter = 0
    deltaValue = 0.0
    jsonDataCPU = popDbApi_.getSingleDSstat(aggr='week', orderby='totcpu', name=dataset)
    jsonData = jsonDataCPU.get('data')[0]
    datasetName = jsonData.get('name')
    cpuData = jsonData.get('data')
    maxIndex, maxValue = max(enumerate(cpuData), key=itemgetter(1))
    maxCpuValue = int(maxValue[1])
    index = maxIndex - 1
    check = 0
    while index >=0:
        if cpuData[index][1] < 0.5*maxCpuValue:
            check += 1
            if check == 2:
                weeksBefore = maxIndex - index - 2
        else:
            check = 0
        index -= 1
    index = maxIndex + 1
    lastIndex = len(cpuData)
    while index < lastIndex:
        if cpuData[index][1] < 0.5*maxCpuValue:
            check += 1
            if check == 2:
                weeksAfter = index - maxIndex -2
        else:
            check = 0
        index += 1
    popularityTime = weeksBefore + weeksAfter + 1
    if (popularityTime > 2):
        continue
    deltaCpu = cpuData[maxIndex-weeksBefore][1] - cpuData[maxIndex-weeksBefore-1][1]
    dataTier = datasetName.split('/')[2]
    age = maxIndex*7
    size = phedexData_.getDatasetSize(datasetName)
    jsonDataAcc = popDbApi_.getSingleDSstat(aggr='week', orderby='naccess', name=dataset)
    jsonData = jsonDataCPU.get('data')[0]
    accData = jsonData.get('data')
    maxAccValue = int(accData[maxIndex][1])
    deltaAcc = accData[maxIndex-weeksBefore][1] - accData[maxIndex-weeksBefore-1][1]
    fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/datasetTrend.csv', 'a')
    fs.write("%s,%d,%d,%d,%d,%d,%s,%d,%d\n" % (datasetName, maxCpuValue, deltaCpu, maxAccValue, deltaAcc, popularityTime, dataTier, size, age))
    fs.close()
