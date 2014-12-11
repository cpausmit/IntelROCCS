#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
from operator import itemgetter
import phedexData, popDbData, phedexApi, popDbApi

fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/datasets.csv', 'w')
fs.write("dataset,maxCPU,deltaCPU,maxAcc,deltaAcc,popularityTime,dataTier,sizeGb,age\n")
fs.close()
phedexData_ = phedexData.phedexData()
datasets = phedexData_.getAllDatasets()
phedexApi_ = phedexApi.phedexApi()
popDbApi_ = popDbApi.popDbApi()
startDates = []
dates = dict()
for dataset in datasets:
    weeksBefore = 0
    weeksAfter = 0
    jsonDataCPU = popDbApi_.getSingleDSstat(aggr='week', orderby='totcpu', name=dataset)
    if not jsonDataCPU:
        continue
    jsonData = jsonDataCPU.get('data')[0]
    datasetName = jsonData.get('name')
    cpuData = jsonData.get('data')
    maxValue = max(cpuData, key=itemgetter(1))
    maxIndex = cpuData.index(maxValue)
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
    if (popularityTime < 2):
        continue
    deltaCpu = cpuData[maxIndex-weeksBefore][1] - cpuData[maxIndex-weeksBefore-1][1]
    dataTier = datasetName.split('/')[3]
    age = maxIndex*7
    size = phedexData_.getDatasetSize(datasetName)
    jsonDataAcc = popDbApi_.getSingleDSstat(aggr='week', orderby='naccess', name=dataset)
    if not jsonDataAcc:
        continue
    jsonData = jsonDataCPU.get('data')[0]
    accData = jsonData.get('data')
    maxAccValue = int(accData[maxIndex][1])
    deltaAcc = accData[maxIndex-weeksBefore][1] - accData[maxIndex-weeksBefore-1][1]
    fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/datasets.csv', 'a')
    fs.write("%s,%d,%d,%d,%d,%d,%s,%d,%d\n" % (datasetName, maxCpuValue, deltaCpu, maxAccValue, deltaAcc, popularityTime, dataTier, size, age))
    fs.close()
