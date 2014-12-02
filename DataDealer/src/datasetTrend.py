#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
import phedexData, popDbData, phedexApi, popDbApi

phedexData = phedexData.phedexData()
datasets = phedexData.getAllDatasets()
phedexApi_ = phedexApi.phedexApi()
popDbApi_ = popDbApi.popDbApi()
startDates = []
dates = dict()
for dataset in datasets:
    jsonData = phedexApi_.data(dataset=dataset, level='block', create_since='0')
    timestamp = jsonData.get('phedex').get('dbs')[0].get('dataset')[0].get('time_create')
    startDate = datetime.datetime.fromtimestamp(timestamp)
    dates[dataset] = startDate
    startDates.append(startDate)
startDate = min(startDates)
endDate = datetime.datetime.now()
jsonDataCPU = popDbApi_.getDSNdata(tstart=startDate.strftime('%Y-%m-%d'), tstop=endDate.strftime('%Y-%m-%d'), aggr='week', orderby='totcpu')
fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/cpu.tsv', 'w')
json.dump(jsonDataCPU, fs)
fs.close()
jsonDataAcc = popDbApi_.getDSNdata(tstart=startDate.strftime('%Y-%m-%d'), tstop=endDate.strftime('%Y-%m-%d'), aggr='week', orderby='naccess')
fs = open('/local/cmsprod/IntelROCCS/DataDealer/Visualizations/acc.tsv', 'w')
json.dump(jsonDataAcc, fs)
fs.close()
