#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Data modeling
#---------------------------------------------------------------------------------------------------
import sys, json, datetime
import phedexData, popDbData, phedexApi, popDbApi

#phedexData = phedexData.phedexData()
#datasets = phedexData.getAllDatasets()
phedexApi_ = phedexApi.phedexApi()
popDbData_ = popDbData.popDbData()
datasets = ['/DYToMuMu_M-20_TuneZ2star_14TeV-pythia6-tauola/GEM2019Upg14DR-final_phase1_age1k_PU140bx25_PH1_1K_FB_V2-v1/AODSIM']
for dataset in datasets:
	jsonData = phedexApi_.data(dataset=dataset, level='block', create_since='2013-11-01')
	timestamp = jsonData.get('phedex').get('dbs')[0].get('dataset')[0].get('time_create')
	date = datetime.datetime.fromtimestamp(timestamp)
	printDate = datetime.datetime.fromtimestamp(timestamp).strftime('%d-%b-%Y')
	fs = open('/local/cmsprod/IntelROCCS/DataDealer/Demo/datasetHistory.tsv', 'w')
	fs.write("dataset\tdate\tcpuh\n")
	fs.close()
	endDate = datetime.datetime.now()
	while (date < endDate):
		cpuH = popDbData_.getDatasetCpus(dataset, date.strftime('%Y-%m-%d'))
		if not cpuH:
			cpuH = 0
		fs = open('/local/cmsprod/IntelROCCS/DataDealer/Demo/datasetHistory.tsv', 'a')
		fs.write("%s\t%s\t%d\n" % (dataset, date.strftime('%Y-%m-%d'), cpuH))
		fs.close()
		date = date + datetime.timedelta(days=1)
