#!/usr/local/bin/python
import sys, os, json
import phedexApi

phedexApi_ = phedexApi.phedexApi()

# Get all datasets at UNL for local group
jsonData = phedexApi_.blockReplicas(node='T2_US_Nebraska', group='local', show_dataset='y')

datasets = jsonData.get('phedex').get('dataset')
	for dataset in datasets:
		datasetName = dataset.get('name')
		jsonData = phedexApi_.requestList(type_='xfer', node='T2_US_Nebraska', group='local', dataset=datasetName)
		print jsonData