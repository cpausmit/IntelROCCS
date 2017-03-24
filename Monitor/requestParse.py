#!/usr/bin/env python

import json

import re

def parseDeletion(fpath):
	f = open(fpath)
	payload = json.load(f)['phedex']['request']
	deletions = {}
	for r in payload:
		datasets = [x['name'] for x in r['data']['dbs']['dataset']]
		for d in datasets:
			if d not in deletions:
				deletions[d] = []
		for n in r['nodes']['node']:
			if 'decided_by' not in n:
				continue
			if not n['decided_by']['decision']=='y':
				continue
			timestamp = n['decided_by']['time_decided']
			name = n['name']
			for d in datasets:
				deletions[d].append((timestamp,name))
	return deletions

def parseTransfer(fpath):
	f = open(fpath)
	payload = json.load(f)['phedex']['request']
	transfers = {}
	for r in payload:
		datasets = [x['name'] for x in r['data']['dbs']['dataset']]
		for d in datasets:
			if d not in transfers:
				transfers[d] = []
		for n in r['destinations']['node']:
			if 'decided_by' not in n:
				continue
			if not n['decided_by']['decision']=='y':
				continue
			timestamp = n['decided_by']['time_decided']
			name = n['name']
			for d in datasets:
				transfers[d].append((timestamp,name))
	return transfers

