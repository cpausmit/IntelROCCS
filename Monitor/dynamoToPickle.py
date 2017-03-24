#!/usr/bin/env python 

import os, sys

import re, glob, time, json, pprint
import cPickle as pickle
import multiprocessing as mp
from Dataset import Dataset,Request
import dynamoDB
import requestParse
import config

#===================================================================================================
#    M A I N
#===================================================================================================

if __name__=="__main__":

    sw = Stopwatch()
    print 'Loading interesting datasets'
    # load all interesting datasets
    cursor = dynamoDB.getDbCursor()
    table_dump = dynamoDB.getDatasetsAndProps(cursor)
    datasets = {}
    for name,nfiles,size,dt in table_dump:
        if not re.match(config.dataset_pattern,name):
            continue
        ds = Dataset(name)
        ds.nFiles = int(nfiles)
        ds.sizeGB = size/(10.**9)
        ds.cTime = time.mktime(dt.timetuple())
        datasets[name] = ds

    sw.report()
    print 'Considering %i relevant datasets'%(len(datasets))

    print 'Importing transfer history'
    # import phedex history
    pool = mp.Pool(processes=10)
    all_transfers = pool.map(requestParse.parseTransfer, 
                                 glob.glob(config.requests_dir+'/requests_transfer_*.json'))
    for reqs in all_transfers:
        for d in reqs:
            if not d in datasets:
                continue
            for t,s in reqs[d]:
                if not re.match(config.site_pattern,s):
                    continue
                datasets[d].addTransfer(s,t)
    sw.report()

    print 'Importing deletion history'
    all_deletions = pool.map(requestParse.parseDeletion, 
                                 glob.glob(config.requests_dir+'/requests_delete_*.json'))
    for reqs in all_deletions:
        for d in reqs:
            if not d in datasets:
                continue
            for t,s in reqs[d]:
                if not re.match(config.site_pattern,s):
                    continue
                datasets[d].addDeletion(s,t)
    sw.report()

    print 'Sorting history'
    # organize the history
    for name,d in datasets.iteritems():
        d.sortRequests()
    sw.report()

    print 'Importing CRAB+xrootd accesses'
    # import access history
    all_accesses = dynamoDB.getDatasetsAccesses(cursor)
    for name,node,dt,n in all_accesses:
        if name not in datasets:
            continue
        if not re.match(config.site_pattern,node):
            continue
        timestamp = time.mktime(dt.timetuple())
        datasets[name].addAccesses(node,n,timestamp)
    sw.report()

    print 'Exporting to pickle'
    i=0
    for k in datasets:
        if i == 10: break
        print k
        print datasets[k]
        i+= 1

    pickleJar = open('monitorCache_'+ config.name+'.pkl',"wb")
    pickle.dump(datasets,pickleJar,2) # put the pickle in a jar
    pickleJar.close() # close the jar
    sw.report()

    sys.exit(0)
