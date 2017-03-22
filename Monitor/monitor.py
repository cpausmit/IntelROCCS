#!/usr/bin/env python

import os
import re, glob, time, json, pprint
import cPickle as pickle
import multiprocessing as mp

from Dataset import Dataset,Request
import dynamoDB
import requestParse
import getRequests
import plotFromPickle
import generateXls

if __name__=="__main__":

    # global config
    class Stopwatch():
        def __init__(self):
            self.start = time.time()
        def report(self):
            now = time.time()
            print '=> %.2f seconds elapsed'%(now-self.start)
            self.start = now
     
    repl = { 'X' : '.*' , '12' : '[12]' } # used for messing with regexes
    def fix_regex(s):
        for k,v in repl.iteritems():
            s = s.replace(k,v)
        return s

    monitor_db = os.getenv('MONITOR_DB')
    os.system('mkdir -p %s/requests/'%monitor_db)

    dataset_pattern = os.getenv('MONITOR_DATASETS')
    site_pattern = os.getenv('MONITOR_SITES')
    dataset_regex = fix_regex(dataset_pattern)
    site_regex = fix_regex(site_pattern)

    refresh_cache = int(os.getenv('REFRESH_CACHE'))
    refresh_pickle = os.getenv('REFRESH_PICKLE')
    if refresh_pickle:
        refresh_pickle = int(refresh_pickle)
    else:
        refresh_pickle = 1
    refresh_plots = os.getenv('REFRESH_PLOTS')
    if refresh_plots:
        refresh_plots = int(refresh_plots)
    else:
        refresh_plots = 1

    NPROC = os.getenv('MONITOR_THREADS') # do NOT use >1 thread in prod
    NPROC = int(NPROC) if NPROC else 1

    genesis=1378008000
    nowish = time.time()
    sPerYear = 365*24*60*60


    ### MAIN ### 

    ### CATALOGING ###
    datasets = None
    sw = Stopwatch()

    if refresh_pickle:
        print 'Building the request history cache'
        # build the request history
        if refresh_cache:
            existing = glob.glob('%s/requests/requests_*json'%(monitor_db))
            last = max([int(x.split('/')[-1]
                             .replace('.json','')
                             .replace('requests_transfer_','')
                             .replace('requests_delete_',''))
                        for x in existing])
            for rt in ['transfer','delete']:
                os.system('rm -f %s/requests/requests_%s_%i.json/'%(monitor_db,rt,last)) # always refresh the last one
                for i in xrange(last,last+20): # unlikely to have 20*100 new requests in the last cycle
                    getRequests.request(which=rt,idx=i,nper=100,outdir=monitor_db+'/requests/')
            

        print 'Loading interesting datasets'
        # load all interesting datasets
        cursor = dynamoDB.getDbCursor()
        table_dump = dynamoDB.getDatasetsAndProps(cursor)
        datasets = {}
        for name,nfiles,size,dt in table_dump:
            if not re.match(dataset_regex,name.split('/')[-1]):
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
        pool = mp.Pool(processes=NPROC)
        all_transfers = pool.map(requestParse.parseTransfer, 
                                     glob.glob(monitor_db+'/requests/requests_transfer_*.json'))
        for reqs in all_transfers:
            for d in reqs:
                if not d in datasets:
                    continue
                for t,s in reqs[d]:
                    if not re.match(site_regex,s):
                        continue
                    datasets[d].addTransfer(s,t)
        sw.report()

        print 'Importing deletion history'
        all_deletions = pool.map(requestParse.parseDeletion, 
                                     glob.glob(monitor_db+'/requests/requests_delete_*.json'))
        for reqs in all_deletions:
            for d in reqs:
                if not d in datasets:
                    continue
                for t,s in reqs[d]:
                    if not re.match(site_regex,s):
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
            if not re.match(site_regex,node):
                continue
            timestamp = time.mktime(dt.timetuple())
            datasets[name].addAccesses(node,n,timestamp)
        sw.report()

        print 'Exporting to pickle'

        pickleJar = open('monitorCache_'+ site_pattern + '_' + dataset_pattern + '.pkl',"wb")
        pickle.dump(datasets,pickleJar,2) 
        pickleJar.close() 
        sw.report()


    # after everything is done, draw plots
    if refresh_plots:
        if not datasets:
            datasets = pickle.load(open('monitorCache_'+ site_pattern + '_' + dataset_pattern + '.pkl',"rb"))

        plot_site_patterns = ['T1_X','T2_X','T12X']

        if 'AOD' in dataset_pattern:
            plot_dataset_patterns = ['XAODX','MINIAODX','XAODSIM','XAOD']
        else:
            plot_dataset_patterns = ['RECO']

        intervals = [3,6,12]
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

        for plot_dataset_pattern in plot_dataset_patterns:
            for plot_site_pattern in plot_site_patterns:
                for end_date in end_dates:
                    end = time.mktime(end_date)
                    str_end_time = time.strftime('%Y-%m-%d',end_date)
                    for interval in intervals:
                        start = end - interval*86400*30
                        plot_dataset_regex = fix_regex(plot_dataset_pattern)
                        plot_site_regex = fix_regex(plot_site_pattern)
                        plot_title = plot_site_pattern + '_' + plot_dataset_pattern
                        label = '%s_%iMonths'%(plot_title,interval)
                        crb_label = '%s_ending_%s'%(label,str_end_time)
                        plotFromPickle.makeActualPlots(plot_site_regex,
                                                       plot_dataset_regex,
                                                       start,
                                                       end,
                                                       #'monitorCache_'+site_pattern+'_'+dataset_pattern+'.pkl',
                                                       datasets,
                                                       crb_label,
                                                       label,
                                                       plot_site_pattern+'_'+str_end_time+'.root',
                                                       monitor_db)
    
            












