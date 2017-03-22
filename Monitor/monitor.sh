#!/bin/bash

mkdir -p    $MONITOR_DB
cd $MONITOR_DB #preventing temp files from being created in weird places
rm -f $MONITOR_DB/*png # clean up old images, will be replaced shortly

export REFRESH_PICKLE=0

# this catalogs things for later use 
export MONITOR_SITES='T12X'
export MONITOR_DATASETS='XAODX'
export REFRESH_CACHE=0
python ${MONITOR_BASE}/monitor.py 

export REFRESH_CACHE=0
export MONITOR_DATASETS='RECO'
python ${MONITOR_BASE}/monitor.py 

# now we produce the plots
