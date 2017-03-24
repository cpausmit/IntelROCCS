#!/bin/bash

mkdir -p    $MONITOR_DB
cd $MONITOR_DB #preventing temp files from being created in weird places

cp $MONITOR_BASE/html/* $MONITOR_DB/

export REFRESH_PICKLE=1 

# this catalogs things for later use 
export MONITOR_SITES='T12X'

export REFRESH_CACHE=1 
export MONITOR_DATASETS='RECO'
python ${MONITOR_BASE}/monitor.py 

export MONITOR_DATASETS='XAODX'
export REFRESH_CACHE=0
python ${MONITOR_BASE}/monitor.py 

# now we produce the sheets
python ${MONITOR_BASE}/generateXls.py
