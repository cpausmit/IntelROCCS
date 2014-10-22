#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Installation script for Data Dealer package
#
# B.Barrefors (Aug 13, 2014)
# --------------------------------------------------------------------------------------------------
# Reset in case getopts has been used previously in the shell
OPTIND=1

# Initialize our own variables:
testing=0
keep_cache=0

while getopts "tc" opt; do
    case "$opt" in
    t)  testing=1
        ;;
    c)  keep_cache=1
        ;;
    esac
done

# Configuration parameters
export INTELROCCS_USER=cmsprod
export INTELROCCS_GROUP=zh

if [ $testing -eq 0 ]
then
	INSTALL_DIR=/usr/local/IntelROCCS/Install
	DATA_DEALER_BUDGET="50000"
else
	INSTALL_DIR=/usr/local/IntelROCCS/Install-test
	DATA_DEALER_BUDGET="1000"
fi

# get src and install paths
#==========================

# get path of installation script, source code, and install path
INTELROCCS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATA_DEALER_SRC=${INTELROCCS_DIR}/DataDealer/src
DATA_DEALER_INSTALL=${INSTALL_DIR}/DataDealer
DATA_DEALER_LOG=/local/cmsprod/IntelROCCS/DataDealer
PHEDEX_CACHE=/usr/local/IntelROCCS/Cache/DataDealer/Phedex
POP_DB_CACHE=/usr/local/IntelROCCS/Cache/DataDealer/PopDb
RANKINGS_CACHE=/usr/local/IntelROCCS/Cache/DataDealer/Rankings

mkdir -p $DATA_DEALER_LOG

chown ${INTELROCCS_USER}:${INTELROCCS_GROUP} -R $DATA_DEALER_LOG

# copy the software
#==================

if [ -d "$DATA_DEALER_INSTALL" ]
then
	# make sure to remove completely the previous installed software
	echo " Removing previous data dealer installation."
	rm -rf $DATA_DEALER_INSTALL/*
else
	# create file structur if it doesn't exist
	mkdir -p $DATA_DEALER_INSTALL
fi
# copy all source files to install directory
cp $DATA_DEALER_SRC/* $DATA_DEALER_INSTALL
cp $DATA_DEALER_SRC/../data_dealerd $DATA_DEALER_INSTALL
touch ${DATA_DEALER_INSTALL}/__init__.py
touch ${INSTALL_DIR}/__init__.py


# create init scripts
#====================

# set up cache environment
# phedex
if [ -d "$PHEDEX_CACHE" ]
then
	# make sure to remove completely the previous installed software
	if [ $keep_cache -eq 0 ]
	then
		echo " Cleaning up phedex cache."
		rm -rf $PHEDEX_CACHE/*
	else
		echo " Phedex cache is not cleared"
	fi
fi
# create file structur if it doesn't exist
mkdir -p $PHEDEX_CACHE

# pop db
if [ -d "$POP_DB_CACHE" ]
then
	# make sure to remove completely the previous installed software
	if [ $keep_cache -eq 0 ]
	then
		echo " Cleaning up pop db cache."
		rm -rf $POP_DB_CACHE/*
	else
		echo " Pop DB cache is not cleared"
	fi
fi
# create file structur if it doesn't exist
mkdir -p $POP_DB_CACHE

# rankings
if [ -d "$RANKINGS_CACHE" ]
then
	# make sure to remove completely the previous installed software
	if [ $keep_cache -eq 0 ]
	then
		echo " Cleaning up rankings cache."
		rm -rf $RANKINGS_CACHE/*
	else
		echo " Rankings cache is not cleared"
	fi
fi
# create file structur if it doesn't exist
mkdir -p $RANKINGS_CACHE

PYTHONPATH=${INSTALL_DIR}/Api
DATA_DEALER_THRESHOLD="1" # not used right now

CACHE_DEADLINE="12"

INIT_FILE=$DATA_DEALER_INSTALL/init.py

echo "#!/usr/local/bin/python" > $INIT_FILE
echo "import sys, os" >> $INIT_FILE
echo "sys.path.append('"${PYTHONPATH}"')" >> $INIT_FILE
echo "os.environ['DATA_DEALER_PHEDEX_CACHE']='"${PHEDEX_CACHE}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_POP_DB_CACHE']='"${POP_DB_CACHE}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_CACHE_DEADLINE']='"${CACHE_DEADLINE}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_THRESHOLD']='"${DATA_DEALER_THRESHOLD}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_BUDGET']='"${DATA_DEALER_BUDGET}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_RANKINGS_CACHE']='"${RANKINGS_CACHE}"'" >> $INIT_FILE

chmod 755 $INIT_FILE

if [ $testing -eq 0 ]
then
	# install and start daemons
	#==========================

	# stop potentially existing server process
	if [ -e "/etc/init.d/data_dealerd" ]
	then
	  /etc/init.d/data_dealerd status
	  /etc/init.d/data_dealerd stop
	fi

	# copy Detox daemon
	cp ${INTELROCCS_DIR}/DataDealer/sysv/data_dealerd /etc/init.d/

	# start new server
	/etc/init.d/data_dealerd status
	/etc/init.d/data_dealerd start
	sleep 2
	/etc/init.d/data_dealerd status

	# start on boot
	chkconfig --level 345 data_dealerd on
fi

exit 0
