#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Installation script for Data Dealer package
#
# B.Barrefors (Aug 13, 2014)
# --------------------------------------------------------------------------------------------------
# get src and install paths
#==========================
# TODO -- Make it possible to change install path by passing argument

# get path of installation script, source code, and install path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATA_DEALER_SRC=${DIR}/src
INTELROCCS_DIR=${HOME}/IntelROCCS
INSTALL_DIR=${INTELROCCS_DIR}/Install
DATA_DEALER_INSTALL=${INSTALL_DIR}/DataDealer


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
touch ${DATA_DEALER_INSTALL}/__init__.py
touch ${INSTALL_DIR}/__init__.py


# install api
#============

API_INSTALL=${INSTALL_DIR}/Api
${INTELROCCS_DIR}/Api/installApi.sh

# create init scripts
#====================

# set up cache environment
# phedex
PHEDEX_CACHE=/tmp/IntelROCCS/Cache/Phedex
if [ -d "$PHEDEX_CACHE" ]
then
  	# make sure to remove completely the previous installed software
  	echo " Cleaning up phedex cache."
  	#rm -rf $PHEDEX_CACHE/*
else
	# create file structur if it doesn't exist
	mkdir -p $PHEDEX_CACHE
fi

# pop db
POP_DB_CACHE=/tmp/IntelROCCS/Cache/PopDb
if [ -d "$POP_DB_CACHE" ]
then
  	# make sure to remove completely the previous installed software
  	echo " Cleaning up pop db cache."
  	#rm -rf $POP_DB_CACHE/*
else
	# create file structur if it doesn't exist
	mkdir -p $POP_DB_CACHE
fi

PYTHONPATH=${INSTALL_DIR}/Api
DATA_DEALER_THRESHOLD="1"
DATA_DEALER_BUDGET="10000"
CACHE_DEADLINE="12"

INIT_FILE=$DATA_DEALER_INSTALL/init.py

echo "#!/usr/local/bin/python" > $INIT_FILE
echo "import sys, os" >> $INIT_FILE
echo "sys.path.append('"${PYTHONPATH}"')" >> $INIT_FILE
echo "os.environ['PHEDEX_CACHE']='"${PHEDEX_CACHE}"'" >> $INIT_FILE
echo "os.environ['POP_DB_CACHE']='"${POP_DB_CACHE}"'" >> $INIT_FILE
echo "os.environ['CACHE_DEADLINE']='"${CACHE_DEADLINE}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_THRESHOLD']='"${DATA_DEALER_THRESHOLD}"'" >> $INIT_FILE
echo "os.environ['DATA_DEALER_BUDGET']='"${DATA_DEALER_BUDGET}"'" >> $INIT_FILE

chmod 755 $INIT_FILE

# install and start daemons
#==========================

# stop potentially existing server process
#if [ -e "/etc/init.d/data_dealerd" ]
#then
#  /etc/init.d/data_dealerd status
#  /etc/init.d/data_dealerd stop
#fi

# copy Data Dealer daemon
#cp ${INTELROCCS_DIR}/DataDealer/sysv/data_dealerd /etc/init.d/

# start new server
#/etc/init.d/data_dealerd status
#/etc/init.d/data_dealerd start
#sleep 2
#/etc/init.d/data_dealerd status

# start on boot
#chkconfig --level 345 data_dealerd on

exit 0
