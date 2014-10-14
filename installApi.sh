#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Installation script for Api package
#
# B.Barrefors (Aug 13, 2014)
# --------------------------------------------------------------------------------------------------
# get src and install paths
#==========================
# TODO -- Make it possible to change install path by passing argument

# get path of installation script, source code, and install path
INTELROCCS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
API_SRC=${INTELROCCS_DIR}/Api/src
INSTALL_DIR=${INTELROCCS_DIR}/Install
API_INSTALL=${INSTALL_DIR}/Api


# copy the software
#==================

if [ -d "$API_INSTALL" ]
then
  	# make sure to remove completely the previous installed software
  	echo " Removing previous api installation."
  	rm -rf $API_INSTALL/*
else
	# create file structur if it doesn't exist
	mkdir -p $API_INSTALL
fi
# copy all source files to install directory
cp $API_SRC/* $API_INSTALL
touch ${API_INSTALL}/__init__.py


# create init scripts
#====================

# mit db variables

DB_SERVER="t3btch039.mit.edu"
DB_DB="IntelROCCS"
DB_USER="cmsSiteDb"
DB_PW="db78user?Cms"

DB_INIT_FILE=$API_INSTALL/initDb.py

echo "#!/usr/local/bin/python" > $DB_INIT_FILE
echo "import sys, os" >> $DB_INIT_FILE
echo "os.environ['DB_SERVER']='"${DB_SERVER}"'" >> $DB_INIT_FILE
echo "os.environ['DB_DB']='"${DB_DB}"'" >> $DB_INIT_FILE
echo "os.environ['DB_USER']='"${DB_USER}"'" >> $DB_INIT_FILE
echo "os.environ['DB_PW']='"${DB_PW}"'" >> $DB_INIT_FILE

chmod 755 $DB_INIT_FILE

# phedex variables

PHEDEX_BASE="https://cmsweb.cern.ch/phedex/datasvc"

PHEDEX_INIT_FILE=$API_INSTALL/initPhedex.py

echo "#!/usr/local/bin/python" > $PHEDEX_INIT_FILE
echo "import sys, os" >> $PHEDEX_INIT_FILE
echo "os.environ['PHEDEX_BASE']='"${PHEDEX_BASE}"'" >> $PHEDEX_INIT_FILE

chmod 755 $PHEDEX_INIT_FILE

# pop db variables

POP_DB_BASE="https://cms-popularity.cern.ch/popdb/popularity"

POP_DB_INIT_FILE=$API_INSTALL/initPopDb.py

echo "#!/usr/local/bin/python" > $POP_DB_INIT_FILE
echo "import sys, os" >> $POP_DB_INIT_FILE
echo "os.environ['POP_DB_BASE']='"${POP_DB_BASE}"'" >> $POP_DB_INIT_FILE

chmod 755 $POP_DB_INIT_FILE
