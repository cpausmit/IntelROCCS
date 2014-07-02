#!/bin/bash
# --------------------------------------------------------------------------------------------------
#
# Installation script for IntelROCCS. There will be lots of things to test and to fix, but this is
# the starting point. This installation has to be performed as user root.
#
#                                                                Ch.Paus: Version 0.0 (Mar 09, 2014)
# --------------------------------------------------------------------------------------------------
# Configuration parameters (this needs more work but for now)
export INTELROCCS_USER=cmsprod
export INTELROCCS_GROUP=zh

source Detox/setupDetox.sh
source Detox/setupMonitor.sh

# make sure mysql is setup properly for server and clients otherwise this will not work
# check out the README

# General installation (you have to be in the directory of install script and you have to be root)

INTELROCCS_BASE=`dirname $DETOX_BASE`
TRUNC=`dirname $INTELROCCS_BASE`

# copy the software
#==================

if [ -d "$INTELROCCS_BASE" ]
then
  # make sure to remove completely the previous installed software
  echo " Removing previous installation."
  rm -rf "$INTELROCCS_BASE"
fi
cp -r ../IntelROCCS $TRUNC


# create log/db structure
#========================

# owner has to be $INTELROCCS_USER:$INTELROCCS_GROUP, this user runs the process
INTELROCCS_DB=`dirname $DETOX_DB`
mkdir -p $INTELROCCS_DB $DETOX_DB $MONITOR_DB
chown ${INTELROCCS_USER}:${INTELROCCS_GROUP} -R $INTELROCCS_DB


# install and start daemons
#==========================

# stop potentially existing server process
if [ -e "/etc/init.d/detoxd" ]
then
  /etc/init.d/detoxd stop
fi

# copy Detox daemon
cp /usr/local/IntelROCCS/Detox/sysv/detoxd /etc/init.d/

# start new server
/etc/init.d/detoxd status
/etc/init.d/detoxd start
sleep 2
/etc/init.d/detoxd status

# start on boot
chkconfig --level 345 detoxd on

# stop potentially existing server process
if [ -e "/etc/init.d/monitord" ]
then
  /etc/init.d/monitord stop
fi

# copy Monitor daemon
cp /usr/local/IntelROCCS/Monitor/sysv/monitord /etc/init.d/

# start new server
/etc/init.d/monitord status
/etc/init.d/monitord start
sleep 2
/etc/init.d/monitord status

# start on boot
chkconfig --level 345 monitord on

exit 0
