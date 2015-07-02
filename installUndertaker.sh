#!/bin/bash
# --------------------------------------------------------------------------------------------------
#
# Installation script for IntelROCCS/Undertaker. There will be lots of things to test and to fix, but
# this is the starting point. This installation has to be performed as user root.
#
#                                                        M. Goncharov: Version 0.0 (Oct 14, 2014)
# --------------------------------------------------------------------------------------------------
# Configuration parameters (this needs more work but for now)
export INTELROCCS_USER=cmsprod
export INTELROCCS_GROUP=zh

source SitesCaretaker/setupUndertaker.sh

# make sure mysql is setup properly for server and clients otherwise this will not work check out
# the README

# General installation (you have to be in the directory of install script and you have to be root)

TRUNC=`dirname $UNDERTAKER_BASE`

# copy the software
#==================

if [ -d "$UNDERTAKER_BASE" ]
then
  # make sure to remove completely the previous installed software
  echo " Removing previous installation."
  rm -rf "$UNDERTAKER_BASE"
fi
cp -r ../IntelROCCS/Undertaker $TRUNC

# create log/db structure
#========================
# owner has to be $INTELROCCS_USER:$INTELROCCS_GROUP, this user runs the process
mkdir -p $UNDERTAKER_DB
chown ${INTELROCCS_USER}:${INTELROCCS_GROUP} -R $UNDERTAKER_DB


# install and start daemons
#==========================

# stop potentially existing server process
if [ -e "/etc/init.d/caretakerd" ]
then
  /etc/init.d/undertakerd status
  /etc/init.d/undertakerd stop
fi

# copy Detox daemon
cp /usr/local/IntelROCCS/Undertaker/sysv/undertakerd /etc/init.d/

# start new server
/etc/init.d/undertakerd status
/etc/init.d/undertakerd start
sleep 2
/etc/init.d/undertakerd status

# start on boot
chkconfig --level 345 undertakerd on

exit 0
