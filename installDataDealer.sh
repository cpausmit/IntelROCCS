#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Installation script for Data Dealer package
#
# B.Barrefors (Aug 13, 2014)
# --------------------------------------------------------------------------------------------------
# Reset in case getopts has been used previously in the shell
OPTIND=1

# Initialize our own variables:
clear_cache=0
daemon=0
install_path=/usr/local/IntelROCCS/DataDealer

while getopts "dh" opt; do
    case "$opt" in
    d) daemon=1
       ;;
    h) echo "usage: sudo ./installDataDealer [-d | -h]"
       echo "d: install a new daemon and restart it"
       echo "h: show this message"
       exit 0
       ;;
    esac
done

# Configuration parameters
export INTELROCCS_USER=cmsprod
export INTELROCCS_GROUP=zh

# get src and install paths
#==========================

# get path of installation script, source code, and install path
intelroccs_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
data_dealer_src=${intelroccs_path}/DataDealer/src
api_src=${intelroccs_path}/Api/src



# copy the software
#==================

if [ -d "$install_path" ]
then
    # make sure to remove completely the previous installed software
    echo " Removing previous data dealer installation."
    rm -rf $install_path/*
else
    # create file structur if it doesn't exist
    mkdir -p $install_path
fi
# copy all source files to install directory
cp $data_dealer_src/* $install_path
cp $api_src/* $install_path
cp $data_dealer_src/../data_dealerd $install_path
cp $data_dealer_src/../data_dealer.cfg $install_path/intelroccs.cfg
cp $data_dealer_src/../html/* /local/cmsprod/IntelROCCS/DataDealer/Demo
chown ${INTELROCCS_USER}:${INTELROCCS_GROUP} /local/cmsprod/IntelROCCS/DataDealer/Demo/*

if [ $daemon -eq 1 ]
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
    cp ${intelroccs_path}/DataDealer/sysv/data_dealerd /etc/init.d/

    # start new server
    /etc/init.d/data_dealerd status
    /etc/init.d/data_dealerd start
    sleep 2
    /etc/init.d/data_dealerd status

    # start on boot
    chkconfig --level 345 data_dealerd on
fi

exit 0
