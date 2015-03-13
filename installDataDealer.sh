#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Installation script for Data Dealer package
#
# B.Barrefors
# --------------------------------------------------------------------------------------------------
set -e

# get command line arguments
#===========================
testing=0  # install data dealer in test folder
daemon=0   # install new daemon and restart it

while getopts "dth" opt; do
    case $opt in
    d) daemon=1
       ;;
    t) testing=1
       ;;
    h) echo "usage: sudo ./installDataDealer [-d | -h]"
       echo "d: install a new daemon and restart it"
       echo "t: install in test folder"
       echo "h: show this message"
       exit 0
       ;;
    esac
done


# safely grab variables from config file
#=======================================
# get path to this script and config files
intelroccs_path=$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )
configfile=$intelroccs_path/DataDealer/data_dealer.conf
configfile_secured=/etc/data_dealer.conf
if [ $testing -eq 1 ]
then
    configfile=$intelroccs_path/DataDealer/data_dealer_test.conf
    configfile_secured=/etc/data_dealer_test.conf
fi

# check if the file contains something we don't want
if egrep -q -v '^#|^[^ ]*=[^;]*' "$configfile";
then
    # filter the original to a new file
    egrep '^#|^[^ ]*=[^;&]*'  "$configfile" > $configfile_secured
fi

# fetch variables from config file
source $configfile_secured


# get src and install paths
#==========================
data_dealer_src=$intelroccs_path/DataDealer/src
api_src=$intelroccs_path/Api/src


# copy the software
#==================
# check and fix folder structure for source code
if [ -d "$install_path" ]
then
    # make sure to completely remove the previous installed software
    echo " Removing previous data dealer installation in $install_path"
    rm -rf "$install_path/"*".py"
else
    # create file structur if it doesn't exist
    mkdir -p "${install_path}"
fi

# check and fix folder structure for data
for i in "${data_folders[@]}"
do
    if [ -d "$data_path/$i" ]
    then
        echo " $i already exists"
    else
        mkdir -p "$data_path/$i"
    fi
done

# copy all source files to install directory
cp "$data_dealer_src/"* "$install_path"
cp "$api_src/"* "$install_path"
cp "$data_dealer_src/../html/"* "$data_path/Visualizations"
if [ $testing -eq 1 ]
then
    cp "$data_dealer_src/../data_dealer-test.cfg" "$install_path/data_dealer.cfg"
    cp "$data_dealer_src/../api-test.cfg" "$install_path/api.cfg"
    cp "$data_dealer_src/../manual_injection-test.cfg" "$install_path/manual_injection.cfg"
else
    cp "$data_dealer_src/../data_dealer.cfg" "$install_path/data_dealer.cfg"
    cp "$data_dealer_src/../api.cfg" "$install_path/api.cfg"
    cp "$data_dealer_src/../manual_injection.cfg" "$install_path/manual_injection.cfg"
fi

# make sure permissions are correct
chown -R "$intelroccs_user:$intelroccs_group" "$data_path"


# install and start daemons
#==========================
if [ $daemon -eq 1 ]
then
    set +e
    if [ $testing -eq 1 ]
    then
        # stop potentially existing server process
        if [ -e "/etc/init.d/data_dealer_testd" ]
        then
            /etc/init.d/data_dealer_testd status
            /etc/init.d/data_dealer_testd stop
        fi

        # copy data_dealer daemon
        cp "$data_dealer_src/../data_dealer_testd" "$install_path"
        cp "$data_dealer_src/../sysv/data_dealer_testd" /etc/init.d/

        # start new daemon
        /etc/init.d/data_dealer_testd status
        /etc/init.d/data_dealer_testd start
        sleep 2
        /etc/init.d/data_dealer_testd status

        # start on boot
        chkconfig --level 345 data_dealer_testd on
    else
        # stop potentially existing server process
        if [ -e "/etc/init.d/data_dealerd" ]
        then
            /etc/init.d/data_dealerd status
            /etc/init.d/data_dealerd stop
        fi

        # copy data_dealer daemon
        cp "$data_dealer_src/../data_dealerd" "$install_path"
        cp "$data_dealer_src/../sysv/data_dealerd" /etc/init.d/

        # start new daemon
        /etc/init.d/data_dealerd status
        /etc/init.d/data_dealerd start
        sleep 2
        /etc/init.d/data_dealerd status

        # start on boot
        chkconfig --level 345 data_dealerd on
    fi   
fi

# done
echo " Data Dealer successfully installed"
exit 0
