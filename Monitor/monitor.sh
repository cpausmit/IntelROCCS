#!/bin/bash
# --------------------------------------------------------------------------------------------------
#
# This script will extract all relevant log file monitoring data from the present log files and
# prepare them for further monitoring display.
#
#                                                        C.Paus, S. Narayanan  ( 2015 February 11)
# --------------------------------------------------------------------------------------------------
# check if detox package is properly setup
if [ -z "$DETOX_DB" ] || [ -z "$MONITOR_DB" ]
then
  echo " ERROR - logfile base not defined: DETOX_DB = \"$DETOX_DB\"  MONITOR_DB = \"$MONITOR_DB\""
  exit 0
fi

mkdir -p    $MONITOR_DB
cd $MONITOR_DB #preventing temp files from being created in weird places
rm -f $MONITOR_DB/*png # clean up old images, will be replaced shortly
# are we interested in nSites or nSitesAv
# define relevant environment variables
export MIT_ROOT_STYLE=/home/cmsprod/MitRootStyle/MitRootStyle.C
export SITE_MONITOR_FILE=$MONITOR_DB/MonitorSummary.txt
rm -f $SITE_MONITOR_FILE
touch $SITE_MONITOR_FILE
#  echo "# Site Quota Used ToDelete LastCp" >> $SITE_MONITOR_FILE

echo ""
echo "Extracting log file monitoring data from DETOX_DB = $DETOX_DB."
echo ""

# find present site quotas
for site in `ls -1 $DETOX_DB/$DETOX_RESULT | grep ^T[0-3]`
do

#  echo " Analyzing site : $site"
  quota=`grep 'Total Space' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' ' | head -n 1`
  used=`grep 'Space Used' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' ' | head -n 1`
  toDelete=`grep 'Space to delete' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' ' | head -n 1`
  lastCp=`grep 'Space last CP' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' ' | head -n 1`
  echo "$site $quota $used $toDelete $lastCp" >> $SITE_MONITOR_FILE

done

# make nice histograms
pwd
 root -q -b -l $MONITOR_BASE/plotSites.C
 $MONITOR_BASE/plotSites.py
echo "Done making site plots"

cd $MONITOR_BASE

./monitor.py 

# copy things for plotting interactively
# rsync will put them on the web 
cp findDatasetHistoryAll.py ${MONITOR_DB}
cp findDatasetProperties.py ${MONITOR_DB}
cp Dataset.py ${MONITOR_DB}
cp plotFromPickle.py ${MONITOR_DB}

exit 0
