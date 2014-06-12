#!/bin/bash
# --------------------------------------------------------------------------------------------------
#
# This script will extract all relevant log file monitroing data from the present log files and
# prepare them for further monitoring.
#
#                                                                             C.Paus (June 10, 2014)
# --------------------------------------------------------------------------------------------------
# check if detox package is properly setup
if [ -z "$DETOX_DB" ]
then
  echo " ERROR - logfile base is not defined: DETOX_DB = \"$DETOX_DB\""
  exit 0
fi

# define relevant environment variables
export MIT_ROOT_STYLE=/home/cmsprod/MitRootStyle/MitRootStyle.C
export MONITOR_FILE=$DETOX_DB/MonitorSummary.txt
rm -f $MONITOR_FILE
touch $MONITOR_FILE
#echo "# Site Quota Used ToDelete LastCp" >> $MONITOR_FILE

echo ""
echo " Extracting log file monitoring data from DETOX_DB = $DETOX_DB."
echo ""

# find present site quotas
for site in `ls -1 $DETOX_DB/$DETOX_RESULT | grep ^T[0-3]`
do

  #echo " Analyzing site : $site"
  quota=`grep 'Total Space' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' '`
  used=`grep 'Space Used' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' '`
  toDelete=`grep 'Space to delete' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' '`
  lastCp=`grep 'Space last CP' $DETOX_DB/$DETOX_RESULT/$site/Summary.txt|cut -d: -f2|tr -d ' '`
  echo "$site $quota $used $toDelete $lastCp" >> $MONITOR_FILE

done

# show our raw data
cat $MONITOR_FILE
echo ""

# make nice histograms
root -q -b -l plot.C

# move them tot he log file area
mkdir -p    $DETOX_DB/$DETOX_MONITOR
mv    *.png $DETOX_DB/$DETOX_MONITOR

exit 0
