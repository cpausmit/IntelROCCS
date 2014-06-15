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
export SITE_MONITOR_FILE=$DETOX_DB/monitor/MonitorSummary.txt
rm -f $SITE_MONITOR_FILE
touch $SITE_MONITOR_FILE
#echo "# Site Quota Used ToDelete LastCp" >> $SITE_MONITOR_FILE

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
  echo "$site $quota $used $toDelete $lastCp" >> $SITE_MONITOR_FILE

done

# make nice histograms
root -q -b -l plotSites.C

# extract dataset info
$DETOX_BASE/../Monitor/readJsonSnapshot.py T2*
mv DatasetSummary.txt DatasetSummaryAll.txt
export DATASET_MONITOR_TEXT="since 07/2013"
export DATASET_MONITOR_FILE=DatasetSummaryAll
root -q -b -l plotDatasets.C

$DETOX_BASE/../Monitor/readJsonSnapshot.py T2* 2014*
mv DatasetSummary.txt DatasetSummary2014.txt
export DATASET_MONITOR_TEXT="Summary 2014"
export DATASET_MONITOR_FILE=DatasetSummary2014
root -q -b -l plotDatasets.C

for period in `echo 01 02 03 04 05 06`
do
  $DETOX_BASE/../Monitor/readJsonSnapshot.py T2* 2014-$period*
  mv     DatasetSummary.txt DatasetSummary${period}-2014.txt
  export DATASET_MONITOR_TEXT="${period}/2014"
  export DATASET_MONITOR_FILE=DatasetSummary${period}-2014
  root -q -b -l plotDatasets.C
done

# move the results to the log file area
mkdir -p    $DETOX_DB/$DETOX_MONITOR
mv    *.txt $DETOX_DB/$DETOX_MONITOR
mv    *.png $DETOX_DB/$DETOX_MONITOR

exit 0
