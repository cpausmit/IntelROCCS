#!/bin/bash
# --------------------------------------------------------------------------------------------------
#
# This script will extract all relevant log file monitoring data from the present log files and
# prepare them for further monitoring display.
#
#                                                                             C.Paus (June 10, 2014)
# --------------------------------------------------------------------------------------------------
# check if detox package is properly setup
if [ -z "$DETOX_DB" ] || [ -z "$MONITOR_DB" ]
then
  echo " ERROR - logfile base not defined: DETOX_DB = \"$DETOX_DB\"  MONITOR_DB = \"$MONITOR_DB\""
  exit 0
fi

mkdir -p    $MONITOR_DB
cd $MONITOR_DB #preventing temp files from being created in weird places

# are we interested in nSites or nSitesAv
average=1
if [ -z $1 ]
then
  average="1"
else
  average=$1
fi
# define relevant environment variables
export MIT_ROOT_STYLE=/home/cmsprod/MitRootStyle/MitRootStyle.C
export INCLUDE_DELETED='yes'
export PRORATE_REPLICAS='yes'
export FILL_BY_REPLICAS='yes'

RUN_AGGREGATION='yes' # turn off if you just want to make plots

begin=$(date --date=10/01/2014 +%s)
end=$(date --date=12/31/2015 +%s)
let interval=end-begin
export DATASET_MONITOR_TEXT="CRB_3MONTHS"
echo "Period "${DATASET_MONITOR_TEXT}
if [[ $RUN_AGGREGATION == "yes" ]]; then
  $MONITOR_BASE/readJsonSnapshotCRB.py T2* $begin $end
  mv DatasetSummary.txt DatasetSummary${DATASET_MONITOR_TEXT}.txt
  mv DatasetSummaryByDS.txt DatasetSummaryByDS${DATASET_MONITOR_TEXT}.txt
fi
  if [[ $FILL_BY_REPLICAS == "yes" ]]; then 
    export DATASET_MONITOR_FILE=DatasetSummary${DATASET_MONITOR_TEXT}
  else
    export DATASET_MONITOR_FILE=DatasetSummaryByDS${DATASET_MONITOR_TEXT}
  fi
  echo $DATASET_MONITOR_FILE
root -q -b -l $MONITOR_BASE/plotCRB.C\("$average",$interval\)

begin=$(date --date=07/01/2014 +%s)
end=$(date --date=12/31/2015 +%s)
let interval=end-begin
export DATASET_MONITOR_TEXT="CRB_6MONTHS"
echo "Period "${DATASET_MONITOR_TEXT}
if [[ $RUN_AGGREGATION == "yes" ]]; then
  $MONITOR_BASE/readJsonSnapshotCRB.py T2* $begin $end
  mv DatasetSummary.txt DatasetSummary${DATASET_MONITOR_TEXT}.txt
  mv DatasetSummaryByDS.txt DatasetSummaryByDS${DATASET_MONITOR_TEXT}.txt
fi
  if [[ $FILL_BY_REPLICAS == "yes" ]]; then 
    export DATASET_MONITOR_FILE=DatasetSummary${DATASET_MONITOR_TEXT}
  else
    export DATASET_MONITOR_FILE=DatasetSummaryByDS${DATASET_MONITOR_TEXT}
  fi
  echo $DATASET_MONITOR_FILE
root -q -b -l $MONITOR_BASE/plotCRB.C\("$average",$interval\)


begin=$(date --date=01/01/2014 +%s)
end=$(date --date=12/31/2014 +%s)
let interval=end-begin
export DATASET_MONITOR_TEXT="CRB_12MONTHS"
echo "Period "${DATASET_MONITOR_TEXT}
if [[ $RUN_AGGREGATION == "yes" ]]; then
  $MONITOR_BASE/readJsonSnapshotCRB.py T2* $begin $end
  mv DatasetSummary.txt DatasetSummary${DATASET_MONITOR_TEXT}.txt
  mv DatasetSummaryByDS.txt DatasetSummaryByDS${DATASET_MONITOR_TEXT}.txt
fi
  if [[ $FILL_BY_REPLICAS == "yes" ]]; then 
    export DATASET_MONITOR_FILE=DatasetSummary${DATASET_MONITOR_TEXT}
  else
    export DATASET_MONITOR_FILE=DatasetSummaryByDS${DATASET_MONITOR_TEXT}
  fi
  echo $DATASET_MONITOR_FILE
root -q -b -l $MONITOR_BASE/plotCRB.C\("$average",$interval\)

exit 0
