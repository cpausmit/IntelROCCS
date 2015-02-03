#!/bin/bash
#---------------------------------------------------------------------------------------------------
#
# This script will extract all relevant log file monitoring data from the present log files and
# prepare them for further monitoring display.
#
#                                                                             C.Paus (June 10, 2014)
#---------------------------------------------------------------------------------------------------
# check if detox package is properly setup
if [ -z "$DETOX_DB" ] || [ -z "$MONITOR_DB" ]
then
  echo " ERROR - logfile base not defined: DETOX_DB = \"$DETOX_DB\"  MONITOR_DB = \"$MONITOR_DB\""
  exit 0
fi

# make files and run root in good location
mkdir -p  $MONITOR_DB
cp .root* $MONITOR_DB
cd        $MONITOR_DB

# are we interested in nSites or nSitesAv
average=1
if [ -z $1 ]
then
  average="1"
else
  average=$1
fi

# define relevant environment variables
export MIT_ROOT_STYLE=/home/paus/MitRootStyle

RUN_AGGREGATION="yes" # turn off if you just want to make plots
REPRODUCE="no"

if [ "$REPRODUCE" == "yes" ]
then
  export INCLUDE_DELETED='no'
  export PRORATE_REPLICAS='no'
  export FILL_BY_REPLICAS='no'
else
  export INCLUDE_DELETED='yes'
  export PRORATE_REPLICAS='yes'
  export FILL_BY_REPLICAS='yes'
fi

# Setting up the paramameters
end=$(date --date=12/31/2014 +%s)
starts="$(date --date=10/01/2014 +%s):03M $(date --date=07/01/2014 +%s):06M $(date --date=01/01/2014 +%s):12M"
datasetPatterns="_AOD_ _AOD _AODSIM _MINIAOD_"

# Performing the loop over all setups
for startPar in `echo $starts`
do
  start=`echo $startPar|cut -d':' -f1`
  timePeriod=`echo $startPar|cut -d':' -f2`

  # make sure to determine the length of the interval
  let interval=end-start
  
  for datasetPattern in `echo $datasetPatterns`
  do

    echo ""
    echo " --------  N E X T   P L O T  -------- "
    echo ""
    echo " start:    $start "
    echo " interval: $interval "
    echo " period:   $timePeriod "
    echo " pattern:  $datasetPattern "
    echo ""

    # decide whether data agreggation is needed
    export DATASET_MONITOR_TEXT=$timePeriod
    export MONITOR_PATTERN=$datasetPattern

    if [ $RUN_AGGREGATION == "yes" ]
    then
      $MONITOR_BASE/readJsonSnapshotCRB.py T2* $start $end
      mv DatasetSummary.txt     data-${MONITOR_PATTERN}-${DATASET_MONITOR_TEXT}.txt
      mv DatasetSummaryByDS.txt dataByDS-${MONITOR_PATTERN}-${DATASET_MONITOR_TEXT}.txt
    fi
    
    if [ $FILL_BY_REPLICAS == "yes" ]
    then 
      export DATASET_MONITOR_FILE=data-${MONITOR_PATTERN}-${DATASET_MONITOR_TEXT}
    else
      export DATASET_MONITOR_FILE=dataByDS-${MONITOR_PATTERN}-${DATASET_MONITOR_TEXT}
    fi
    
    echo "Analyze file: " $DATASET_MONITOR_FILE
    echo " "
    echo " "
    root -q -b -l $MONITOR_BASE/plotCRB.C+\("$average",$interval\)

  done
done

# synthesize all monitoring data

rm -f crbData.csv
touch crbData.csv
echo ""
for datasetPattern in `echo $datasetPatterns`
do
  echo "Pattern -- $datasetPattern " >> crbData.csv
  echo "bins,0 old,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,>14" >> crbData.csv
  echo "Pattern -- $datasetPattern "
  echo "bins,0 old,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,>14"
  cat data-$datasetPattern-*.dat         >> crbData.csv
  cat data-$datasetPattern-*.dat
done

# finally make all the plots available
cp *.csv ~/public_html/CRB/
cp *.png ~/public_html/CRB/

exit 0
