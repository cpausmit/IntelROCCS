#---------------------------------------------------------------------------------------------------
#
# Here the main parameters for Detox are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#---------------------------------------------------------------------------------------------------
# main directories

export DETOX_DB="/home/maxi/IntelROCCS/Detox"

export DETOX_SITESTORAGE_SERVER="t3btch039.mit.edu"
export DETOX_SITESTORAGE_DB="IntelROCCS"
export DETOX_SITESTORAGE_USER="cmsSiteDb"
export DETOX_SITESTORAGE_PW="db78user?Cms"

# sub directories

export DETOX_STATUS="status"
export DETOX_SNAPSHOTS="snapshots"
export DETOX_RESULT="result"
export DETOX_MONITOR="monitor"

# standard filenames

export DETOX_PHEDEX_CACHE=DatasetsInPhedexAtSites.dat
export DETOX_USED_DATASETS=UsedDatasets.txt
export DETOX_DATASETS_TO_DELETE=RankedDatasets.txt


# PhEDEx group that is considered 
export DETOX_GROUP='AnalysisOps','DataOps','caf-comm','caf-alca'

#Lock file locations for DataOps group

export DETOX_LOCKFILES=\
'https://cmst2.web.cern.ch/cmst2/unified/datalocks.json',\
'https://cmst1.web.cern.ch/CMST1/lockedData/lockTestSamples.json'

# Parameters for cleaning

export DETOX_CYCLE_HOURS=6
export DETOX_USAGE_MAX=0.90
export DETOX_USAGE_MIN=0.85
export DETOX_NCOPY_MIN=1

# What time window (in months) will be used for counting usage

export DETOX_TIME_WIND=12
export DETOX_APP_TDECAY=0

# Email list to notify in case of problems

export DETOX_EMAIL_LIST=maxi@fnal.gov

# Certificate location

export DETOX_X509UP=/tmp/x509up_u`id -u`

# Local logging database config file
#export DETOX_MYSQL_CONFIG=/etc/myIntelROCCS.cnf

# Paths

export DETOX_BASE=/home/maxi/IntelROCCS/Detox
export DETOX_PYTHONPATH="$DETOX_BASE/python"

# Python path etc. (careful it might not be set)

#if [ "`echo $PATH | grep /usr/local/bin`" == "" ]
#then
#  export PATH="/usr/local/bin:${PATH}"
#fi

#export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH}"

#if [ -z "$PYTHONPATH" ]
#then
#  export PYTHONPATH="/usr/local/lib/python2.7:/usr/local/lib/python2.7/site-packages"
#fi
export PYTHONPATH="${DETOX_PYTHONPATH}:$PYTHONPATH"
