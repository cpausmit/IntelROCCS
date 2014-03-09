#----------------------------------------------------------------------------------------------------
#
# Here the main parameters for Detox are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#----------------------------------------------------------------------------------------------------
# main directories

export DETOX_DB="/local/cmsprod/IntelROCCS/Detox"

export DETOX_SITESTORAGE_SERVER="t3btch039.mit.edu"
export DETOX_SITESTORAGE_DB="SiteStorage"
export DETOX_SITESTORAGE_USER="cmsSiteDb"
export DETOX_SITESTORAGE_PW="db78user?Cms"

# sub directories

export DETOX_QUOTAS="Quotas"
export DETOX_STATUS="status"
export DETOX_SNAPSHOTS="snapshots"
export DETOX_RESULT="result"

# standard filenames

export DETOX_EXCLUDED_SITES=ExcludedSites.txt
export DETOX_PHEDEX_CACHE=DatasetsInPhedexAtSites.dat
export DETOX_USED_DATASETS=UsedDatasets.txt
export DETOX_DATASETS_TO_DELETE=DatasetsToDelete.txt

# Parameters for cleaning

export DETOX_USAGE_MAX=0.75
export DETOX_USAGE_MIN=0.65
export DETOX_NCOPY_MIN=2

# Certificate location

export DETOX_X509UP=/tmp/x509up_u`id -u`

# Paths

export DETOX_BASE="/home/cmsprod/cms/IntelROCCS/Detox"
export DETOX_PYTHONPATH="$DETOX_BASE/python"

# Python path (careful it might not be set)

if [ -z "$PTHONPATH" ] 
then
  export PYTHONPATH="/usr/lib64/python2.4:/usr/lib64/python2.4/site-packages:/usr/lib/python2.4:/usr/lib/python2.4/site-packages"
fi
export PYTHONPATH="${DETOX_PYTHONPATH}:$PYTHONPATH"
