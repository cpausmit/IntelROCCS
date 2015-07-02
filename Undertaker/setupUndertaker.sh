#---------------------------------------------------------------------------------------------------
#
# Here the main parameters for Detox are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#---------------------------------------------------------------------------------------------------
# main directories
export UNDERTAKER_DB="/local/cmsprod/IntelROCCS/Undertaker"
export UNDERTAKER_SITESTORAGE_SERVER="t3btch039.mit.edu"
export UNDERTAKER_SITESTORAGE_DB="IntelROCCS"
export UNDERTAKER_SITESTORAGE_USER="cmsSiteDb"
export UNDERTAKER_SITESTORAGE_PW="db78user?Cms"

# sub directories
export UNDERTAKER_TRDIR="transfers"

# location of Detox web 
export UNDERTAKER_DETOXWEB="http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox"

# Email list to notify in case of problems
export UNDERTAKER_EMAIL_LIST=maxi@fnal.gov,maxi@mit.edu

# Certificate location
export UNDERTAKER_X509UP=/tmp/x509up_u`id -u`

# Paths
export UNDERTAKER_BASE="/usr/local/IntelROCCS/Undertaker"
export UNDERTAKER_PYTHONPATH="$UNDERTAKER_BASE/python"

#how often it runs
export UNDERTAKER_CYCLE_HOURS=12

# Python path etc. (careful it might not be set)
export PYTHONPATH="${UNDERTAKER_PYTHONPATH}:$PYTHONPATH"
