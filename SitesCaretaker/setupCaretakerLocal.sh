#---------------------------------------------------------------------------------------------------
#
# Here the main parameters for Detox are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#---------------------------------------------------------------------------------------------------
# main directories
export CARETAKER_DB="/home/maxi/IntelROCCS/SitesCaretaker"
export CARETAKER_SITESTORAGE_SERVER="t3btch039.mit.edu"
export CARETAKER_SITESTORAGE_DB="IntelROCCS"
export CARETAKER_SITESTORAGE_USER="cmsSiteDb"
export CARETAKER_SITESTORAGE_PW="db78user?Cms"

# sub directories
export CARETAKER_TRDIR="transfers"

# location of Detox web 
export CARETAKER_DETOXWEB="http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox"

# Email list to notify in case of problems
export CARETAKER_EMAIL_LIST=maxi@fnal.gov,maxi@mit.edu

# Certificate location
export CARETAKER_X509UP=/tmp/x509up_u`id -u`

# Paths
export CARETAKER_BASE=`pwd`
export CARETAKER_PYTHONPATH="$CARETAKER_BASE/python"

# Python path etc. (careful it might not be set)
export PYTHONPATH="${CARETAKER_PYTHONPATH}:$PYTHONPATH"
