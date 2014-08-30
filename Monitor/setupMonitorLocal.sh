#---------------------------------------------------------------------------------------------------
#
# Here the main parameters for Monitor are defined. This is probably not the way we want to do it on
# the long run but let's start this way.
#
#---------------------------------------------------------------------------------------------------
# main directories

export MONITOR_DB="$HOME/log/IntelROCCS/Monitor"

export MONITOR_SITESTORAGE_SERVER="t3btch039.mit.edu"
export MONITOR_SITESTORAGE_DB="IntelROCCS"
export MONITOR_SITESTORAGE_USER="cmsSiteDb"
export MONITOR_SITESTORAGE_PW="db78user?Cms"

# Parameters for monitoring

export MONITOR_CYCLE_HOURS=1

# Certificate location

export MONITOR_X509UP=/tmp/x509up_u5410

# Local logging database config file

export MONITOR_MYSQL_CONFIG=/etc/myIntelROCCS.cnf

# Paths

export MONITOR_BASE=`pwd`
export MONITOR_PYTHONPATH="$MONITOR_BASE/python"

# Python path etc. (careful it might not be set)

if [ "`echo $PATH | grep /usr/local/bin`" == "" ]
then
  export PATH="/usr/local/bin:${PATH}"
fi

export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH}"

if [ -z "$PYTHONPATH" ]
then
  export PYTHONPATH="/usr/local/lib/python2.7:/usr/local/lib/python2.7/site-packages"
fi
export PYTHONPATH="${MONITOR_PYTHONPATH}:$PYTHONPATH"
