# main directories

export MONITOR_DB="/local/cmsprod/IntelROCCS/Monitor"
export MONITOR_BASE="/usr/local/IntelROCCS/Monitor"

# Parameters for monitoring

export MONITOR_CYCLE_HOURS=1
export MONITOR_THREADS=1

# Certificate location

export MONITOR_X509UP=/tmp/x509up_u5410

# Local logging database config file

export MONITOR_MYSQL_CONFIG=/etc/my.cnf

# Paths

if [ "`echo $PATH | grep /usr/local/bin`" == "" ]
then
  export PATH="/usr/local/bin:${PATH}"
fi

export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH}"

if [ -z "$PYTHONPATH" ]
then
  export PYTHONPATH="/usr/lib64/python2.6/:/usr/lib64/python2.6/site-packages"
fi
export PYTHONPATH="/usr/lib64/python2.6/:/usr/lib64/python2.6/site-packages"
export USERKEY="/home/${USER}/.globus/userkey.pem"
export USERCERT="/home/${USER}/.globus/usercert.pem"


