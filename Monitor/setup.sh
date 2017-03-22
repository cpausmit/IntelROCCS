# main directories

export MONITOR_DB=${PWD}"/db/"

# Parameters for monitoring

export MONITOR_CYCLE_HOURS=1

# Certificate location

export MONITOR_X509UP=/tmp/x509up_u5410

# Local logging database config file

export MONITOR_MYSQL_CONFIG=/etc/my.cnf

# Paths

export MONITOR_BASE=${PWD}
export MONITOR_PYTHONPATH="$DETOX_BASE/python"

# Python path etc. (careful it might not be set)

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
export PYTHONPATH="${MONITOR_PYTHONPATH}:$PYTHONPATH"
export USERKEY="/home/${USER}/.globus/userkey.pem"
export USERCERT="/home/${USER}/.globus/usercert.pem"


export MONITOR_THREADS=8
