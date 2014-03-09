#!/bin/bash

# deal with command line argument
if [ "$1" == "" ]
then
  echo " specify protected web site as first parameter."
  exit 0
fi

PROTECTED_SITE="$1"

# make sure there is a valid kerberos ticket
klist -s
if [ $? != 0 ]
then
  kinit -f
fi

# make a temporary space to store our cookies
mkdir -p ~/.tmp

# create the cookie file (using kerberos ticket)
cern-get-sso-cookie --krb -u "$PROTECTED_SITE" -o ~/.tmp/ssocookie.txt

# access the protected site
wget --load-cookies ~/.tmp/ssocookie.txt "$PROTECTED_SITE" -O -

exit 0;
