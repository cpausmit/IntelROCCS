#!/usr/local/bin/python
"""
File       : utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions
"""

# system modules
import os

def check_tool(tool, debug=0):
    """
    Check if a command line tool exists
    """
    for _dir in os.environ['PATH'].split(':'):
        tool_path = os.path.join(_dir, tool)
        if os.path.exists(tool_path):
            if debug:
                print "Command line tool %s exists in %s" % (tool, tool_path)
            return True
    else:
        print "Command line tool %s not found" % (tool)
        return False

def get_key_cert(debug=0):
    """
    Get user key and certificate
    """
    key = None
    cert = None
    uid = os.getuid()
    globus_key = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if os.path.isfile(globus_key):
        key = globus_key
        if debug:
            print "Found key in %s" % (key)
    if os.path.isfile(globus_cert):
        cert = globus_cert
        if debug:
            print "Found cert in %s" % (cert)

    # First presendence to HOST Certificate, RARE
    if 'X509_HOST_CERT' in os.environ:
        cert = os.environ['X509_HOST_CERT']
        key = os.environ['X509_HOST_KEY']
        if debug:
            print "Found key in %s" % (key)
            print "Found cert in %s" % (cert)

    # Second preference to User Proxy, very common
    elif 'X509_USER_PROXY' in os.environ:
        cert = os.environ['X509_USER_PROXY']
        key = cert
        if debug:
            print "Found key in %s" % (key)
            print "Found cert in %s" % (cert)

    # Third preference to User Cert/Proxy combinition
    elif 'X509_USER_CERT' in os.environ:
        cert = os.environ['X509_USER_CERT']
        key = os.environ['X509_USER_KEY']
        if debug:
            print "Found key in %s" % (key)
            print "Found cert in %s" % (cert)

    # Worst case, look for cert at default location /tmp/x509up_u$uid
    elif os.path.isfile('/tmp/x509up_u%s' % (str(uid))):
        cert = '/tmp/x509up_u'+str(uid)
        key = cert
        if debug:
            print "Found key in %s" % (key)
            print "Found cert in %s" % (cert)

    if not os.path.exists(cert):
        print "Certificate PEM file %s not found" % (key)
    if not os.path.exists(key):
        print "Key PEM file %s not found" % (key)

    return key, cert
