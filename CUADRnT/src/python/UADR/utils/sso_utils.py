#!/usr/local/bin/python
"""
File       : sso_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Handle SSO cookie and fetch data for CERN service PopDB
"""

# system modules
import os
import time
import urllib
import urllib2
import subprocess

# package modules
from UADR.utils.utils import check_tool

def get_sso_key_cert(debug=0):
    """
    Get user globus key (~/.globus/userkey.pem) and certificate (~/.globus/usercert.pem)
    """
    key = None
    cert = None
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

    if not os.path.exists(cert):
        print "Certificate PEM file %s not found" % (key)
    if not os.path.exists(key):
        print "Key PEM file %s not found" % (cert)

    return key, cert

def get_sso_cookie(cookie_path, target_url, debug=0):
    """
    Function generates CERN SSO cookie (~/.globus/cern-sso-cookie) using cern-get-sso-cookie command
    Cookie is valid for 24h
    """
    # cookie is valid for 24h
    cern_tool = 'cern-get-sso-cookie'
    if check_tool(cern_tool, debug):
        sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
        key, cert = get_sso_key_cert(debug)
        if not os.path.exists(cookie_path):
            os.makedirs(cookie_path)
        cmd = subprocess.Popen([cern_tool, "--cert", cert, "--key", key, "-u", target_url, "-o", sso_cookie], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        ret_str = cmd.communicate()[0]
        if cmd.returncode != 0:
            print ret_str
            print "Could not generate SSO cookie %s" % (sso_cookie)
            if os.exists(sso_cookie):
                os.remove(sso_cookie)
        elif debug:
            print "Generated SSO cookie %s" % (sso_cookie)

def check_cookie(cookie_path, target_url, debug=0):
    """
    Check if CERN SSO cookie (~/.globus/cern-sso-cookie) exists and is valid
    A generated cookie is valid for 24h
    """
    sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
    cookie_jar = '%s/%s' % (cookie_path, 'cern_sso_cookie_jar')
    if os.path.exists(cookie_path):
        time_now = time.time()
        valid_seconds = 60*59*24
        if os.path.isfile(sso_cookie):
            mod_time = os.path.getmtime(sso_cookie)
            if (os.path.getsize(sso_cookie)) == 0 or ((time_now-valid_seconds) > mod_time):
                os.remove(sso_cookie)
                if os.path.isfile(cookie_jar):
                    os.remove(cookie_jar)
                if debug:
                    print "Getting a new SSO cookie %s for %s" % (sso_cookie, target_url)
                get_sso_cookie(cookie_path, target_url, debug)
        else:
            if debug:
                print "Getting a new SSO cookie %s for %s" % (sso_cookie, target_url)
            get_sso_cookie(cookie_path, target_url, debug)
    else:
        if debug:
            print "Getting a new SSO cookie %s for %s" % (sso_cookie, target_url)
        get_sso_cookie(cookie_path, target_url, debug)

def sso_fetch(cookie_path, target_url, api, params=dict(), debug=0):
    """
    Fetch data from PopDB API api with parameters params
    """
    check_cookie(cookie_path, target_url, debug)
    url_data = urllib.urlencode(params)
    url = '%s/%s' % (target_url, api)
    request = urllib2.Request(url, url_data)
    full_url = request.get_full_url() + '?' + request.get_data()
    return get_data(cookie_path, full_url, debug)

def get_data(cookie_path, full_url, debug=0):
    """
    Make make using cURL and CERN SSO cookie
    Use different cookie jar to keep mod time of cookie file same as generation time for validity check reasons
    Data is json data returned as a string
    Use json.loads(data) to generate json structure
    """
    data = "{}"
    if debug:
        print "Fetching SSO data for url:\n%s" % (full_url)
    sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
    cookie_jar = '%s/%s' % (cookie_path, 'cern_sso_cookie_jar')
    cmd = subprocess.Popen(['curl', '-k', '-s', '-L', '--cookie', sso_cookie, '--cookie-jar', cookie_jar, full_url], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
    return_data = cmd.communicate()
    if cmd.returncode != 0:
        print "Couldn't fetch SSO data for url %s" % (full_url)
    else:
        data = return_data[0]
    return data
