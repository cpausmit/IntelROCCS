#!/usr/local/bin/python
"""
File       : sso_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Handle SSO cookie and fetch data for CERN service PopDB
"""

# system modules
import logging
import os
import time
import json
import urllib
import urllib2
import subprocess

# package modules
from UADR.utils.utils import check_tool

# Get module specific logger
logger = logging.getLogger(__name__)

def get_sso_key_cert():
    """
    Get user globus key (~/.globus/userkey.pem) and certificate (~/.globus/usercert.pem)
    """
    key = ''
    cert = ''
    globus_key = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if os.path.isfile(globus_key):
        key = globus_key
    if os.path.isfile(globus_cert):
        cert = globus_cert

    if not os.path.exists(key):
        logger.ERROR('Key PEM file %s not found', key)
    if not os.path.exists(key):
        logger.ERROR('Certificate PEM file %s not found', key)

    logger.debug('Key file: %s', key)
    logger.debug('Certificate file: %s', cert)

    return key, cert

def get_sso_cookie(cookie_path, target_url):
    """
    Function generates CERN SSO cookie (~/.globus/cern-sso-cookie) using cern-get-sso-cookie command
    Cookie is valid for 24h
    """
    # cookie is valid for 24h
    cern_tool = 'cern-get-sso-cookie'
    if check_tool(cern_tool):
        sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
        cookie_jar = '%s/%s' % (cookie_path, 'cern_sso_cookie_jar')
        key, cert = get_sso_key_cert()
        if not os.path.exists(cookie_path):
            os.makedirs(cookie_path)
        fs = open(sso_cookie, 'w')
        fs.write('')
        fs.close()
        fs = open(cookie_jar, 'w')
        fs.write('')
        fs.close()
        cmd = subprocess.Popen([cern_tool, "--cert", cert, "--key", key, "-u", target_url, "-o", sso_cookie], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        ret_str = cmd.communicate()[0]
        if cmd.returncode != 0:
            logger.error('Could not generate SSO cookie %s due to:\n    %s', sso_cookie, ret_str)
        logger.debug('Generated SSO cookie %s', sso_cookie)

def check_cookie(cookie_path, target_url):
    """
    Check if CERN SSO cookie (~/.globus/cern-sso-cookie) exists and is valid
    A generated cookie is valid for 24h
    """
    sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
    if os.path.exists(cookie_path):
        time_now = time.time()
        valid_seconds = 60*59*24
        if os.path.isfile(sso_cookie):
            mod_time = os.path.getmtime(sso_cookie)
            if (os.path.getsize(sso_cookie)) == 0 or ((time_now-valid_seconds) > mod_time):
                get_sso_cookie(cookie_path, target_url)
        else:
            get_sso_cookie(cookie_path, target_url)
    else:
        get_sso_cookie(cookie_path, target_url)

def sso_fetch(cookie_path, target_url, api, params=dict()):
    """
    Fetch data from PopDB API api with parameters params
    """
    check_cookie(cookie_path, target_url)
    url_data = urllib.urlencode(params)
    url = '%s/%s' % (target_url, api)
    request = urllib2.Request(url, url_data)
    full_url = request.get_full_url() + '?' + request.get_data()
    return get_data(cookie_path, full_url)

def get_data(cookie_path, full_url):
    """
    Make make using cURL and CERN SSO cookie
    Use different cookie jar to keep mod time of cookie file same as generation time for validity check reasons
    Data is json data returned as a string
    Use json.loads(data) to generate json structure
    """
    # FIXME: Better way of checking for error
    data = "{}"
    logger.debug('Fetching SSO data for url:\n    %s', full_url)
    sso_cookie = '%s/%s' % (cookie_path, 'cern_sso_cookie')
    cookie_jar = '%s/%s' % (cookie_path, 'cern_sso_cookie_jar')
    cmd = subprocess.Popen(['curl', '-k', '-s', '-L', '--cookie', sso_cookie, '--cookie-jar', cookie_jar, full_url], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
    return_data = cmd.communicate()
    try:
        json.loads(return_data[0])
        data = return_data[0]
    except Exception:
        logger.error("Couldn't fetch SSO data for url %s\n    Reason:\n    %s", full_url, return_data[0])
    return data
