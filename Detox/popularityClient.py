#!/usr/bin/python
"""
Popularity client.
Perform SSO Authentication and retrieve data from API
Check: http://linux.web.cern.ch/linux/docs/cernssocookie.shtml to avoid password typing
"""
from sys import argv, exit, stderr, version_info
from urllib2 import HTTPCookieProcessor, AbstractHTTPHandler, \
                    urlopen, build_opener, install_opener
from urllib import urlencode
from urlparse import urljoin
import os, re
try:
  from httplib import HTTPSConnection
except:
  from httplib import HTTPS as HTTPSConnection

DEBUG = False
ssl_key_file = None
ssl_cert_file = None

if DEBUG:
  def debug_init(self, **kwargs): self._debuglevel = 1
  AbstractHTTPHandler.__init__ = debug_init

class X509HTTPS(HTTPSConnection):
  def __init__(self, host, *args, **kwargs):
    HTTPSConnection.__init__(self, host, key_file = ssl_key_file,
			     cert_file = ssl_cert_file, **kwargs)

class X509Auth(AbstractHTTPHandler):
  def default_open(self, req):
    return self.do_open(X509HTTPS, req)

def parse_form_fields(page):
  """Parses and decodes HTML form inputs with 'name' and 'value'."""
  result = {}
  for item in page.split('<input '):
    if item.find('name=') != -1 and item.find('value=') != -1:
      key = item.split('name="')[1].split('"')[0]
      val = item.split('value="')[1].split('"')[0] \
            .replace('&quot;', '"').replace('&lt;','<')
      result[key] = val
  return result

def sso_auth(auth_url):
  # Build opener to include X509 cert auth.
  opener = build_opener(X509Auth())
  opener.addheaders = [('User-agent', 'Mozilla/5.0')]
  install_opener(opener)

  # Read document.
  document = urlopen(auth_url)
  pop_data = document.read()
  return pop_data

if __name__ == "__main__":


  if not ssl_key_file:
    x509_path = os.getenv("X509_USER_KEY", None)
    if x509_path and os.path.exists(x509_path):
      ssl_key_file = x509_path

  if not ssl_cert_file:
    x509_path = os.getenv("X509_USER_CERT", None)
    if x509_path and os.path.exists(x509_path):
      ssl_cert_file = x509_path

  if not ssl_key_file:
    x509_path = os.getenv("HOME") + "/.globus/userkey.pem"
    if os.path.exists(x509_path):
      ssl_key_file = x509_path

  if not ssl_cert_file:
    x509_path = os.getenv("HOME") + "/.globus/usercert.pem"
    if os.path.exists(x509_path):
      ssl_cert_file = x509_path

  if not ssl_key_file or not os.path.exists(ssl_key_file):
    print >>stderr, "no certificate private key file found"
    exit(1)

  if not ssl_cert_file or not os.path.exists(ssl_cert_file):
    print >>stderr, "no certificate public key file found"
    exit(1)

  # Authenticate to CERN SSO and retrieve document from the actual service


  #pop_base_url = "https://cms-popularity.cern.ch/popdb/"
  pop_base_url = "https://cmsweb.cern.ch/popdb/"
  pop_url = '%s/%s'%(pop_base_url,argv[1])

  data = sso_auth(pop_url)

  # print "# CERT: " + ssl_cert_file
  print data
  exit(0)
