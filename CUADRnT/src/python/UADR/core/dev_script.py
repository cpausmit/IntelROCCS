#!/usr/local/bin/python
"""
File       : dev_script.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test script used for development
"""

# system modules
import sys

# package modules
from UADR.services.popdb import PopDBService

popdb = PopDBService()

sys.exit(0)
