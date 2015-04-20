#!/usr/local/bin/python
"""
File       : config.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Fetch and parse configuration file
"""

# system modules
import os
import ConfigParser

def get_config_parser(debug=0):
    """
    Path to config file should be stored in environment variable CUADRANT_CONFIG
    If that variable does not exist use relative path from environmental variable CUADRANT_ROOT
    Set these variables by running setup.sh or they are set in an init script created at installation time
    """
    config_file = ''
    if 'CUADRNT_CONFIG' in os.environ:
        config_file = os.environ['CUADRNT_CONFIG']
        if debug:
            print "Config file %s taken from %s" % (config_file, 'CUADRNT_CONFIG')
    elif 'CUADRNT_ROOT' in os.environ:
        config_file = os.path.join(os.environ.get('CUADRNT_ROOT'), '/etc/cuadrnt.cfg')
        if debug:
            print "Config file %s taken from %s" % (config_file, 'CUADRNT_ROOT')
    if not os.path.isfile(config_file):
        print "Config file %s not found" % (config_file)
        return
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    return config_parser

def get_config(debug=0):
    """
    Config values are read from a config file and parsed into a dictionary
    Dictionary can have multiple levels
    """
    config = dict()
    config_parser = get_config_parser(debug=debug)
    if not config_parser:
        return config
    for section_name in config_parser.sections():
        section_dict = dict()
        if debug:
            print "Section: %s" % (section_name)
        for option, value in config_parser.items(section_name):
            section_dict[option] = value
            if debug:
                print "Option/value: %s : %s" % (option, value)
        config[section_name] = section_dict
    return config
