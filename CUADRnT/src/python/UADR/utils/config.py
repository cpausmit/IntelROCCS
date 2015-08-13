#!/usr/bin/env python2.7
"""
File       : config.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Fetch and parse configuration file
"""

# system modules
import logging
import ConfigParser

# Get module specific logger
logger = logging.getLogger(__name__)

def get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg'):
    """
    Config values are read from a config file and parsed into a dictionary
    Dictionary can have multiple levels
    """
    config = dict()
    config_file = '%s/%s' % (path, file_name)
    logger.debug('Using config file %s', config_file)
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    for section_name in config_parser.sections():
        section_dict = dict()
        for option, value in config_parser.items(section_name):
            section_dict[option] = value
        config[section_name] = section_dict
    if not config:
        logger.error('Config file not found (%s)', config_file)
    return config
