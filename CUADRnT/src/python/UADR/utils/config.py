#!/usr/local/bin/python
"""
File       : config.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Fetch and parse configuration file
"""

# system modules
import logging
import os
import ConfigParser

# Get module specific logger
logger = logging.getLogger(__name__)

def get_config_parser():
    """
    Config file is in /etc under the cuadrnt root
    """
    config_file = ''
    if 'CUADRNT_ROOT' in os.environ:
        config_file = '%s/%s' % (os.environ['CUADRNT_ROOT'], 'etc/cuadrnt.cfg')
    else:
        logger.error('No config file found')
        return

    if not os.path.isfile(config_file):
        logger.error('No config file found in %s', config_file)
        return

    logger.debug('Config file: %s', config_file)
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    return config_parser

def get_config():
    """
    Config values are read from a config file and parsed into a dictionary
    Dictionary can have multiple levels
    """
    config = dict()
    config_parser = get_config_parser()
    if not config_parser:
        return config
    for section_name in config_parser.sections():
        section_dict = dict()
        logger.debug('Section found: %s', section_name)
        for option, value in config_parser.items(section_name):
            section_dict[option] = value
            logger.debug('Option/value pair found: %s:%s', option, value)
        config[section_name] = section_dict
    return config
