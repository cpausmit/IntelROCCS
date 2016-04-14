#!/usr/bin/env python2.7
"""
File       : update_cpu.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Keep maximum CPU for sites updated, run every hour
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.tools.sites import SiteManager

class UpdateCPU(object):
    """
    Update CPU queries CRAB to get current number of CPU's at each site
    Is used to decide maximum CPU capacity for sites
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)

    def start(self):
        """
        Begin Update CPU
        """
        t1 = datetime.datetime.utcnow()
        self.sites.update_cpu()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Update CPU took %s', str(td))

def main(argv):
    """
    Main driver for Update CPU
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: update_cpu.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: update_cpu.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: update_cpu.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: update_cpu.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: update_cpu.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: update_cpu.py --help"
                sys.exit()
        else:
            print "usage: update_cpu.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: update_cpu.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'update_cpu.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    update_cpu = UpdateCPU(config)
    update_cpu.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
