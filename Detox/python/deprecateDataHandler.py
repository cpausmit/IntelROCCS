#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, MySQLdb, json
import datetime, time
import phedexDataset

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class DeprecateDataHandler:
    def __init__(self):
        self.deprecated = {}

    def extractDeprecatedData(self):
        cmd = os.environ['DETOX_BASE'] + '/' + \
            'das_client.py --query=\'dataset status=DEPRECATED\' --limit=50000'

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)

        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 2 minutes
        try:
            strout, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- DAS call timed out, stopping")

        if process.returncode != 0:
            print "    - DAS queries got bad status " + str(process.returncode)
            raise Exception(" FATAL -- DAS call timed out, stopping")

        datasets = strout.split(os.linesep)
        for dset in datasets:
            self.deprecated[dset] = 1
        totalSets = len(datasets)

        print " -- found " + str(totalSets) +" deprecated sets"
                
    def getDeprecatedSets(self):
        return self.deprecated

    def isDeprecated(self,dset):
        if dset in self.deprecated:
            return True
        else:
            return False




