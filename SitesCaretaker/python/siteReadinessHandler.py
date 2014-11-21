#===================================================================================================
#  C L A S S
#===================================================================================================
import os, subprocess, re, signal, sys, MySQLdb, json
from datetime import datetime
import siteReadiness

class Alarm(Exception):
    pass

def alarm_handler(signum, frame):
    raise Alarm

class SiteReadinessHandler:
    def __init__(self):
        self.siteReadiness = {}
    
    def extractReadinessData(self):
        self.getWaitingRoomData()
        self.getDeadSitesData()

    def getDeadSitesData(self):
        webServer = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?'
        args = 'columnid=199&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1'

        url = '"' + webServer + args + '"'
        cmd = 'curl -k -H "Accept: application/json" ' + url
        print ' Access DeadSites: ' + cmd

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to DeadSites timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to DeadSites failed, stopping")

        dataJson = json.loads(mystring)
        data = dataJson["csvdata"]
        for item in data:
            site = item['VOName']
            status = item['Status']
            timest = datetime.strptime(item['Time'],"%Y-%m-%dT%H:%M:%S").date()
            
            if site not in self.siteReadiness:
                self.siteReadiness[site] = siteReadiness.SiteReadiness(site)
            self.siteReadiness[site].update(0,status,timest)

        forced = 'T2_TW_Taiwan'
        if forced not in self.siteReadiness:
            self.siteReadiness[forced] = siteReadiness.SiteReadiness(forced)
        self.siteReadiness[forced].update(0,'in',datetime.strptime('2014-11-03',"%Y-%m-%d").date())
        
    def getWaitingRoomData(self):
        webServer = 'http://dashb-ssb.cern.ch/dashboard/request.py/getplotdata?'
        args = 'columnid=153&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1'

        url = '"' + webServer + args + '"'
        cmd = 'curl -k -H "Accept: application/json" ' + url
        print ' Access SiteWaitingRoom: ' + cmd

        process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10*60)  # 10 minutes
        try:
            mystring, error = process.communicate()
            signal.alarm(0)
        except Alarm:
            print " Oops, taking too long!"
            raise Exception(" FATAL -- Call to SiteWaitingRoom timed out, stopping")

        if process.returncode != 0:
            print " Received non-zero exit status: " + str(process.returncode)
            raise Exception(" FATAL -- Call to SiteWaitingRoom failed, stopping")

        dataJson = json.loads(mystring)
        data = dataJson["csvdata"]
        for item in data:
            site = item['VOName']
            status = item['Status']
            timest = datetime.strptime(item['Time'],"%Y-%m-%dT%H:%M:%S").date()
            
            if site not in self.siteReadiness:
                self.siteReadiness[site] = siteReadiness.SiteReadiness(site)
            self.siteReadiness[site].update(1,status,timest)

    def getSites(self):
        return self.siteReadiness.keys()
            
    def getSiteReadiness(self,site):
        return self.siteReadiness[site]
