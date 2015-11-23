#===================================================================================================
#  C L A S S
#===================================================================================================
import sys, os, subprocess, re, smtplib, shutil, string, statistics
import phedexApi
import dbInfoHandler

class SpreadLowRankSets:
    def __init__(self,dbInfoHandler):

        if not os.environ.get('DETOX_DB'):
            raise Exception(' FATAL -- DETOX environment not defined: source setup.sh\n')

        self.DETOX_USAGE_MAX = float(os.environ['DETOX_USAGE_MAX'])

        self.sitePropers = {}
        self.dataPropers = {}

        self.setsToSites = {}
        self.dbInfoHandler = dbInfoHandler

    def assignSitePropers(self,sitePropers):
        self.sitePropers = sitePropers

    def assignPhedexSets(self,dataPropers):
        self.dataPropers = dataPropers

    def assignDatasets(self):
        #list all datasets ordered by ranks
        #pick T1 site that have enough space, best rank sites match first
        #select datasets for T1 sites, can't increase T1 site by more than 1%
        siteNames = []
        siteAssigned = {}
        ranksAssigned = {}
        for site in sorted(self.sitePropers, cmp=self.compSites):
            if not site.startswith("T1_"):
                continue
            siteNames.append(site)
            self.setsToSites[site] = []
            siteAssigned[site] = 0.0
            ranksAssigned[site] = []

        for dset in sorted(self.dataPropers, cmp=self.compDatasets):
            setSize = self.dataPropers[dset].getTrueSize()
            dsetRank = self.dataPropers[dset].getGlobalRank()
            if dsetRank < 90:
                continue
            if self.dataPropers[dset].isOnT1Site():
                continue

            for site in siteNames:
                taken = self.sitePropers[site].spaceTaken() + siteAssigned[site] + setSize
                quota = self.sitePropers[site].siteSizeGb()
                available = quota*self.DETOX_USAGE_MAX - taken
                siteRank =  self.sitePropers[site].siteRank()
                if setSize < available and (siteAssigned[site] + setSize) < quota*0.03:
                    siteAssigned[site] = siteAssigned[site] + setSize
                    ranksAssigned[site].append(dsetRank)
                    self.setsToSites[site].append(dset)
                
        for site in sorted(self.setsToSites, cmp=self.compSites):
            if len(ranksAssigned[site]) > 0:
                print "  - Subscribing to " + site + " %d TBs"%(siteAssigned[site])
                print "  -- average assigned rank %d"%(statistics.mean(ranksAssigned[site]))
                #print self.setsToSites[site]
                self.submitTransferRequest(site,self.setsToSites[site])
                break


    def submitTransferRequest(self,site,datasets2trans):
        if len(datasets2trans) < 1:
            return
        phedex = phedexApi.phedexApi(logPath='./')
        # compose data for deletion request
        check,data = phedex.xmlData(datasets=datasets2trans,instance='prod')
        if check:
            print " ERROR - phedexApi.xmlData failed"
            print data
            sys.exit(1)
            
        # here the request is really sent
        message = 'IntelROCCS -- Automatic Transfer Request'
        check,response = phedex.subscribe(node=site,data=data,comments=message,group='AnalysisOps')
        if check:
            print " ERROR - phedexApi.subscribe failed"
            print response
            sys.exit(1)

        respo = response.read()
        matchObj = re.search(r'"id":"(\d+)"',respo)
        reqid = int(matchObj.group(1))

        rdate = (re.search(r'"request_date":"(.*?)"',respo)).group(1)
        rdate = rdate[:-3]
        self.dbInfoHandler.logRequest(site,datasets2trans,reqid,rdate,0)
        #self.submitUpdateRequest(site,reqid)

    def compDatasets(self,a,b):
        ra = self.dataPropers[a].getGlobalRank()
        rb = self.dataPropers[b].getGlobalRank()
        if ra >= rb:
            return -1
        else:
            return 1

    def compSites(self,a,b):
        ra = self.sitePropers[a].siteRank()
        rb = self.sitePropers[b].siteRank()
        if ra >= rb:
            return 1
        else:
            return -1 

    def submitUpdateRequest(self,site,reqid):
        # here we brute force deletion to be approved
        phedex = phedexApi.phedexApi(logPath='./')
        check,response = phedex.updateRequest(decision='approve',request=reqid,node=site,instance='prod')
        if check:
            print " ERROR - phedexApi.updateRequest failed - reqid="+ str(reqid)
            print response
        del phedex


   
