#!/usr/bin/python
#--------------------------------------------------------------------------------------------------
#
# This script reads the information from the phedex cache.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb
import siteStatus
#import ROOT

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def processFile(fileName,debug=0):
    # processing the contents of a simple file into a hash array

    sizesPerSite = {}

    iFile = open(fileName,'r')   
    # loop through all lines
    for line in iFile.xreadlines():
        line = line[:-1]
        f = line.split()
        if len(f) == 8:
            dataset = f[0]
            group   = f[1]
            date    = int(f[2])
            size    = float(f[3])
            nFiles  = int(f[4])
            valid   = int(f[5])
            custd   = int(f[6])
            site    = f[7]

        else:
            print 'Columns not equal 8: \n %s'%(line)
            sys.exit(1)

        # first step, find the sizes per site per group hash array
        if site in sizesPerSite:
            sizesPerSitePerGroup = sizesPerSite[site]
        else:
            sizesPerSitePerGroup = {}
            sizesPerSite[site] = sizesPerSitePerGroup

        if group in sizesPerSitePerGroup:
            sizesPerSitePerGroup[group] += size
        else:
            sizesPerSitePerGroup[group]  = size

    iFile.close();

    # return the sizes per site
    return sizesPerSite

def processDb():
    # processing the contents of the Quota database table into a hash array

    quotasPerSite = {}

    # we are looking for all relevant sites
    sites = {}
    db = os.environ.get('DETOX_SITESTORAGE_DB')
    server = os.environ.get('DETOX_SITESTORAGE_SERVER')
    user = os.environ.get('DETOX_SITESTORAGE_USER')
    pw = os.environ.get('DETOX_SITESTORAGE_PW')
    table = os.environ.get('DETOX_QUOTAS')
    # open database connection
    print ' Access quota table (%s) in site storage database (%s) to find all sites.'%(table,db)
    db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # define sql
    sql = "select * from " + table

    # go ahead and try
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            site    = row[0]
            group   = row[1]
            quota   = row[2]
            time    = row[3]
            status  = row[3]

            # first step, find the sizes per site per group hash array
            if site in quotasPerSite:
                quotasPerSitePerGroup = quotasPerSite[site]
            else:
                quotasPerSitePerGroup = {}
                quotasPerSite[site] = quotasPerSitePerGroup
    
            if group in quotasPerSitePerGroup:
                print ' WARNING -- error? entry appears twice, check! (%s,%s)'%(site,group)
                quotasPerSitePerGroup[group] += float(quota)
            else:
                quotasPerSitePerGroup[group]  = float(quota)
    
    except:
        print ' Error(%s) -- could not retrieve sites'%(sql)
        print sys.exc_info()
        sys.exit(1)

    return quotasPerSite

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
sizeAnalysis = True

pagGroups = {"higgs":0,"exotica":0,"susy":0,"top":0,"SMP":0,"forward":0,"b-physics":0,"B2G":0,
             "deprecated-undefined":0 }
pogGroups = {"muon":0,"tracker-pog":0,"e-gamma_ecal":0,"jets-met_hcal":0,"tau-pflow":0,
             "b-tagging":0}
dpgGroups = {"trigger":0,"tracker-dpg":0}

usage  = "\n"
usage += " readPhedexCache.py  <sitePattern> [ <AnalysisGroupPattern>=????-??-?? ]\n"
usage += "\n"
usage += "   sitePattern   - pattern to select sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   groupPattern  - pattern to select analysis groups (ex. 201[34]-0[0-7]-??)\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    group = 'AnalysisOps'
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    group = str(sys.argv[2])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

# figure out which files to consider
file = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
       os.environ['DETOX_PHEDEX_CACHE']

print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Cache in:      %s"%(file) 
print " Site  pattern: %s"%(site) 
print " Group pattern: %s"%(group) 

if debug>0:
    print ' Analyzing: ' + file

# read in all relevant data
quotasPerSite = processDb()               # from our SiteStorage database
sizesPerSite = processFile(file,debug=0)  # from the phedex cache

# create summary information and potentially print the contents
sizeTotalGb = 0.

# looping over all sites and completing business per site
for site in sizesPerSite:
    sizesPerSitePerGroup = sizesPerSite[site]

    print '\n SITE: ' + site

    # keep track of the quota changes and the the additional size needed
    addQuota = 0.
    addSize = 0.

    # FIRST ADJUST QUOTAS

    # do we need to update the quota based on the official group assignment?
    if site in quotasPerSite:
        quotasPerSitePerGroup = quotasPerSite[site]
        if 'AnalysisOps' in quotasPerSitePerGroup and 'AnalysisOps' in sizesPerSitePerGroup:

            print ' Existing quota: %d'%(quotasPerSite[site]['AnalysisOps']) 

            for group in pagGroups:
                if group in quotasPerSitePerGroup:
                    addQuota += quotasPerSite[site][group]
                    print ' PAG quota add:  %d -- %s'%(quotasPerSite[site][group],group)

            for group in pogGroups:
                if group in quotasPerSitePerGroup:
                    addQuota += quotasPerSite[site][group]
                    print ' POG quota add:  %d -- %s'%(quotasPerSite[site][group],group)

            for group in dpgGroups:
                if group in quotasPerSitePerGroup:
                    addQuota += quotasPerSite[site][group]
                    print ' DPG quota add:  %d -- %s'%(quotasPerSite[site][group],group)
                    
            if addQuota>0:
                print " Added quota [TB]:  +%8.2f   %8.2f -->  %8.2f"%\
                      (addQuota,quotasPerSite[site]['AnalysisOps'],
                       quotasPerSite[site]['AnalysisOps']+addQuota)
                quotasPerSite[site]['AnalysisOps'] += addQuota
                print " update Quotas set SizeTb=%d where SiteName = '%s' and GroupName = '%s';"%\
                      (quotasPerSite[site]['AnalysisOps'],site,'AnalysisOps')
    
    # NEXT ACCOUNT FOR ROGUE DATASETS (quota is already updated in memory)

    for group in sizesPerSitePerGroup:
        size = sizesPerSitePerGroup[group]
        sizeTotalGb += size
        print " - data volume [TB]:  %8.2f  %-s"%(size/1024.,group) 

        if group in pagGroups:
            #print ' found pagGroup: %s - %.3f'%(group,size)
            addSize += size
            print " update Quotas set SizeTb=0 where SiteName = '%s' and GroupName = '%s';"%\
                  (site,group)
        if group in pogGroups:
            #print ' found pogGroup: %s - %.3f'%(group,size)
            addSize += size
            print " update Quotas set SizeTb=0 where SiteName = '%s' and GroupName = '%s';"%\
                  (site,group)
        if group in dpgGroups:
            #print ' found dpgGroup: %s - %.3f'%(group,size)
            addSize += size
            print " update Quotas set SizeTb=0 where SiteName = '%s' and GroupName = '%s';"%\
                  (site,group)

    # show the status after removing rogue data
    if site in quotasPerSite:
        quotasPerSitePerGroup = quotasPerSite[site]
        if 'AnalysisOps' in quotasPerSitePerGroup and 'AnalysisOps' in sizesPerSitePerGroup:

            if (addSize+sizesPerSite[site]['AnalysisOps'])/1024. > \
               0.9*quotasPerSite[site]['AnalysisOps']:
                print " TOO SMALL [TB]:  +%8.2f   %8.2f  %8.2f  --> %8.2f at site %s"%\
                      (addSize/1024.,(addSize+sizesPerSite[site]['AnalysisOps'])/1024.,
                       quotasPerSite[site]['AnalysisOps'],1.12*(addSize+sizesPerSite[site]['AnalysisOps'])/1024.,site) 
                print " update Quotas set SizeTb=%d where SiteName = '%s' and GroupName = '%s';"%\
                      (1.12*(addSize+sizesPerSite[site]['AnalysisOps'])/1024.,site,'AnalysisOps')
            
# printout the summary
print " "
print " = = = = S U M M A R Y = = = = "
print " "
print " - total size [TB]:  %9.0f\n"%(sizeTotalGb/1024.) 

sys.exit(0)
