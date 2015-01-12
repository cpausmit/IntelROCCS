#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is a test script for data dealer
#---------------------------------------------------------------------------------------------------
import sys, datetime, ConfigParser
import sites, rockerBoard, subscribe, dataDealerReport
import dbApi, phedexData

# get variables
config = ConfigParser.RawConfigParser()
config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.test.cfg')
#rankingsCachePath = config.get('DataDealer', 'cache')

#===================================================================================================
#  M A I N
#===================================================================================================
# test db api
print " ----  Test DB API  ---- "
dbApi_ = dbApi.dbApi()
siteName = "T2_US_Nebraska"
# passing test
query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
values = [siteName, "AnalysisOps"]
data = dbApi_.dbQuery(query, values=values)
print data

# incorrect number of values
query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteId=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
values = [siteName]
data = dbApi_.dbQuery(query, values=values)

# incorrect value
query = "SELECT Quotas.SizeTb FROM Quotas INNER JOIN Sites ON Quotas.SiteI=Sites.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.SiteName=%s AND Groups.GroupName=%s"
values = [siteName, "AnalysisOps"]
data = dbApi_.dbQuery(query, values=values)
print ""

# # get all datasets
# print " ----  Get Datasets  ---- "
# phedexData = phedexData.phedexData()
# datasets = phedexData.getAllDatasets()
# print ""

# # get all sites
# print " ----  Get Sites  ---- "
# sites_ = sites.sites()
# availableSites = sites_.getAvailableSites()
# print ""

# # rocker board algorithm
# print " ----  Rocker Board Algorithm  ---- "
# rba = rockerBoard.rockerBoard()
# subscriptions = rba.rba(datasets, availableSites)
# print ""

# # subscribe selected datasets
# print " ----  Subscribe Datasets  ---- "
# subscribe_ = subscribe.subscribe()
# subscribe_.createSubscriptions(subscriptions)
# print ""

# # send summary report
# print " ----  Daily Summary  ---- "
# dataDealerReport_ = dataDealerReport.dataDealerReport()
# dataDealerReport_.createReport()
# print ""

# done
print " ----  Done  ---- "

sys.exit(0)
