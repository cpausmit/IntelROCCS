#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, datetime
import sites, dailyRockerBoard, subscribe, report, subscriptionProgress, weeklyRockerBoard
import phedexData, phedexApi, dbApi

# # initialize
# startingTime = datetime.datetime.now()
# print " ----  Start time : " + startingTime.strftime('%Hh %Mm %Ss') + "  ---- "
# print ""

#===================================================================================================
#  M A I N
#===================================================================================================
# get all datasets
print " ----  Get Datasets  ---- "
startTime = datetime.datetime.now()
phedexData_ = phedexData.phedexData()
datasets = phedexData_.getAllDatasets()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# get all sites
print " ----  Get Sites  ---- "
startTime = datetime.datetime.now()
sites_ = sites.sites()
availableSites = sites_.getAvailableSites()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# rocker board algorithm
print " ----  Rocker Board Algorithm  ---- "
startTime = datetime.datetime.now()
dailyRockerBoard_ = dailyRockerBoard.dailyRockerBoard()
subscriptions = dailyRockerBoard_.dailyRba(datasets, availableSites)
print subscriptions
# weeklyRockerBoard_ = weeklyRockerBoard.weeklyRockerBoard()
# subscriptions = weeklyRockerBoard_.weeklyRba(datasets, availableSites)
# print subscriptions
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# # subscribe selected datasets
# print " ----  Subscribe Datasets  ---- "
# startTime = datetime.datetime.now()
# subscribe_ = subscribe.subscribe()
# subscribe_.createSubscriptions(subscriptions)
# totalTime = datetime.datetime.now() - startTime
# print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
# print ""

# # update and check progress of subscriptions
# print " ----  Update and Check Subscriptions  ---- "
# startTime = datetime.datetime.now()
# subscriptionProgress_ = subscriptionProgress.subscriptionProgress()
# subscriptionProgress_.updateProgress()
# subscriptionProgress_.checkProgress()
# totalTime = datetime.datetime.now() - startTime
# print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
# print ""

# # send summary report
# print " ----  Daily Summary  ---- "
# startTime = datetime.datetime.now()
# report_ = report.report()
# report_.createDailyReport()
# report_ = report.report()
# report_.createWeeklyReport()
# totalTime = datetime.datetime.now() - startTime
# print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
# print ""

#===================================================================================================
#  T E S T I N G
#===================================================================================================
# # get all subscriptions with an invalid dataset id
# dbApi_ = dbApi.dbApi()
# phedexApi_ = phedexApi.phedexApi()
# print "Subscriptions"
# query = "SELECT DISTINCT Requests.RequestId, Requests.DatasetId, Datasets.DatasetName FROM Requests LEFT JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Datasets.DatasetName IS NULL AND Requests.RequestType=%s GROUP BY Requests.DatasetId"
# values = [0]
# data1 = dbApi_.dbQuery(query, values=values)
# for row1 in data1:
#     print "Request ID: " + str(row1[0])
#     print "Dataset ID: " + str(row1[1])
#     # get all valid datasets in the subscription
#     query = "SELECT Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Requests.RequestId=%s AND Requests.RequestType=%s"
#     values = [row1[0], 0]
#     data2 = dbApi_.dbQuery(query, values=values)
#     validDatasets = [row2[0] for row2 in data2]
#     # get all datasets in subscription
#     data3 = phedexApi_.transferRequests(request=row1[0])
#     allDatasets = []
#     datasets = data3.get('phedex').get('request')[0].get('data').get('dbs').get('dataset')
#     allDatasets = [dataset.get('name') for dataset in datasets]
#     invalidDatasets = [dataset for dataset in allDatasets if dataset not in validDatasets]
#     for dataset in invalidDatasets:
#         print dataset
#     print ""

# print "Deletions"
# query = "SELECT DISTINCT Requests.RequestId, Requests.DatasetId, Datasets.DatasetName FROM Requests LEFT JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Datasets.DatasetName IS NULL AND Requests.RequestType=%s GROUP BY Requests.DatasetId"
# values = [1]
# data1 = dbApi_.dbQuery(query, values=values)
# for row1 in data1:
#     print "Request ID: " + str(row1[0])
#     print "Dataset ID: " + str(row1[1])
#     # get all valid datasets in the subscription
#     query = "SELECT Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Requests.RequestId=%s AND Requests.RequestType=%s"
#     values = [row1[0], 1]
#     data2 = dbApi_.dbQuery(query, values=values)
#     validDatasets = [row2[0] for row2 in data2]
#     # get all datasets in subscription
#     data3 = phedexApi_.deleteRequests(request=row1[0])
#     allDatasets = []
#     datasets = data3.get('phedex').get('request')[0].get('data').get('dbs').get('dataset')
#     allDatasets = [dataset.get('name') for dataset in datasets]
#     invalidDatasets = [dataset for dataset in allDatasets if dataset not in validDatasets]
#     for dataset in invalidDatasets:
#         print dataset
#     print ""

#===================================================================================================
#  E X I T
#===================================================================================================
# print " ----  Done  ---- "
# endingTime = datetime.datetime.now()
# totalTime = endingTime - startingTime
# print " ----  Complete run took " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
# print ""
# print " ----  End time : " + endingTime.strftime('%Hh %Mm %Ss') + "  ---- "

sys.exit(0)
