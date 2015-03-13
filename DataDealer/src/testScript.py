#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, datetime
import sites, dailyRockerBoard, subscribe, report, subscriptionProgress, weeklyRockerBoard
import phedexData, phedexApi

# initialize
startingTime = datetime.datetime.now()
print " ----  Start time : " + startingTime.strftime('%Hh %Mm %Ss') + "  ---- "
print ""

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
weeklyRockerBoard_ = weeklyRockerBoard.weeklyRockerBoard()
subscriptions = weeklyRockerBoard_.weeklyRba(datasets, availableSites)
print subscriptions
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

# update and check progress of subscriptions
print " ----  Update and Check Subscriptions  ---- "
startTime = datetime.datetime.now()
subscriptionProgress_ = subscriptionProgress.subscriptionProgress()
subscriptionProgress_.updateProgress()
subscriptionProgress_.checkProgress()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# send summary report
print " ----  Daily Summary  ---- "
startTime = datetime.datetime.now()
report_ = report.report()
report_.createDailyReport()
report_ = report.report()
report_.createWeeklyReport()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

#===================================================================================================
#  T E S T I N G
#===================================================================================================

#===================================================================================================
#  E X I T
#===================================================================================================
print " ----  Done  ---- "
endingTime = datetime.datetime.now()
totalTime = endingTime - startingTime
print " ----  Complete run took " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""
print " ----  End time : " + endingTime.strftime('%Hh %Mm %Ss') + "  ---- "

sys.exit(0)
