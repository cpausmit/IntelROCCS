#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, datetime
import sites, dailyRockerBoard, subscribe, dataDealerReport
import phedexData

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
subscriptions = rockerBoard_.dailyRba(datasets, availableSites)
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

print subscriptions
sys.exit(0)

# subscribe selected datasets
print " ----  Subscribe Datasets  ---- "
startTime = datetime.datetime.now()
subscribe_ = subscribe.subscribe()
subscribe_.createSubscriptions(subscriptions)
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# send summary report
print " ----  Daily Summary  ---- "
startTime = datetime.datetime.now()
dataDealerReport_ = dataDealerReport.dataDealerReport()
dataDealerReport_.createReport()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# done
print " ----  Done  ---- "
endingTime = datetime.datetime.now()
totalTime = endingTime - startingTime
print " ----  Complete run took " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""
print " ----  End time : " + endingTime.strftime('%Hh %Mm %Ss') + "  ---- "

sys.exit(0)
