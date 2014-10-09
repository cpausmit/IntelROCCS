#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, datetime
import init
import sites, rockerBoard, subscribe, dataDealerReport
import phedexApi, popDbApi, phedexData

# initialize
startingTime = datetime.datetime.now()
startTime = startingTime
print " ----  Start time : " + startingTime.strftime('%Hh %Mm %Ss') + "  ---- "
print ""
print " ----  Initialize  ---- "
phedexCache = os.environ['DATA_DEALER_PHEDEX_CACHE']
cacheDeadline = int(os.environ['DATA_DEALER_CACHE_DEADLINE'])
threshold = int(os.environ['DATA_DEALER_THRESHOLD'])

phedexApi_ = phedexApi.phedexApi()
phedexApi_.renewProxy()

popDbApi_ = popDbApi.popDbApi()
popDbApi_.renewSsoCookie()

phedexData = phedexData.phedexData(phedexCache, cacheDeadline)
sites_ = sites.sites()
rba = rockerBoard.rockerBoard()
subscribe_ = subscribe.subscribe()
dataDealerReport_ = dataDealerReport.dataDealerReport()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

#===================================================================================================
#  M A I N
#===================================================================================================
# get all datasets
print " ----  Get Datasets  ---- "
startTime = datetime.datetime.now()
datasets = phedexData.getAllDatasets()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# get all sites
print " ----  Get Sites  ---- "
startTime = datetime.datetime.now()
availableSites = sites_.getAvailableSites()
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# rocker board algorithm
print " ----  Rocker Board Algorithm  ---- "
startTime = datetime.datetime.now()
subscriptions = rba.rba(datasets, availableSites)
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# subscribe selected datasets
print " ----  Subscribe Datasets  ---- "
startTime = datetime.datetime.now()
subscribe_.createSubscriptions(subscriptions)
totalTime = datetime.datetime.now() - startTime
print " ----  " + str(totalTime.seconds) + "s " + str(totalTime.microseconds) + "ms" + "  ---- "
print ""

# send summary report
print " ----  Daily Summary  ---- "
startTime = datetime.datetime.now()
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
