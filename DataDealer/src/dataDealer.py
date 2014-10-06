#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys
import init
import sites, rockerBoard, subscribe, subscriptionReport
import phedexApi, popDbApi, phedexData

# initialize
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

#===================================================================================================
#  M A I N
#===================================================================================================
# get all datasets
print " ----  Get Datasets  ---- "
datasets = self.phedexData.getAllDatasets()
for dataset in datasets:
	print dataset
sys.exit(0)
# get all sites
print " ----  Get Sites  ---- "
availableSites = getAvailableSites()

# rocker board algorithm
print " ---- Rocker Board Algorithm ---- "
subscriptions = rba.rba(datasets, availableSites)

# subscribe selected datasets
print " ---- Subscribe Datasets ---- "
subscribe_.createSubscriptions(subscriptions)

# send summary report
print " ---- Daily Summary ---- "
dataDealerReport_.createReport()

# done
print " ---- Done ---- "
sys.exit(0)