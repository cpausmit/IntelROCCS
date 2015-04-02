#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, datetime
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
