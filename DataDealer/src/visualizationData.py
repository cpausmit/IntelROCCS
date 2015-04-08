#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This script collects data for all visualizations
#---------------------------------------------------------------------------------------------------
import sys, datetime
import systemVis

# initialize
startingTime = datetime.datetime.now()
print " ----  Start time : " + startingTime.strftime('%Hh %Mm %Ss') + "  ---- "
print ""

#===================================================================================================
#  M A I N
#===================================================================================================
# get all datasets
print " ----  System Data  ---- "
startTime = datetime.datetime.now()
systemVis_ = systemVis.systemVis()
systemVis_.collectData()
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
