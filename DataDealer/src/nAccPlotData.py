#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# This is the main script of the DataDealer. See README.md for more information.
#---------------------------------------------------------------------------------------------------
import sys, os, ConfigParser
import phedexData, popDbData

#===================================================================================================
#  M A I N
#===================================================================================================
# get variables
config = ConfigParser.RawConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'data_dealer.cfg'))
visualizationPath = config.get('data_dealer', 'visualization_path')

# get all datasets
phedexData_ = phedexData.phedexData()
datasets = phedexData_.getAllDatasets()

# initialize popularity data
popDbApData_ = popDbData.popDbData()
popDbApData_.buildGetSingleDSstatCache(datasets)

# print headers
fs = open('%s/accesses.csv' % (visualizationPath), 'w')
fs.write("dataset_name,data_tier,size,minus_7,minus_6,minus_5,minus_4,minus_3,minus_2,minus_1,zero,plus_1,plus_2,plus_3,plus_4,plus_5,plus_6,plus_7\n")
fs.close()

# get data
for datasetName in datasets:
    dataTier = datasetName.split('/')[-1:][0]
    size = phedexData_.getDatasetSize(datasetName)
    historicalData = popDbApData_.getHistoricalData(datasetName)
    minus7 = historicalData[0]
    minus6 = historicalData[1]
    minus5 = historicalData[2]
    minus4 = historicalData[3]
    minus3 = historicalData[4]
    minus2 = historicalData[5]
    minus1 = historicalData[6]
    zero = historicalData[7]
    plus1 = historicalData[8]
    plus2 = historicalData[9]
    plus3 = historicalData[10]
    plus4 = historicalData[11]
    plus5 = historicalData[12]
    plus6 = historicalData[13]
    plus7 = historicalData[14]

    # print data
    fs = open('%s/accesses.csv' % (visualizationPath), 'w')
    fs.write("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n" % (datasetName, dataTier, size, minus7, minus6, minus5, minus4, minus3, minus2, minus1, zero, plus1, plus2, plus3, plus4, plus5, plus6, plus7))
    fs.close()

sys.exit(0)
