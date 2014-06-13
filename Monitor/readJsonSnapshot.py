#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script reads the information from a set of JSON snapshot files that we use to cache our
# popularity information.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, glob, subprocess, json, pprint
#import pyroot

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def processFile(fileName,debug=0):
    # processing the contents of a simple file into a hash array

    nSkipped  = 0
    nAccessed = {} # the result is a hash array

    # read the data from the json file
    with open(fileName) as data_file:    
        data = json.load(data_file)

    # generic full print (careful this can take very long)
    if debug>1:
        pprint.pprint(data)

    if debug>0:
        print " SiteName: " + data["SITENAME"]

    # loop through the full json data an extract the accesses per dataset
    for entry in data["DATA"]:
        key = entry["COLLNAME"]
        value = entry["NACC"]

        if debug>0:
            print " NAccess - %6d %s"%(value,key)
        # filter out non-AOD data
        if not re.search(r'/.*/.*/.*AOD.*',key):
            if debug>0:
                print ' WARNING -- rejecting non *AOD* type data: ' + key
            nSkipped += 1
            continue

        # here is where we assign the values to the hash (checking if duplicates are registered)
        if key in nAccessed:
            print ' WARNING - suspicious entry - the entry exists already (continue)'
            nAccessed[key] += value
        else:
            nAccessed[key] = value

    # return the datasets
    return (nSkipped, nAccessed)

def addData(nAllAccessed,nAccessed,debug=0):
    # adding a hash array (nAccessed) to the mother of all hash arrays (nAllAccessed)

    # loop through the hash array
    for key in nAccessed:
        # add the entries to our all access hash array
        if key in nAllAccessed:
            nAllAccessed[key] += nAccessed[key]
        else:
            nAllAccessed[key] = nAccessed[key]

    # return the updated all hash array
    return nAllAccessed

def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)

    # this is the text including the size units, that need to be converted
    sizeGb  = float(sizeTxt[0:-2])
    units   = sizeTxt[-2:]
    # decide what to do for the given unit
    if   units == 'MB':
        sizeGb = sizeGb/1024.
    elif units == 'GB':
        pass
    elif units == 'TB':
        sizeGb = sizeGb*1024.
    else:
        print ' ERROR - Could not identify size. EXIT!'
        sys.exit(0)

    # return the size in GB as a float
    return sizeGb

def findDatasetSize(dataset,debug=0):
    # find the file size of a given data set (dataset) and return the number of files in the dataset
    # and its size in GB

    cmd = './findDatasetProperties.py ' + dataset + ' short | tail -1'
    if debug>0:
        print ' CMD: ' + cmd
    nFiles = 0
    sizeGb = 0.
    for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
        line = line[:-1]
        g = line.split()
        try:
            nFiles = int(g[0])
            sizeGb = float(g[1])
        except:
            # when the decoding fails either there was an extra line or no information found
            # which means the counts are zero
            print ' WARNING - decoding failed: %d %f.3 -- %s'%(nFiles,sizeGb,dataset)
            pass
    
    return nFiles, sizeGb

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0
sizeAnalysis = True

usage  = "\n"
usage += " readJsonSnapshot.py  <sitePattern [ <datePattern>=????-??-?? ]\n"
usage += "\n"
usage += "   sitePattern  - pattern to select particular sites (ex. T2* or T2_U[SK]* etc.)\n"
usage += "   datePattern  - pattern to select date pattern (ex. 201[34]-0[0-7]-??)\n\n"

# decode command line parameters
if   len(sys.argv)<2:
    print ' ERROR - not enough arguments\n' + usage
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    date = '????-??-??'
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    date = str(sys.argv[2])
else:
    print ' ERROR - too many arguments\n' + usage
    sys.exit(2)

# figure out which files to consider
workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
files = glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)

# process each snapshot and create a list of datasets
nAllSkipped  = 0
nAllAccessed = {}
sizesGb      = {}
fileNumbers  = {}

print "\n = = = = S T A R T  A N A L Y S I S = = = =\n"
print " Logfiles in:  %s"%(workDirectory) 
print " Site pattern: %s"%(site) 
print " Date pattern: %s"%(date) 
if sizeAnalysis:
    print "\n Size Analysis is on. Please be patient, this might take a while!\n"
    

for fileName in files:
    if debug>0:
        print ' Analyzing: ' + fileName

    # analyze this file
    (nSkipped, nAccessed) = processFile(fileName,debug)

    # add the results to our 'all' record
    nAllSkipped += nSkipped
    nAllAccessed = addData(nAllAccessed,nAccessed,debug)

# create summary information and potentially print the contents
nAll        = 0
nAllAccess  = 0
sizeTotalGb = 0.
nFilesTotal = 0
for key in nAllAccessed:
    value = nAllAccessed[key]
    nAllAccess += value
    nAll       += 1
    if debug>0:
        print " NAccess - %6d %s"%(value,key)

    if sizeAnalysis:
        (nFiles,sizeGb) = findDatasetSize(key)
        # add to our memory
        fileNumbers[key] = nFiles
        sizesGb[key]     = sizeGb
        # add up the total data volume/nFiles
        sizeTotalGb     += sizeGb
        nFilesTotal     += nFiles
        
# printout the summary
print " "
print " = = = = S U M M A R Y = = = = "
print " "
print " - number of datasets:         %d"%(nAll) 
print " - number of accesses:         %d"%(nAllAccess) 
print " - number of skipped datasets: %d"%(nAllSkipped) 
if sizeAnalysis:
    print " - data volume [GB]:           %.3f"%(sizeTotalGb) 

# careful watch cases where no datasets were found
if nAll>0:
    print " "
    print " - number of accesses/dataset: %.2f"%(float(nAllAccess)/float(nAll)) 
if sizeAnalysis:
    if sizeTotalGb>0:
        print " - number of accesses/GB:      %.2f"%(float(nAllAccess)/sizeTotalGb)
    if nFilesTotal>0:
        print " - number of accesses/file:    %.2f"%(float(nAllAccess)/nFilesTotal)
print " "

# last step: produce monitoring information

## to be implemented

sys.exit(0)
