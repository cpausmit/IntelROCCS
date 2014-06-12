#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script reads the information from a JSON snapshot file that we use to cache our popularity
# information.
#
#---------------------------------------------------------------------------------------------------
import os, sys, glob, subprocess, json, pprint
#import pyroot

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

if   len(sys.argv)<2:
    print 'not enough arguments\n'
    sys.exit(1)
elif len(sys.argv)==2:
    site = str(sys.argv[1])
    date = '????-??-??'
elif len(sys.argv)==3:
    site = str(sys.argv[1])
    date = str(sys.argv[2])
else:
    print 'too many arguments\n'
    sys.exit(2)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def processFiles(fileName,debug=0):
    # processing the contents of a simpla file into a hash array

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
        # here is where we assignt he values to the hash (checking if duplicates are registered)
        if key in nAccessed:
            print ' WARNING - suspicious entry - the entry exists already (continue)'
            nAccessed[key] += value
        else:
            nAccessed[key] = value

    # return the datasets
    return nAccessed

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

#===================================================================================================
#  M A I N
#===================================================================================================
debug = 0

# figure out which files to consider
workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
files = glob.glob(workDirectory + '/' + site + '*/' + os.environ['DETOX_SNAPSHOTS'] + '/' + date)

# process each snapshot and create a list of datasets
nAllAccessed = {}

print '\n = = = = S T A R T  A N A L Y S I S = = = =\n'
for fileName in files:
    if debug>0:
        print ' DEBUGGING -- File: ' + fileName
    print ' Analyzing: ' + fileName
    nAccessed = processFiles(fileName,debug)
    nAllAccessed = addData(nAllAccessed,nAccessed,debug)

# create summary information and potentially print the contents
nAll        = 0
nAllAccess  = 0
sizeTotalGb = 0.
sizes       = {}
for key in nAllAccessed:
    value = nAllAccessed[key]
    nAllAccess += value
    nAll       += 1
    if debug>0:
        print " NAccess - %6d %s"%(value,key)

    ## use das client to find the present size of the dataset
    #cmd = 'das_client.py --format=plain --limit=0 --query="file dataset=' + \
    #      key + ' | sum(file.size)" |tail -1 | cut -d= -f2'
    #print ' CMD: ' + cmd
    #for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
    #    line = line[:-1]
    #
    ## this is the text including the size units, that needs to be converted)
    #if line != '':
    #    sizeGb = convertSizeToGb(line)
    #else:
    #    print ' Error - no reasonable size found with das_client.py.'
    #    sys.exit(1)
    #
    #sizeTotalGb += sizeGb
    #sizes[key]   = sizeGb


# printout the summary
print " "
print " = = = = S U M M A R Y = = = = "
print " "
print " Logfiles in:  %s"%(workDirectory) 
print " Site pattern: %s"%(site) 
print " Date pattern: %s"%(date) 
print " "
print " - number of datasets:         %d"%(nAll) 
print " - number of accesses:         %d"%(nAllAccess) 
print " - data volume [GB]:           %.3f"%(sizeTotalGb) 

# careful watch cases where no datasets were found
if nAll>0:
    print " "
    print " - number of accesses/dataset: %.2f"%(float(nAllAccess)/float(nAll)) 
if sizeTotalGb>0:
    print " - number of accesses/GB:      %.2f"%(float(nAllAccess)/sizeTotalGb) 
print " "

sys.exit(0)
