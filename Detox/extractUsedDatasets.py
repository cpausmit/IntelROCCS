#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script unifies information extracted from the popularity service and phedex database. It
# reads in datasets reported by PhEDEx and from popularity service determines when/if the dataset
# was used how many times.  At the moment this script just calculates the total numbers of accesses
# over the past 8 months. If in the future the ranking formular is to be modified to count accesses
# on a sliding scale this script would need to be unified with the ranking algorithm.
#
# This script will add a site to the sites that are dropped from consideration if it discoveres that
# the snapshots from the ppopularity service are corrupted for this site (there is at least one site
# for which popularity service does not report anything)
#
#----------------------------------------------------------------------------------------------------
import os, sys, re, glob, time, datetime

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

if len(sys.argv) < 2:
    print 'not enough arguments\n'
    sys.exit()
else:
    site = str(sys.argv[1])

debug = 0

datasets = {}
excludedSitesList = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS'] + '/' + \
                    os.environ['DETOX_EXCLUDED_SITES']

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def processFiles(fileName):

    date = (re.search(r"(\d+)-(\d+)-(\d+)", fileName)).group(0)  

    # make sure file is clean
    rawinp = None
    isFileCorrupt = False
    inputFile = open(fileName,'r')
    for line in inputFile.xreadlines():
        if "SITENAME" in line:
            rawinp = line
        if "Traceback" in line:
            isFileCorrupt = True
        if "Error" in line:
            isFileCorrupt = True
    inputFile.close();

    # >> CP-CP test that all variables are properly set
    #
    # one of the big issues with local ASCII caches are identification of failures. We have to check
    # whether the output is empty because this seems to be one reason for failure. I am not sure
    # what the best way is but this has to be handled better.
    #
    if rawinp == None:
        isFileCorrupt = True
    #
    # this particular failure mode is not a file corruption but rather that there are no datasets
    # we should not exclude this site....
    # 
    # << CP-CP test that all variables are properly set

    # deal with file corruption
    if isFileCorrupt:
        print fileName + " has corrupted snapshot"
        foundline = False
        outputFile = open(excludedSitesList,'r+')
        while True:
            line = outputFile.readline()
            if re.search(site, line):
                foundline = True
            if not line:
                if not foundline:
                    outputFile.write(site+"\n")
                outputFile.close()
                sys.exit(0)
        outputFile.close()
    
    # process good snapshots
    array = re.split('{',rawinp)
    for line in array:
        aka = re.split(',',line)
	dataset = None
        nAccessed = None
	
        for field in aka:
            field = re.sub('[:\"}\]]', '', field)
            field = re.sub('^\s', '', field)
            ta = re.split('\s+',field)

            if len(ta) < 2:
                continue

            one = ta[0]
            two = ta[1]

	    if one == 'COLLNAME':
                dataset = two
	    if one == 'NACC':
                nAccessed = int(two)
	
        if dataset == None:
            continue;

        if dataset in datasets.keys() :
            dateTime = (datasets[dataset])[1]
            prepoch = int(time.mktime(time.strptime(dateTime, "%Y-%m-%d")))
            now = int(time.mktime(time.strptime(date, "%Y-%m-%d")))
            nAccessed = (datasets[dataset])[0] + nAccessed
            
	    if now > prepoch:
                datasets[dataset] = [nAccessed,date]
            else:
                datasets[dataset] = [nAccessed,dateTime]
	else:
	    datasets[dataset] = [nAccessed,date]

#====================================================================================================
#  M A I N
#====================================================================================================
# always first reset the excluded sites (no site can be excluded before an error is found)
os.system("rm -f " + excludedSitesList)
if not os.path.exists(excludedSitesList):
    open(excludedSitesList,'a').close()

workDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
files = glob.glob(workDirectory + '/' + site + '/' + os.environ['DETOX_SNAPSHOTS'] + '/????-??-??')

# process each snapshot
for fileName in files:
    if debug>0:
        print ' File: ' + fileName
    processFiles(fileName)

# write result into forseen cache
outputFile = open(workDirectory+'/'+site+'/' + os.environ['DETOX_USED_DATASETS'], 'w')
for fileName in datasets:
    nAccessed = (datasets[fileName])[0]
    date = (datasets[fileName])[1]
    outputFile.write(fileName + " " + str(nAccessed) + " " + date + "\n");
outputFile.close();
