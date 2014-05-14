#!/usr/local/bin/python
#----------------------------------------------------------------------------------------------------
#
# This script looks at the results of deletion suggestions for a specified site and actually asks
# PhEDEx for deletions.  The PhEDEx communications are handled inside phedexApi class.
#
# ToDo: If needed, we will add a mechanism to maintain a local list of submitted subscriptions so
# that we will NOT ask again if it looks like there is orginal request submitted earlier still
# pending for approval.
#
#----------------------------------------------------------------------------------------------------
import sys, os, re, glob, time, glob, shutil, pickle
import phedexApi, siteStatus


if len(sys.argv) < 2:
    allSites = siteStatus.getAllSites()
else:
    strAllSites = str(sys.argv[1])
    allSites = pickle.loads(strAllSites)

datasetInfo = {}
datasetGroup = {}

#====================================================================================================
#  H E L P E R S
#====================================================================================================
def getDatasetGroup(site):
    datasetGroup.clear();
    groupName = ''

    # need to open phedex info to access group info
    statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
    inputFile = statusDirectory + '/'+os.environ['DETOX_PHEDEX_CACHE']
    fileHandle = open(inputFile,"r")
    
    for line in fileHandle.xreadlines():
        items = line.split()
        datasetName = items[0]
        items.remove(datasetName)
        
        group = items[0]
        creationDate = int(items[1])
        size = float(items[2])
        t2Site = items[3]
        
        if site != t2Site:
            continue
        datasetGroup[datasetName] = group

    fileHandle.close()


def submitRequest(site, datasets=[]):

    if len(datasets) < 1:
        print " ERROR - Trying to submit empty request for " + site
        return

    phedex = phedexApi.phedexApi(logPath='./')

    # compose data for deletion request
    check,data = phedex.xmlData(datasets=datasets,instance='prod')

    if check: 
        print " ERROR - phedexApi.xmlData failed"
        sys.exit(1)
    
    # here the request is really sent
    message = 'IntelROCCS -- Automatic Cache Release Request (if not acted upon will repeat ' + \
              'in about %s hours).'%(os.environ['DETOX_CYCLE_HOURS']) + \
              ' Summary at: http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/'
    check,response = phedex.delete(node=site,data=data,comments=message,instance='prod')
    if check:
        print " ERROR - phedexApi.delete failed"
        print response
        sys.exit(1)

    respo = response.read()
    matchObj = re.search(r'"id":"(\d+)"',respo)
    id = int(matchObj.group(1))
    
    matchObj = re.search(r'"request_date":"(.*?)"',respo)
    date = matchObj.group(1)
    date = date[:-3]
    myCnf = os.environ['DETOX_MYSQL_CONFIG']

    for dataset in datasets:
        rank = float(datasetInfo[dataset][0])
        size = float(datasetInfo[dataset][1])
        group = datasetGroup[dataset]
        
        db = MySQLdb.connect(read_default_file=myCnf,read_default_group="mysql")
        cursor = db.cursor()
        sql = "insert into Requests(RequestId,RequestType,SiteName,Dataset,Size,Rank,GroupName," + \
              "TimeStamp) values ('%d', '%d', '%s', '%s', '%d', '%d', '%s', '%s' )" % \
              (id, 1, site, dataset,size,rank,group,date)

        # ! this could be done in one line but it is just nice to see what is deleted !
        try:
            cursor.execute(sql)
            db.commit()
        except:
            print "caught an exception"
            db.rollback()
        
        db.close()


#===================================================================================================
#  M A I N
#===================================================================================================
if __name__ == '__main__':
    """
__main__

"""

if not os.environ.get('DETOX_DB'):
    print '\n ERROR - DETOX environment not defined: source setup.sh\n'
    sys.exit(0)

# directories we work in
statusDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_STATUS']
resultDirectory = os.environ['DETOX_DB'] + '/' + os.environ['DETOX_RESULT']


# record active sites in a file
activeSites = os.environ['DETOX_DB'] + '/ActiveSites.txt'
fileHandle = open(activeSites,"w")
for site in allSites.keys():
    if allSites[site].getStatus() == 1:
        fileHandle.write(site + "\n")
fileHandle.close()

# prepare for deletion by site
deletionFile = "DeleteDatasets.txt"

# hash of sites and corresponding dataset deletion list
siteDeletionList = {}

# look at the results, and for each site get a list to be deleted
allSubDirs = glob.glob(resultDirectory + "/T*")
for member in allSubDirs:
    site = member.split('/')[-1]
    inputFile = resultDirectory + '/' + site + '/' + deletionFile

    print " File: " + inputFile
    fileHandle = open(inputFile,"r")
    for line in fileHandle.xreadlines():
        items = line.split()

	# CP-CP this is bad decoding prone to failure
        if len(items) != 5:
            continue
        dataset = items[4]
        print ' -> ' + dataset
        if not dataset.startswith('/'):
            continue
        datasetInfo[dataset] = [items[0],items[1]]
        if site in siteDeletionList.keys():
            siteDeletionList[site].append(dataset)
        else:
            siteDeletionList[site] = [dataset]
    fileHandle.close()

# now submit actual requests
for site in siteDeletionList.keys():

    if site not in allSites.keys():
        continue
    if allSites[site].getStatus() == 0:
        continue

    getDatasetGroup(site)
        
    print "Deletion request for site " + site
    print siteDeletionList[site]
    submitRequest(site,siteDeletionList[site])
