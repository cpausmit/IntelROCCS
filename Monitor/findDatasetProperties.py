#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script uses das_client.py to extract the given dataset properties. It will determine the
# number of files and the dataset size.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb

short = False
if   len(sys.argv)<2:
    print 'not enough arguments\n'
    sys.exit(1)
elif len(sys.argv)==2:
    dataset = str(sys.argv[1])
elif len(sys.argv)==3:
    dataset = str(sys.argv[1])
    short = True
else:
    print 'too many arguments\n'
    sys.exit(2)

#===================================================================================================
#  H E L P E R S
#===================================================================================================
def getDbCursor():
    # configuration
    db = os.environ.get('DETOX_SITESTORAGE_DB')
    server = os.environ.get('DETOX_SITESTORAGE_SERVER')
    user = os.environ.get('DETOX_SITESTORAGE_USER')
    pw = os.environ.get('DETOX_SITESTORAGE_PW')
    # open database connection
    db = MySQLdb.connect(host=server,db=db, user=user,passwd=pw)
    # prepare a cursor object using cursor() method
    return db.cursor()

def getDatasetId(dataset):
    id = -1                          # initialize the id (-1 -> invalid)
    # get access to the database
    cursor = getDbCursor()
    sql = "select * from Datasets where DatasetName=\'" + dataset + "\'"
    # go ahead and try
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            id     = row[0]
    except:
        pass
    return id

def addDataset(dataset):
    # get access to the database
    cursor = getDbCursor()
    sql = "insert into Datasets (DatasetName) values (\'" + dataset + "\')"
    try:
        # Execute the SQL command
        print '        ' + sql
        cursor.execute(sql)
    except:
        print " Error (%s): unable to insert record into table."%(sql)
        return False
    return True

def addDatasetProperties(dataset,nFiles,sizeGb):
    # get access to the database
    cursor = getDbCursor()
    sql = "insert into DatasetProperties values " + \
          "((select DatasetId from Datasets where DatasetName='" + dataset + "'),%d,%f)" \
          %(nFiles,sizeGb)
    try:
        # Execute the SQL command
        print '        ' + sql
        cursor.execute(sql)
    except:
        print " Error (%s): unable to insert record into table."%(sql)
        return False
    return True

def checkDatabase(dataset):

    nFiles = -1
    sizeGb = 0.
    # get access to the database
    id = getDatasetId(dataset)
    if id==-1:
        if (addDataset(dataset)):
            print " Added the dataset %s successfully."%(dataset)
        else:
            print " Error: Dataset %s addition failed."%(dataset)
            sys.exit(1)
        id = getDatasetId(dataset)

    # make sure we have a valid id to continue
    if id==-1:
        print " Error: unable to assign proper Id to dataset (%s). Stopping here."%(dataset)
        sys.exit(3)

    # define sql
    cursor = getDbCursor()
    sql = "select * from DatasetProperties where DatasetId=" + str(id)
    # go ahead and try
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            id     = row[0]
            nFiles = row[1]
            sizeGb = row[2]
        #print " Properties -- Id:  %d  NFiles:  %d  Size:  %f.2"%(id,nFiles,sizeGb)
    except:
        print ' Info (%s) -- dataset properties not in database.'%(sql)

    return nFiles,sizeGb
            
def convertSizeToGb(sizeTxt):

    # first make sure string has proper basic format
    if len(sizeTxt) < 3:
        print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
        sys.exit(1)
    # this is the text including the size units, that need to be converted
    sizeGb = float(sizeTxt[0:-2])
    units  = sizeTxt[-2:]
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

# first make sure not to analyze any weird data (require /*/*/* name pattern)
if not re.search(r'/.*/.*/.*',dataset,re.S):
    print ' Error: Dataset does NOT match expected pattern'
    sys.exit(0)

# test whether we know this dataset already
(nFiles,sizeGb) = checkDatabase(dataset)

# check failed so need to go to the source
if nFiles<0:
    # use das client to find the present size of the dataset
    cmd = 'das_client.py --format=plain --limit=0 --query="file dataset=' + \
          dataset + ' | sum(file.size), count(file.name)" | sort -u'
    if debug>-1:
        print ' CMD: ' + cmd
    nFiles = 0
    sizeGb = 0.
    try:
        for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
            line = line[:-1]
            if   re.search('file.name',line):
                if debug>0:
                    print ' count ' + line
                nFiles = int(line.split("=")[1])
            elif re.search('file.size',line):
                if debug>0:
                    print ' size  ' + line
                size = line.split("=")[1]
                sizeGb = convertSizeToGb(size)
    except:
        print ' Error: output data not compliant.'
        sys.exit(0)

    # add it to our database
    addDatasetProperties(dataset,nFiles,sizeGb)

# some derived quantities
averageSizeGb = 0
if nFiles>0:
    averageSizeGb = sizeGb/nFiles

# give us the print
if short:
    print '%d %.1f %.3f %s'%(nFiles,sizeGb,averageSizeGb,dataset)
else:
    print ' nFiles:%d  size:%.1f GB [average file size:%.3f GB] -- dataset:%s'\
          %(nFiles,sizeGb,averageSizeGb,dataset)

sys.exit(0)
