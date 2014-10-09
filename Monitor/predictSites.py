#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# Script to predict the number of sites on which a given dataset existed in a given time interval
# based on the Phedex history
#
#---------------------------------------------------------------------------------------------------
import sys, re, time, glob, fnmatch
from   xml.etree import ElementTree

usage = '\n usage: ./predictSites.py\n'

# check command line arguments
# ============================
if len(sys.argv) != 1:
    print usage
    sys.exit(1)

# define variables
# ================
datasetInfo    = '/local/cmsprod/IntelROCCS/Detox/status/DatasetsInPhedexAtSites.dat' # FIX: use current file
phedDirectory  = '/local/cmsprod/IntelROCCS/Monitor/datasets/' # FIX: use env from setupMonitor.sh
xmlFiles       = []
nowish         = int(round(time.time()/300)*300)               # now to the nearest five min
phedData       = {}
predictedSites = {}
actualSites    = {}

# find current datasets, locations
# ================================
for line in open(datasetInfo,'r'):
    dataset = line.split()[0]
    site    = line.split()[7]

    # DEBUGGING - limit datasets to look at
    if not fnmatch.fnmatch(dataset.split('/')[1],'GluGluToHTo*'):
        continue

    # only interested in AOD files
    if not fnmatch.fnmatch(dataset.split('/')[-1],'AOD*'):
        continue

    if not dataset in actualSites:
        actualSites[dataset] = []

    if not site in actualSites[dataset]:
        actualSites[dataset].append(site)
        # append site to list of files to look for in phedex history
        xmlFiles.append(phedDirectory + dataset.replace('/','%'))

# retrieve from phedex xfer, del history of current datasets
# ==========================================================
for file in xmlFiles:
    #print file
    try:
        document = ElementTree.parse(file)
    except:
        print ' WARNING: failed to read %s'%(file)
        continue
    dataset = file.split('/')[-1].replace('%','/')

    if not dataset in phedData:
        phedData[dataset] = {}

    for request in document.findall('request'):

        nSubRqsts = len(request)
        if nSubRqsts > 4:
            print ' Number of items in request %s: %d'%(request.attrib['id'],nSubRqsts)
            #continue


        type = request.attrib['type']


        for node in request.findall('node'):
            site = node.attrib['name']
            t    = node.attrib['time_decided']

            if not re.match('T2',site):                  # only to requested sites
                #print ' WARNING - no site match.'
                #continue
                pass

            if node.attrib['decision']:
                try:
                    t = int(float(t))
                except:
                    #print ' WARNING: time corrupted for %s on %s'%(dataset,site)
                    continue

                if not site in phedData:
                    phedData[dataset][site] = [[],[]]

                if type == 'xfer':
                    phedData[dataset][site][0].append(t)
                    if nSubRqsts > 4:
                        print ' xfer - SITE: %d  %s'%(t,site)
                elif type == 'delete':
                    phedData[dataset][site][1].append(t)
                    if nSubRqsts > 4:
                        print ' dele - SITE: %d  %s'%(t,site)
                else:
                    continue

# use phedex data to construct prediction of current datasets, locations
# ======================================================================
for dataset in phedData:
    if not dataset in predictedSites:
        predictedSites[dataset] = []
        
    for site in phedData[dataset]:
        # ensure reasonable phedData entry
        phedData[dataset][site][0].sort()
        phedData[dataset][site][1].sort()

        lenXfer = len(phedData[dataset][site][0])
        lenDel  = len(phedData[dataset][site][1])

        if lenXfer == lenDel:
            pass
        elif lenXfer == lenDel - 1:
            phedData[dataset][site][0].insert(0,0)
        elif lenXfer == lenDel + 1:
            phedData[dataset][site][1].append(nowish)
        else:
            continue

        i = 0
        while i < lenXfer:
            
            tXfer = phedData[dataset][site][0][i]
            tDel  = phedData[dataset][site][1][i]

            if (tXfer <= nowish <= tDel):
                if not site in predictedSites[dataset]:
                    predictedSites[dataset].append(site)
            i += 1

# find difference between prediction and actual dataset presence
# ==============================================================
predictedN = {}
actualN    = {}
dN         = {}
integral   = 0
nMissing   = 0
for dataset in actualSites:
    if not dataset in predictedSites:
        print ' WARNING: %s not in predicted sites'%(dataset)
        nMissing += 1
        continue
    
    actualN[dataset]    = len(actualSites[dataset])
    predictedN[dataset] = len(predictedSites[dataset])

    d = predictedN[dataset] - actualN[dataset]
    if not d in dN:
        dN[d] = 0
    dN[d] += 1

    if d != 0:
        print ' ---- Difference: %d  --> %s'%(d,dataset)
        print ' ACTUAL    ', actualSites[dataset]
        print ' PREDICTED ', predictedSites[dataset]


print '\n number of datasets not found in phedex: %d\n'%(nMissing)

# print results
# =============
for d in dN:
    integral += dN[d]
    print ' %d \t%d'%(d,dN[d])

print '\n integral: %d\n'%(integral)
