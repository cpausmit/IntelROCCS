#===================================================================================================
#  C L A S S E S  concerning the information about a phedex request
#===================================================================================================
import sys, os, re, time, glob
from   time      import mktime
from   xml.etree import ElementTree

#---------------------------------------------------------------------------------------------------
"""
Class:  PhedexRequestInfo(requestId='')
Each phedex request has a number of characteristics which can be convieniently accessed through this
interface.
"""
#---------------------------------------------------------------------------------------------------
class PhedexRequestInfo:
    "A PhedexRequestINfo defines all needed request information."

    def __init__(self, requestId):
        # for a given request Id read the full information into a xml structure

        self.info = None # initialize to empty object


        file = 'phedex-' + requestId + '.xml' 
        cmd = "wget --no-check-certificate -O " + file + " " \
            + "https://cmsweb.cern.ch/phedex/datasvc/xml/prod/transferrequests?request=" \
            + requestId + " 2> /dev/null"
        print " CMD: " + cmd
        os.system(cmd)
    
        # initialize our container
        document = {}
        try:                                                      # check that we can parse the xml file
            document = ElementTree.parse(file)
        except:
            print ' WARNING - file reading failed (%s).'%file
    
        # cleanup the file we used
        os.system("rm -f " + file)
    
        if len(document.findall('request')) == 1:
            self.info = document.findall('request')[0]
            
    def isValid(self):
        if self.info != None:
            return True
        else:
            return False

    def isMove(self):
        # for a given xml structure of a requestInfo extract whether it is a move
        return self.info.attrib['move'] == 'y'
    
    def getGroup(self):
        # for a given xml structure of a requestInfo extract responsible group
        return self.info.attrib['group']
    
    def getSources(self):
        # for a given xml structure of a requestInfo extract the source(s)
    
        sources = []
        for source in (self.info).findall('move_sources'):
            for node in source.findall('node'):
                site = node.attrib['name']
                #print ' Move_Source: ' + site
                sources.append(site)
        return sources
    
    def getDestinations(self):
        # for a given xml structure of a requestInfo extract the destination(s)
    
        destinations = []
        for destination in self.info.findall('destinations'):
            for node in destination.findall('node'):
                site = node.attrib['name']
                #print ' Destination: ' + site
                destinations.append(site)
        return destinations
    
    def getDatasets(self):
        # for a given xml structure of a requestInfo extract the datasets affected
    
        datasets = []
        for data in self.info.findall('data'):
            for dbs in data.findall('dbs'):
                for dataset in dbs.findall('dataset'):
                    datasetName= dataset.attrib['name']
                    #print ' Dataset: ' + datasetName
                    datasets.append(datasetName)
        return datasets
