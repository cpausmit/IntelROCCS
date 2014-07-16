#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
#
# Collects all the necessary data to generate rankings for all available sites
#
#---------------------------------------------------------------------------------------------------
import sys, random

class select():
#    def __init__(self):

#===================================================================================================
#  H E L P E R S
#===================================================================================================
    def weightedChoice(self, choices):
        total = sum(w for c, w in choices.iteritems())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.iteritems():
            if upto + w > r:
                return c, w
            upto += w

#===================================================================================================
#  M A I N
#===================================================================================================
# Use this for testing purposes or as a script. 
# Usage: python ./select.py
if __name__ == '__main__':
    if not (len(sys.argv) == 1):
        print "Usage: python ./select.py"
        sys.exit(2)
    select = select()
    # Testcase
    choices = {'dataset1': 1, 'dataset2': 2, 'dataset3': 3}
    datasetName, ranking = select.weightedChoice(choices)
    print datasetName + " : " + str(ranking)
    sys.exit(0)