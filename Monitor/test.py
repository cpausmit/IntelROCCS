#!/usr/bin/env python

from pprint import pprint
'''
import requestParse
d = requestParse.parseDeletion('/data/t3serv014/snarayan/requests_dir/requests_delete_8000.json')
pprint(d)
'''

import dynamoDB
import time

l = dynamoDB.getDatasetsAndProps()

ll = [(x[0],int(x[1]),x[2],time.mktime(x[3].timetuple())) for x in l[:10]]
pprint(ll)
