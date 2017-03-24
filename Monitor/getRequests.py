#!/usr/bin/env python

import os
from sys import argv
import json

def request(which,idx,outdir,nper=100):
    command = 'wget --no-check-certificate -O %s "https://cmsweb.cern.ch/phedex/datasvc/json/prod/%s" >/dev/null 2>/dev/null'

    if idx%20==0:
        print idx

    if which == 'transfer':
        api = 'transferrequests?approval=approved&'
    else:
        api = 'deleterequests?approval=approved&'

    outfile = '%s/requests_%s_%i.json'%(outdir,which,idx)

    try:
        f = open(outfile)
        payload = json.load(f)['phedex']['request'][0] # file contains at least one request!
    except:
        requests = '&'.join(['request=%i'%i for i in range(idx*nper,(idx+1)*nper)])
        args = api+requests

        os.system(command%(outfile,args))
        try:
            f = open(outfile)
            payload = json.load(f)['phedex']
            if len(payload['request'])==0:
                # there are no more to read
                if os.path.isfile(outfile):
                    os.system('rm -f %s'%outfile)
                return -1
            return 0
        except:
            if os.path.isfile(outfile):
                os.system('rm -f %s'%outfile)
            return 1

if __name__=="__main__":
    import config
    nper = 100
    which = argv[1]
    idx = int(argv[2])
    request(which,idx,config.requests_dirnper)
