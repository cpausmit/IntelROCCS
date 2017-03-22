#!/usr/bin/python

from time import strftime,gmtime
import time
import re
from copy import deepcopy

'''Dataset object definition
		and common functions to 
		manipulate Datasets'''

# genesis=int(time.mktime(time.strptime("2014-09-01","%Y-%m-%d")))
genesis=1378008000
now = time.time()

class Request(object):
	def __init__(self,is_xfer,timestamp,node):
		self.is_xfer = is_xfer
		self.timestamp = timestamp
		self.node = node
	def __cmp__(self,other):
		return cmp(self.timestamp,other.timestamp)
	def __str__(self):
		return '%s %s at %i'%('Transferred to' if self.is_xfer else 'Deleted at',
				                     self.node, self.timestamp)

class Dataset(object):
		siteList = []
		"""Object containing relevant dataset properties"""
		def __init__(self, name):
				self.name = name
				self.nFiles = -1
				self.sizeGB = -1
				self.nAccesses = {}
				self.requests = {}
				self.cTime = genesis
				self.nSites = -1
				self.isDeleted = None
		def getTimeOnSites(self,start,end,site_pattern='.*',skip_tape=True):
			start = max(self.cTime,start)
			end = min(now,end)
			interval = float(end-start)
			time_by_site = {}
			for s,_reqs in self.requests.iteritems():
				if not re.match(site_pattern,s):
					continue
				if skip_tape and 'MSS' in s:
					continue
				time_by_site[s] = 0
				reqs = deepcopy(_reqs)
				while (not reqs[0].is_xfer):
					reqs.pop(0)
					if not len(reqs):
						break
				while len(reqs)>0:
					if not reqs[0].is_xfer:
						reqs.pop(0)
						continue
					xreq = reqs[0]
					dreq = None
					to_remove = []
					for r in reqs[1:]:
						if r.is_xfer:
							to_remove.append(r)
							continue
						dreq = r
						break
					for r in to_remove:
						reqs.remove(r)
					if dreq:
						existed = min(end,max(start,dreq.timestamp)) - min(end,max(start,xreq.timestamp))
						reqs.remove(dreq)
					else:
						existed = end - min(end,max(start,xreq.timestamp))
					time_by_site[s] += existed/interval
					reqs.remove(xreq)
			return time_by_site
		def addTransfer(self,site_name,t):
			if site_name not in self.requests:
				self.requests[site_name] = []
			self.requests[site_name].append( Request(True,t,site_name) )
		def addDeletion(self,site_name,t):
			if site_name not in self.requests:
				self.requests[site_name] = []
			self.requests[site_name].append( Request(False,t,site_name) )
		def sortRequests(self):
			for s in self.requests:
				self.requests[s] = sorted(self.requests[s])
		def addAccesses(self,siteName,n,utime):
				if n==0:
						return
				if siteName not in self.nAccesses:
						self.nAccesses[siteName]=[]
				siteAccess = self.nAccesses[siteName]
				siteAccess.append((utime,n))
		def getTotalAccesses(self,start=-1,end=-1,site_pattern='.*',skip_tape=True):
			r=0
			for node,accesses in self.nAccesses.iteritems():
				if not re.match(site_pattern,node):
					continue
				if skip_tape and 'MSS' in node:
					continue
				for t,n in accesses:
					if t<start:
						continue
					if end>0 and t>end:
						continue
					r += n
			return r
		def __str__(self):
				s = "================================================\n"
				s += self.name
				s += "\n\t nFiles = %i"%(self.nFiles)
				s += "\t sizeGB = %.2f"%(self.sizeGB)
				s += "\t cTime = %s\n"%(strftime("%Y-%m-%d",gmtime(self.cTime)))
				s += "\t Movement history:\n"
				for site,rs in self.requests.iteritems():
					for r in rs:
						s += '\n\t\t %s'%(str(r))
				s += "\n================================================"
				return s
