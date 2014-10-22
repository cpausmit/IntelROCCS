#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Queries database to get all sites on which AnalysisOps have quota. Can also return only those that
# are currently not blacklisted or those that are currently blacklisted.
#---------------------------------------------------------------------------------------------------
import sys
import dbApi

class sites():
	def __init__(self):
		self.dbApi = dbApi.dbApi()

	def getAllSites(self):
		# Change query when tables are updated
		query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Groups.GroupName=%s"
		values = ["AnalysisOps"
		data = self.dbApi.dbQuery(query, values=values)
		return [site[0] for site in data]

	def getBlacklistedSites(self):
		# Change query when tables are updated
		query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.Status=%s AND Groups.GroupName=%s"
		values = [0, "AnalysisOps"]
		data = self.dbApi.dbQuery(query, values=values)
		return [site[0] for site in data]

	def getAvailableSites(self):
		# Change query when tables are updated
		query = "SELECT Sites.SiteName FROM Sites INNER JOIN Quotas ON Sites.SiteId=Quotas.SiteId INNER JOIN Groups ON Groups.GroupId=Quotas.GroupId WHERE Sites.Status=%s AND Groups.GroupName=%s AND Sites.SiteName!=%s"
		values = [1, "AnalysisOps", "T1_US_FNAL_Disk"]]
		data = self.dbApi.dbQuery(query, values=values)
		return [site[0] for site in data]
