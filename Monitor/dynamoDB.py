#!/usr/bin/python

import os, sys, re, subprocess, MySQLdb

def getDbCursor():
		db = MySQLdb.connect(read_default_file = '/etc/my.cnf', read_default_group = 'mysql-dynamo', db = 'dynamo')
		return db.cursor()

def getDatasetsAndProps(cursor=None,limit=''):
	if not cursor:
		cursor = getDbCursor()
	# sql = 'SELECT d.`name`, SUM(b.`num_files`), d.`size`, d.`last_update` FROM `datasets` AS d	 INNER JOIN `blocks` AS b ON b.`dataset_id` = d.`id`	 GROUP BY d.`id` %s;'%(limit)
	sql = ' SELECT d.`name`, d.`num_files` + SUM(b.`num_files`), d.`size`, d.`last_update` FROM `datasets` AS d	 INNER JOIN `blocks` AS b ON b.`dataset_id` = d.`id`	 GROUP BY d.`id`;'
	cursor.execute(sql)
	results = cursor.fetchall()
	print 'Query fetched %i results'%(len(results))
	return results

def getDatasetsAccesses(cursor=None,limit=''):
	if not cursor:
		cursor = getDbCursor()
	sql = 'SELECT d.`name`, s.`name`, da.`date`, da.`num_accesses` FROM `dataset_accesses` AS da	 INNER JOIN `datasets` AS d ON d.`id` = da.`dataset_id`	  INNER JOIN `sites` AS s ON s.`id` = da.`site_id`	 ORDER BY da.`date` ASC;'
	cursor.execute(sql)
	return cursor.fetchall()
