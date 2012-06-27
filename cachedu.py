#!/usr/bin/env python

import os
import sys

import errno
import redis

rdb = redis.StrictRedis(host='localhost', port=6379, db=0)

def hashpath(path):
	return hashlib.md5(path).hexdigest()

def store(path):
	try:
		st = os.stat(path)
	except OSError as e:
		if e.errno == errno.ENOENT:
			print "no such file or directory: ", path 
			return 0	

	rdb.hset(path,'size',st.st_size)
	rdb.hset(path,'mtime',st.st_mtime)
	rdb.hset(path,'basename', os.path.basename(path))
	rdb.expire(path,3600) #set a 1 hour expiry on all keys
	
	return st.st_size

def fetch(path):
	return rdb.hget(path,'size')

def check(path):
	if rdb.exists(path):
		return True
	else:
		return False

def visit(path):
	fsize = 0
	if check(path):
		fsize = int(fetch(path))
	else:
		fsize = int(store(path))
	return fsize

def walk(wp):
	totalsize=0
	for dirname,dirnames,filenames in os.walk(wp):
		for subdirname in dirnames:
			dn = os.path.join(dirname, subdirname)
			#print "DIR:", dn
			totalsize += 4096
#			rdb.sadd(wp,dn)
		for filename in filenames:
			fn = os.path.join(dirname, filename)
#			rdb.sadd(wp,fn)
			#print "FIL:", fn
			totalsize += visit(fn)
			
	return totalsize

def totalkeys(listofkeys):
	total = 0
#	print "L", len(listofkeys)
	for key in listofkeys:
#		print "K: ",key
		value = int(rdb.hget(key,'size'))
#		print "V: ",value
		total += value
	return total
	

if __name__=="__main__":
	walkpath = sys.argv[1]
	totalSize = 0
	if rdb.exists(walkpath):
		keypath = "%s*" % walkpath
		print keypath
		allkeys = rdb.keys(keypath)
		print "found %s cached values for %s" % (len(allkeys),walkpath)
		totalSize = totalkeys(allkeys)
	else:
#		rdb.sadd(walkpath,'.')
		totalSize += store(walkpath)
		print "no cached keys found"
		totalSize = walk(walkpath)
	
	print totalSize, " Bytes"
	print totalSize/1024, " KB"
	print totalSize/(1024*1024), " MB"
	print totalSize/(1024*1024*1024), " GB"
