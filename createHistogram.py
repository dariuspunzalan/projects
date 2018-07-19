#!/usr/bin/python

#################################################
#createhistogram.py				#
#version: 1.0					#
#Author: Darius Punzalan			#
#Date: 24-May-2018				#
#################################################

import sys, threading, json, time
import logging, fcntl, math
try:
	import bsddb
except:
	import bsddb3 as bsddb
	
from urllib2 import urlopen
from Queue import Queue
from IPy import IP

start = time.time()
datetag = time.strftime("%y%m%d%H%M%S")
basename = 'createHistogram'
ofile = '%s_%s.tsv' % (basename, datetag)

lock = threading.RLock()
invalidip, duplicateip, failgeolookup, failtemp = 0, 0, 0, 0
ltemp, htemp = None, None
ipdb = bsddb.hashopen('%s.db' % basename, 'n')
histdict={}

logging.basicConfig(filename='%s.log'% basename,
			level=logging.DEBUG,
			format='%(asctime)s %(name)-5s %(levelname)-3s %(message)s',
	)
logger= logging.getLogger(__name__)

# Load the configuration file from config/createHistogram_config.py
try:
	import createHistogram_config as cfg

	geourls = cfg.Default['geourls']
	weatherurl = cfg.Default['weatherurl']
	apiid = cfg.Default['apiid']
	thread_count = cfg.Default['thread_count']
	Qmax = cfg.Default['Queue_maxsize']

except:
	logger.error('Unable to load required config - %s' % sys.exc_info()[0] )
	sys.exit()

qin=Queue(maxsize=Qmax)

def checkURLs(URL):
	try:
		urldata=urlopen(URL, timeout=1)
		if urlopen(URL).code == 200:
			return URL
		else:
			return None
	except:
		return None


def readlog(inputfile, qin):
	try:
		infile = open(inputfile)
	except IOError:
		print 'Unable to read the file %s.' % inputfile
		logger.error('Unable to read the file %s.' % inputfile)
		sys.exit()
	else:
		for line in infile:
			qin.put(line)
def checkIP(ip):
	"""
	Check if IP is valid and fetch the geo coordinates.
	returns Longitude and Latitude in list form on successful lookup
	"""

	global invalidip
	global duplicateip
	global failgeolookup

	try:
		if IP(ip).iptype() is 'PUBLIC':
                    if ip in ipdb:
                        duplicateip += 1
                        return
                    else:
                        ipdb[ip] = ''
                else:
                    with lock:
                        invalidip += 1
                    logger.info('IP address is not public %s' % ip)
                    return
	except ValueError:
		with lock:
			invalidip += 1
		logger.info('Invalid IP address %s' % ip)
		return
	
        try:
		gdata = urlopen(geourl + ip, timeout=1)
	except:
		logger.error("Unable to query coordinates: %s" % sys.exc_info()[0])
		with lock:
			failgeolookup += 1
		return
	else:
		data = json.load(gdata)
		if data['lon'] != '' or data['lat'] != '':
			return([str(data['lon']), str(data['lat'])])
		else:
			logger.info("Unable to get coordinates for %s" % ip)
			with lock:
				failgeolookup += 1

def querytemp(coordinates, i):
	"""
	Takes geo coodinates as input and provide the next day's  maximum forecasted temperature of
	the location.
	"""

	global ltemp
	global htemp
	global failtemp
	lat = coordinates[1]
	lon = coordinates[0]
	wquery = '%s/%s/%s,%s?exclude=hourly,currently.minutely,alerts,flags&units=us' % (weatherurl, apiid, lat, lon)
	try:
		wdata = urlopen(wquery, timeout=1)
	except:
		failtemp += 1
		logger.error("Unable to query temperature:%s -  %s" % (wquery, sys.exc_info()[0]))
		return
	else:
		data = json.load(wdata)
		maxtemp = data['daily']['data'][1]['temperatureHigh']
		with lock:
			if ltemp == None or maxtemp < ltemp:
				ltemp = math.floor(maxtemp)
			if htemp == None or maxtemp > htemp:
				htemp = math.ceil(maxtemp)
		return(maxtemp)

def histogram(ofile, buckets):
	global ltemp
	global htemp
	f = open(ofile, 'w')
	f.write("bucketMin\tbucketMax\tcount\n")
	delta = round(((htemp - ltemp)/ buckets), 2) - .01
        min = ltemp
        for x in range(1, (buckets + 1)):
            max = min + delta
            histdict[max] = 0
            min = max + .01

	for i, temp in ipdb.iteritems():
		if temp:
			min=ltemp
			for x in sorted(histdict):
				if float(temp) <= x :
					histdict[x] += 1
                                        break
				else:
					continue
				
	for maxB, value in sorted(histdict.iteritems()):
		minB = maxB - delta
		f.write("%.2f\t%.2f\t%d \n" %(minB, maxB, value))
	print 'Output file: %s' % ofile
	f.close()


def process(qin, i):
	global qweathercount
	while True:
		line = qin.get()
		ip = line.split('\t')[23]
#		logger.info('Thread-%d : Checking %s' % (i, ip))
		coordinates=checkIP(ip)
		if coordinates is not None:
			maxtemp = querytemp(coordinates, i)
			if maxtemp:
				ipdb[ip] = str(maxtemp)
			else:
				ipdb[ip] = ''
		qin.task_done()

if __name__ == "__main__":
	try:
		buckets = int(sys.argv[2])
		inputfile = sys.argv[1]
	except:
		print 'Syntax: %s <inputfile> <bucket_count>' % sys.argv[0]
		print 'Example: %s to_read.log 5' % sys.argv[0]
		sys.exit()

	try:
		plock = open('/tmp/%s.lock' % basename, 'w')
		fcntl.flock(plock, fcntl.LOCK_EX | fcntl.LOCK_NB)
	except IOError as err:
		logger.warn('Another instance of %s is running...' %sys.argv[0])
		sys.exit()

	for uAddress in geourls:
		geourl = checkURLs(uAddress)
		if geourl:
			break
		else:
			continue
	if geourl is None:
		logger.error('No valid url to check IP address location')
                sys.exit()

	for i in range(thread_count):
		pproc=threading.Thread(target=process, args=(qin, i))
		pproc.setDaemon(True)
		pproc.start()

	#threading.Thread(target=readlog, args=(inputfile, qin)).start()
	print '****** Starting to fetch information ****'
	logger.info('%s started processing file %s using %d threads' % (sys.argv[0], inputfile, thread_count))

	readlog(inputfile, qin)

	qin.join()
	uniqueip = len(ipdb)
	totalip = uniqueip + invalidip + duplicateip
	
	print 'Summary Report:'
	print 'Total IP: {}'.format(totalip)
	print 'Total Unique IPs: {} / {:.2%}'.format(uniqueip, uniqueip/float(totalip))
	print 'Invalid IP: {} / {:.2%}'.format(invalidip, invalidip/float(totalip))
	print 'Duplicate IP: {} / {:.2%}'.format(duplicateip, duplicateip/float(totalip))
	print 'Failed Geo Lookup: {} / {:.2%} failed/unique IP'.format(failgeolookup, failgeolookup/float(uniqueip))
	print 'Failed Temp lookup:{} / {:.2%} failed/unique IP'.format(failtemp, failtemp/float(uniqueip))
	
	if htemp == None or ltemp == None:
		print 'No data to create histogram'
		logger.warn('%s ended without result due to no data were captured' % sys.argv[0])
	else:
		histogram(ofile, buckets)
		elapsedtime = time.time() - start
		logger.info('Complete: Output file: %s with elapsed time %.2f seconds' % (ofile, elapsedtime))
		print 'Elapsed time: %.2f seconds' % elapsedtime
	print '***** Done *****'
fcntl.flock(plock, fcntl.LOCK_UN)
