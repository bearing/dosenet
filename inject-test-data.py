#!/usr/bin/env python
# Navrit Bal
# Tigran Ter-Stepanyan
#
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sun 1/03/15
# Last updated: Mon 03/08/15

import MySQLdb as mdb
import time
from random import randrange, randint, random
import datetime
import math
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ID',type=int,nargs=1,required=True,
    help='')
parser.add_argument('--iterations',type=int,nargs=1,required=False,
    help='Intervals of 5 minutes, eg. 2016 = 1 week')
parser.add_argument('--weeks',type=float,nargs=1,required=False,
	help='Number[Int] of weeks to inject, eg. 4 = 1 month')
args = parser.parse_args()

db = mdb.connect("127.0.0.1",
				"ne170group",
				"ne170groupSpring2015",
				"dosimeter_network")
cursor = db.cursor()
# Globals
iterations = 2016 # Default is 1 week (10080 mins / 5 mins)
stationID = args.ID[0]
if args.iterations:
	iterations = args.iterations[0]
if args.weeks:
	iterations = args.weeks[0]*10080/5 # number of minutes in a week / 5 minute interval
cpm = 1.0
cpmError = 1.0
errorFlag = 0
receiveTime = datetime.datetime.now() + datetime.timedelta(days = 365)

for t in range(0, int(iterations)): # 0,60 --> 1 hr #0,1440 --> 1 day #0,10080 --> 1 week
	# TIME INCREMENT --> add 5 minutes
	receiveTime = receiveTime.replace(microsecond = 0)
	receiveTime = receiveTime + datetime.timedelta(minutes = 5)
	#for i in range(2,ID+1): # Set one station at a time - 2 is first test station
	try:
		#stationID = i
		cpm = abs(random()* (math.sin(stationID)* 10. ** 1))
		cpmError = math.sqrt(cpm)
		errorFlag = 127 # Test data error code
		# without time
		# cursor.execute("""INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) VALUES (%s,%s,%s,%s);""",(stationID,cpm,cpmError,errorFlag))
		try:
			cursor.execute("INSERT INTO dosnet(receiveTime, stationID, \
				cpm, cpmError, errorFlag) VALUES (%s,%s,%s,%s,%s);",\
				(receiveTime, stationID, cpm, cpmError, errorFlag)) # WITH time
		except Exception as e:
			print 'SQL query failed'
			print str(e)
			sys.exit(1)
	except Exception as e:
		raise e
		sys.exit(1)
try:
	db.commit()
except Exception as e:
	print 'Could not commit changes to database - no changes have been applied.'
	raise e
finally:
	db.close()
