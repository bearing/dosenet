#!/usr/bin/python

# Navrit Bal
# Tigran Ter-Stepanyan
#
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sun 1/03/15
# Last updated: Mon 27/06/15

import MySQLdb as mdb
import time
from random import randrange, randint, random
import datetime
import math
import sys

db = mdb.connect("localhost",
				"ne170group",
				"ne170groupSpring2015",
				"dosimeter_network")
cursor = db.cursor()
# Globals
stationID= 1
cpm = 1.0
cpmError = 1.0
errorFlag = 0
receiveTime = datetime.datetime.now() + datetime.timedelta(hours = -1)

for t in range(0, 1440): # 0,60 --> 1 hr #0,1440 --> 1 day #0,10080 --> 1 week
	# TIME INCREMENT --> add 5 minutes
	receiveTime = receiveTime.replace(microsecond = 0)
	receiveTime = receiveTime + datetime.timedelta(minutes = 5)
	for i in range(2,3): # Set one station at a time
		try:
			stationID = i
			cpm = abs(random()* (math.sin(i)* 10. ** 1))
			cpmError = math.sqrt(cpm)
			errorFlag = randint(0,1)
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
