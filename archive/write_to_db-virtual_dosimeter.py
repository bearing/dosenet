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

db = mdb.connect("localhost",
				"ne170group",
				"ne170groupSpring2015",
				"dosimeter_network")
cursor = db.cursor()

stationID= 1
cpm = 1.0
errorFlag = 0
rem = 1.0
rad = 1.0
receiveTime = datetime.datetime.now() + datetime.timedelta(hours=-1) #+ 22*one_min

for t in range(0, 1440): # 0,60 --> 1 hr #0,1440 --> 1 day #0,10080 --> 1 week
	# TIME INCREMENT --> add 5 minutes
	receiveTime = receiveTime.replace(microsecond=0)
	receiveTime = receiveTime + datetime.timedelta(minutes=5)
	for i in range(2,3): # Set one station at a time
		try:
			stationID = i
			cpm = abs(random()* (math.sin(i)* 10. ** 1))
			rem = cpm * 0.01886792 # mREM/hr
			rad = cpm * 0.00980392 # uSv/hr
			errorFlag = randint(0,1)
			# without time
			# cursor.execute("""INSERT INTO dosnet(stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(stationID,cpm,rem,rad,errorFlag))
			try:
				cursor.execute("INSERT INTO dosnet(receiveTime, stationID, \
					cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s,%s);",\
					(receiveTime, stationID,cpm,rem,rad,errorFlag)) # WITH time
			except Exception as e:
				print 'SQL query failed'
				print str(e)
		except Exception as e:
			raise e
try:
	db.commit()
except Exception as e:
	print 'Could not commit changes to database - no changes have been applied.'
	raise e
finally:
	db.close()
