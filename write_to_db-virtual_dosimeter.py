#!/usr/bin/python

# Navrit Bal
# Tigran Ter-Stepanyan
#
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sun 1/03/15
# Last updated: Fri 13/03/15

import MySQLdb as mdb
import time
from random import randrange, randint, random
import datetime
import math
#connect to db
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network")

#setup cursor
cursor = db.cursor()

stationID= 1
cpm = 1.0
errorFlag = 1
rem = 1.0
rad = 1.0
sqlstring =""
#one_min = datetime.timedelta(minutes=1)
#one_hour = datetime.timedelta(hours=1) #not used yet
#one_day = datetime.timedelta(days=1) #not used yet
#one_week = datetime.timedelta(weeks=1) #not used yet - Does actually work
receiveTime = datetime.datetime.now() + datetime.timedelta(hours=-1) #+ 22*one_min


# INCREMENT TIME t [minutes]
for t in range(0,60): # 0,60 --> 1 hr #0,1440 --> 1 day #0,10080 --> 1 week
	# TIME INCREMENT --> add 1 minute
	#receiveTime = receiveTime.replace(microsecond=0) + one_min
	receiveTime = receiveTime.replace(microsecond=0)
	receiveTime = receiveTime + datetime.timedelta(minutes=1)
	#insert to table
	for i in range(1,89):
		try:
			stationID = i
			# Make realistic
			# FIND OUT WHAT IS REALISTIC
			# cpm = str(abs(5*random()* (math.sin(i)* 10. ** -6)))
			cpm = abs(100+random()* (math.sin(i)* 10. ** 1))
			#conversion from cpm to rem - apparently correct
			#####################################
			# NEEDS TO BE CONFIRMED  # ASK RYAN #
			#####################################
			rem = cpm * 1200/60/1000 # FOR CAESIUM-137 #per hour # milliRem
			#############################
			# GET the ACTUAL CONVERSION #
			#############################
			#conversion from cpm to rad
			rad = rem
			errorFlag = randint(0,1)
			#print receiveTime, stationID, cpm, rem, rad, errorFlag
			#print("---------")
			# without time
			# cursor.execute("""INSERT INTO dosnet(stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(stationID,cpm,rem,rad,errorFlag))
			# WITH time
			try:
				cursor.execute("""INSERT INTO dosnet(receiveTime, stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s,%s);""",(receiveTime, stationID,cpm,rem,rad,errorFlag))
				db.commit()
			except Exception, e:
				print "SQL query is wrong"
				print str(e)
			# No need to add delay to the simulation --> we are now incrementing time in the outer for loop
			#time.sleep(0.1)
		except:
			print("Some exception")

db.close()