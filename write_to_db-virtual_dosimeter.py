#!/usr/bin/python

# Navrit Bal
# Tigran Ter-Stepanyan
#
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sun 1/03/15
# Last updated: Wed 11/03/15

import MySQLdb as mdb
import time
from random import randrange, randint, random
import datetime
#connect to db
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network")

#setup cursor
cursor = db.cursor()

stationID= 1
cpm = 1
errorFlag = 1
#one_min = datetime.timedelta(minutes=1)
#one_hour = datetime.timedelta(hours=1) #not used yet
#one_day = datetime.timedelta(days=1) #not used yet
#one_week = datetime.timedelta(weeks=1) #not used yet - Does actually work
receiveTime = datetime.datetime.now() + datetime.timedelta(hours=-3) #+ 22*one_min


# INCREMENT TIME t [minutes]
for t in range(0,10080): # 0,60 --> 1 hr #0,1440 --> 1 day #0,10080 --> 1 week
	# TIME INCREMENT --> add 1 minute
	#receiveTime = receiveTime.replace(microsecond=0) + one_min
	receiveTime = receiveTime.replace(microsecond=0)
	#insert to table
	for i in range(0,5):
		try:
			receiveTime = receiveTime + datetime.timedelta(seconds=randint(0,i))
			stationID = i
			# Make realistic
			# FIND OUT WHAT IS REALISTIC
			cpm = randint(0,(i*10)) + random()*3
			#conversion from cpm to rem - apparently correct
			#####################################
			# NEEDS TO BE CONFIRMED  # ASK RYAN #
			#####################################
			rem = cpm * 8.76
			#############################
			# GET the ACTUAL CONVERSION #
			#############################
			#conversion from cpm to rad
			rad = rem * 0.1
			errorFlag = randint(0,1)
			#print stationID, errorFlag, cpm, rem, rad
			#print("---------")
			# without time
			# cursor.execute("""INSERT INTO dosnet(stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(stationID,cpm,rem,rad,errorFlag))
			# WITH time
			cursor.execute("""INSERT INTO dosnet(receiveTime,stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s,%s);""",(receiveTime,stationID,cpm,rem,rad,errorFlag))
			db.commit()
			# No need to add delay to the simulation --> we are now incrementing time in the outer for loop
			#time.sleep(0.1)
		except:
			print("Some exception")

db.close()