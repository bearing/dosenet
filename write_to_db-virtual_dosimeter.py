#!/usr/bin/python

# Navrit Bal
# Tigran Ter-Stepanyan
#
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sun 1/03/15
# Last updated: Tue 3/03/15

import MySQLdb as mdb
import time
from random import randrange, randint
import datetime
#connect to db
#db = mdb.connect("localhost","root","password","testdb" )
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network")

#setup cursor
cursor = db.cursor()

#create anooog1 table
# Why is this line here?
#cursor.execute("DROP TABLE IF EXISTS dosnet;")

stationID= 1
cpm = 1
errorFlag = 1
receiveTime = datetime.datetime.now()
one_min = datetime.timedelta(minutes=1)

# INCREMENT TIME t [minutes]
for t in range(0,60): # 0,60 --> 1 hr
	# TIME INCREMENT --> add 1 minute
	receiveTime = receiveTime.replace(microsecond=0) + one_min
	#print(receiveTime)
	#insert to table
	for i in range(0,20):
		try:
			stationID = i
			# Make realistic
			# FIND OUT WHAT IS REALISTIC
			cpm = randint(0,30000)
			#conversion from cpm to rem - apparently correct
			#####################################
			# NEEDS TO BE CONFIRMED  # ASK RYAN #
			#####################################
			rem = cpm * 8.76
			#############################
			# GET the ACTUAL CONVERSION #
			#############################
			#conversion from cpm to rad
			rad = rem
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
			#db.rollback()

#show table
#cursor.execute("""SELECT * FROM dosnet;""")
#cursor.fetchall()

#rows = cursor.fetchall()
# Convert query to row arrays
#rowarray_list = []
#for row in rows:
#    t = (str(row[0]), row[1], row[2], row[3], row[4], row[5])
#    print(t)
#'''
db.close()