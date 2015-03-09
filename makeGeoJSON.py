#!/usr/bin/python

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sat 21/02/15
# Last updated: Mon 9/03/15

import MySQLdb as mdb
import sys
import collections 
import geojson
from geojson import Point, Feature, FeatureCollection
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
#import plotly.plotly as py
#from plotly.graph_objs import *
import pandas as pd
import time
import datetime

one_min = datetime.timedelta(minutes=1)
one_hour = datetime.timedelta(hours=1) 
one_day = datetime.timedelta(days=1) 
one_week = datetime.timedelta(weeks=1) #not used yet
#py.sign_in('ne170','ilo0p1671e')
# Open database connection
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )
# prepare a cursor object using cursor() method
cursor = db.cursor()


cursor.execute("SELECT receiveTime, stationID, cpm, rem, rad, errorFlag FROM dosnet WHERE 1; ")
rows = cursor.fetchall()

# Convert query to row arrays
rowarray_list = []
for row in rows:
    t = (str(row[0]), row[1], row[2], row[3], row[4], row[5])
    rowarray_list.append(t)

#print rowarray_list[1][1] #[row number][column]

# Panda code to get SQL tuples of tuples into panda df tables which are WAAAY easier to work with (and apparently are faster?)
df = pd.DataFrame( [[ij for ij in i] for i in rows] )
df.rename(columns={0:'receiveTime', 1:'stationID', 2:'CPM', 3:'rem', 4:'rad', 5:'errorFlag'}, inplace=True)

# Get # of stations, station name, longitude, latitude
cursor.execute("SELECT * FROM stations WHERE 1;")
station_rows = cursor.fetchall()
# Convert query to row arrays
station_rowarray_list = []
for station_row in station_rows:
    s = (station_row[0], station_row[1], station_row[2], station_row[3])
    #print s
    station_rowarray_list.append(s)


#######################################################
## Iterate through to define all features (stations) ##
#######################################################
numberOfDays = 1
feature_list=[]
dtRow_rowarray_list = []
dosesForEachStation_1d_list =[]
cnt = 0
for station_row in range(len(station_rows)):
	cnt = cnt+1
	print cnt
	# builds up a tuple coordinates of the stations, longitude & latitude
	longlat = [ station_rowarray_list[station_row][3], station_rowarray_list[station_row][2] ]
	point = Point(longlat)
	# gets the stationID for insertion into the features
	stationID = station_rowarray_list[station_row][1]
	# get latest dose (cpm, rem, rad) and time for that measurement in the loop so we can display in exported GeoJSON file
	sqlString = "SELECT MAX(dosnet.receiveTime) AS mostRecent, dosnet.cpm, dosnet.rem, dosnet.rad, stations.Name FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s';" % stationID
	cursor.execute(sqlString)
	sqlString = ""
	dtRows = cursor.fetchall()
	for dtRow in dtRows:
		#print dtRow[0]
		#t = (dtRow[0], dtRow[1])
		t = (dtRow[0],dtRow[1],dtRow[2],dtRow[3],dtRow[4])
		#dtRow_rowarray_list.append(t)
		# L --> Latest ...
		LTime = t[0]
		LCPM = t[1]
		Lrem = t[2]
		Lrad = t[3] # Not used yet
		LstationID = t[4] #print 'LCPM =',LCPM,' LTime =',LTime,' ID:',t[4]
		try:
			feature = Feature(geometry=point,properties={"Name": stationID, "Latest dose (CPM)": LCPM, "Latest dose (mrem)": Lrem, "Latest measurement": str(LTime)})
			feature_list.append(feature)
			startPlotTime = LTime + datetime.timedelta(days=-numberOfDays)
		except Exception as e:
			print e
			print '''Probably found all of the stations, if you're lucky :) '''
		# Let's say this is the last 24hours plot for now --> latest time - 1 day
		# Can't do this LTime = t[0] - one_day - one_min
		# CAN'T DO this LTime - datetime.timedelta(days=+numberOfDays) NOTE THE PLUS AND MINUS
		# Gets the required plot information for the given time interval for that specific station
		sqlString = "SELECT receiveTime, cpm, rem FROM dosnet WHERE receiveTime BETWEEN '%s' AND '%s';" % (startPlotTime, LTime)
		cursor.execute(sqlString)
		sqlString = ""
		dosesForEachStation_1ds = cursor.fetchall()
		#
		# Panda code to get SQL tuples of tuples into panda df tables which are WAAAY easier to work with (and apparently are faster?)
		# I'm doing this purely because the other more manual method did not work
		# Pandas are easier to work with
		df_1d = pd.DataFrame( [[ij for ij in i] for i in dosesForEachStation_1ds] )
		sys.getsizeof(df_1d)
		df_1d.rename(columns={0:'receiveTime', 1:'CPM', 2:'rem'}, inplace=True)
		df_1d = df_1d.fillna(0) # with 0s rather than NaNs
		# Populate time-restricted row array list
		'''for dosesForEachStation_1d in dosesForEachStation_1ds:
			#print dosesForEachStation_1d[0], dosesForEachStation_1d[1], dosesForEachStation_1d[2]
			d = (dosesForEachStation_1d[0],dosesForEachStation_1d[1],dosesForEachStation_1d[2])
			dosesForEachStation_1d_list.append(d)'''
			#print dosesForEachStation_1d_list[0]
		# PLOT CPM VS TIME
		title = (t[4]+' starting at '+str(startPlotTime)) # t[4] --> stationID
		plt.plot(df['receiveTime'],df['CPM'])
		#plt.plot(dosesForEachStation_1d_list[0][:],dosesForEachStation_1d_list[1][:]) #Plot CPM (y) vs Time (x)
		plt.xlabel(r'$Time$',fontsize=24) # X Y labels
		plt.ylabel(r'$CPM$',fontsize=24)
		plt.savefig(title+'.svg', format='svg')
		# PLOT REM VS TIME
		title = (t[4]+' starting at '+str(startPlotTime)) # t[4] --> stationID
		plt.plot(df['receiveTime'],df['rem'])
		#plt.plot(dosesForEachStation_1d_list[0][:],dosesForEachStation_1d[2][:]) #Plot CPM (y) vs Time (x)
		plt.xlabel(r'$Time$',fontsize=24) # X Y labels
		plt.ylabel(r'$REM/hour$',fontsize=24)
		plt.savefig(title+'.svg', format='svg')
		
	#print(dtRow_rowarray_list)
	
featurecollection = FeatureCollection(feature_list)
##########################################################
# matplotlib plotting to screen

'''
#receiveString = str(df['receiveTime'][0])
receiveString = str(receiveTime)
#title = ('Dose (CPM) over time starting '+receiveString)
title = (t[4]+' starting at '+receiveString)
plt.title(title,fontsize=16)
plt.plot(df['receiveTime'],df['CPM'])
plt.ylabel(r'$CPM$',fontsize=24)
plt.xlabel(r'$Time$',fontsize=24)

plt.savefig(title+'.svg', format='svg')
#plt.show()
'''


# Export dump to GeoJSON file
dump = geojson.dumps(featurecollection)
geojson_file = 'output.geojson'
f = open(geojson_file, 'w')
print >> f, dump

print("Succuss!!")
# disconnect from server
db.close()