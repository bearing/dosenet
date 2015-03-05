#!/usr/bin/python

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sat 21/02/15
# Last updated: Mon 2/03/15

import MySQLdb as mdb
import sys
import collections 
import geojson
from geojson import Point, Feature, FeatureCollection
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import plotly.plotly as py
from plotly.graph_objs import *
import pandas as pd


py.sign_in('ne170','ilo0p1671e')
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

# iterate through station MySQL DB to get '# of stations, station name, longitude, latitude '
cursor.execute("SELECT * FROM stations WHERE 1; ")
station_rows = cursor.fetchall()
# Convert query to row arrays
station_rowarray_list = []
for station_row in station_rows:
    s = (station_row[0], station_row[1], station_row[2], station_row[3])
    print s
    station_rowarray_list.append(s)


feature_list=[]
dtRow_rowarray_list = []
for station_row in range(len(station_rows)):
	# builds up a tuple coordinates of the stations, longitude & latitude
	longlat = [ station_rowarray_list[station_row][3], station_rowarray_list[station_row][2] ]
	# gets the stationID for insertion into the features
	stationID = station_rowarray_list[station_row][1]
	# get latest dose (cpm) and time for that measurement in the loop so we can display in exported GeoJSON file
	sqlString = "SELECT dosnet.receiveTime, dosnet.cpm, stations.Name FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s';" % stationID
	cursor.execute(sqlString)
	dtRows = cursor.fetchall()
	for dtRow in range(len(dtRows)):
		print dtRow[0]
		"""#t = (dtRow[0], dtRow[1])
		#print t
		#dtRow_rowarray_list.append(t)"""
	print(dtRow_rowarray_list)
	LCPM = dtRow[1]
	Ltime = dtRow[0]
	point = Point(longlat)
	feature = Feature(geometry=point,properties={"Name": stationID, "Latest dose (CPM)": L_CPM,"Latest measurement": L_time})
	feature_list.append(feature)

featurecollection = FeatureCollection(feature_list)
##########################################################

time_1 = rowarray_list[0][0]
time_2 = rowarray_list[1][0]

#Should be done with Plot.ly
cpm_1 = rowarray_list[0][2]
cpm_2 = rowarray_list[1][2]

#point1 = Point(longlat_1)
#point2 = Point(longlat_2)
#for reference
# feature1 = Feature(geometry=point1,properties={"Name": stationID_1, "doseRate": doseRate_1})


#######################################################
## Iterate through to define all features (stations) ##
#######################################################

#feature1 = Feature(geometry=point1,properties={"Name": stationID_1})
#feature2 = Feature(geometry=point2,properties={"Name": stationID_2})

#featurestring = "["+str(feature1)+"," +str(feature2)+"]"

#featurecollection = FeatureCollection([feature1, feature2])

# Panda code to get SQL tuples of tuples into panda df tables which are WAAAY easier to work with (and apparently are faster?)
df = pd.DataFrame( [[ij for ij in i] for i in rows] )
df.rename(columns={0:'receiveTime', 1:'stationID', 2:'CPM', 3:'rem', 4:'rad', 5:'errorFlag'}, inplace=True)
df = df.sort(['receiveTime'],ascending=[1])

trace1 = Scatter(
	x=df['receiveTime'],
	y=df['CPM'],
	mode='markers'
	)

layout = Layout(
	xaxis=XAxis( title='Time' ),
	yaxis=YAxis( type='log',title='Counts per minute (CPM) [1/min]')
	)

# Plot.ly export
data = Data([trace1])
fig = Figure(data=data, layout=layout)
#py.iplot(fig, filename='Dose vs Time')

# Export dump to GeoJSON file
dump = geojson.dumps(featurecollection)
geojson_file = 'output.geojson'
f = open(geojson_file, 'w')
print >> f, dump

# matplotlib plotting to screen

#print(str(df['receiveTime'][-1:])[5:])
#print(str(df['receiveTime'][-2:-2])[5:])
#print((str(df['receiveTime']))[:-41])
#print((str(df['receiveTime']))[-11:][0])
receiveString = str(df['receiveTime'][0])
title = ('Dose over time starting '+receiveString)

plt.title(title,fontsize=16)
plt.plot(df['receiveTime'],df['CPM'])
plt.ylabel(r'$CPM$',fontsize=24)
plt.xlabel(r'$Time$',fontsize=24)

plot_url = py.plot(fig, filename=receiveString)
print(plot_url)

plt.savefig(title+'.svg', format='svg')
#plt.show()

print("Succuss!!")
# disconnect from server
db.close()