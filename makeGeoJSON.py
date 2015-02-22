#!/usr/bin/python2.7

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Saturday 21/2/15

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


py.sign_in('navrit','g739jakmhr')
# Open database connection
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

#cursor.execute(" SELECT receiveTime, stationID, doseRate, errorFlag FROM dosnet; ")

cursor.execute(" SELECT receiveTime, stationID, doseRate, errorFlag FROM dosnet WHERE stationID=1; ")
rows = cursor.fetchall()

# Convert query to row arrays
rowarray_list = []
for row in rows:
    t = (str(row[0]), row[1], row[2], row[3])
    rowarray_list.append(t)

#print rowarray_list[1][1] #[row number][column]


longlat_1 = (-122.25924432277681,37.8755647731171)
longlat_2 = (-122.25871324539183,37.87563252319576)

# May remove this feature - unnecassary work? We can see where they are on the maps...
stationID_1 = "Etcheverry Hall"
stationID_2 = "Soda Hall"

#Also probably pointless
#time_1 = rowarray_list[0][0]
#time_2 = rowarray_list[1][0]

#Should be done with Plot.ly
doseRate_1 = rowarray_list[0][2]
doseRate_2 = rowarray_list[1][2]

point1 = Point(longlat_1)
point2 = Point(longlat_2)
#for reference
# feature1 = Feature(geometry=point1,properties={"Name": stationID_1, "doseRate": doseRate_1})

feature1 = Feature(geometry=point1,properties={"Name": stationID_1})
feature2 = Feature(geometry=point2,properties={"Name": stationID_2})

featurecollection = FeatureCollection([feature1, feature2])

# Panda code to get SQL tuples of tuples into panda df tables
df = pd.DataFrame( [[ij for ij in i] for i in rows] )
df.rename(columns={0:'receiveTime', 1:'stationID', 2:'doseRate', 3:'errorFlag'}, inplace=True)
df = df.sort(['receiveTime'],ascending=[1])

trace1 = Scatter(
	x=df['receiveTime'],
	y=df['doseRate'],
	mode='markers'
	)

layout = Layout(
	xaxis=XAxis( title='Time' ),
	yaxis=YAxis( type='log',title='Dose')
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
plt.plot(df['receiveTime'],df['doseRate'])
plt.ylabel(r'$Dose$',fontsize=24)
plt.xlabel(r'$Time$',fontsize=24)

print("I have connected to the database, grabbed the whole table, parsed the data and plotted it via plot.ly and a matlab type to a SVG")
plt.savefig(title+'.svg', format='svg')
#plt.show()

print("Succuss!!")
# disconnect from server
db.close()