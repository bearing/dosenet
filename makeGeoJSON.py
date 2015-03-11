#!/usr/bin/python

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sat 21/02/15
# Last updated: Tues 10/03/15

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
import time
import datetime


#one_min = datetime.timedelta(minutes=1)
#one_hour = datetime.timedelta(hours=1) 
#one_day = datetime.timedelta(days=1) 
#one_week = datetime.timedelta(weeks=1) 
# Plot.ly sign in using
py.sign_in('ne170','ilo0p1671e')
# Open database connection
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )
# prepare a cursor object using cursor() method
cursor = db.cursor()


#cursor.execute("SELECT receiveTime, stationID, cpm, rem, rad, errorFlag FROM dosnet WHERE 1; ")
#rows = cursor.fetchall()

# Convert query to row arrays
#rowarray_list = []
#for row in rows:
#    t = (str(row[0]), row[1], row[2], row[3], row[4], row[5])
#    rowarray_list.append(t)

#print rowarray_list[1][1] #[row number][column]

# Panda code to get SQL tuples of tuples into panda df tables which are WAAAY easier to work with (and apparently are faster?)
#df = pd.DataFrame( [[ij for ij in i] for i in rows] )
#df.rename(columns={0:'receiveTime', 1:'stationID', 2:'CPM', 3:'rem', 4:'rad', 5:'errorFlag'}, inplace=True)

# Get # of stations, station name, longitude, latitude
cursor.execute("SELECT `ID`, `Name`, `Lat`, `Long` FROM dosimeter_network.stations;")
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
secondsInYear = 31557600 #365.23 days
secondsInMonth = 2592000 #30 days
secondsInWeek = 604800 #7 days
secondsInDay = 86400 #24 hours
secondsInHour = 3600 #60 minutes
secondsInMinute = 60 # Duh

feature_list=[]
dtRow_rowarray_list = []
dosesForEachStation_1d_list =[]
station_row = 0
fname = ''
#cnt = 0
for station_row in range(len(station_rows)):
	#cnt = cnt+1
	#print cnt
	# builds up a tuple coordinates of the stations, longitude & latitude
	longlat = [ station_rowarray_list[station_row][3], station_rowarray_list[station_row][2] ]
	point = Point(longlat)
	# gets the stationID for insertion into the features
	stationID = station_rowarray_list[station_row][1]
	# get latest dose (cpm, rem, rad) and time for that measurement in the loop so we can display in exported GeoJSON file
	sqlString = "SELECT MAX(dosnet.receiveTime) AS mostRecent, dosnet.cpm, dosnet.rem, dosnet.rad, stations.Name FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s';" % stationID
	#print stationID
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
		Lrad = t[3]
		LstationID = t[4]
		#print 'LCPM =',LCPM,' LTime =',LTime,' ID:',LstationID
		if LTime=='':
			print 'Finished list?'
			pass
		else:
			try:
				startPlotTime = LTime + datetime.timedelta(seconds=-secondsInHour)
			except Exception, e:
				print "There probably isn't any data from this time.\n" + str(e)
		#print '''Probably found all of the stations, if you're lucky :) '''
		# Let's say this is the last 24hours plot for now --> latest time - 1 day
		# Can't do this LTime = t[0] - one_day - one_min
		# CAN'T DO this LTime - datetime.timedelta(days=+numberOfDays) NOTE THE PLUS AND MINUS
		# Gets the required plot information for the given time interval for that specific station
		sqlString = "SELECT receiveTime, cpm, rem, rad FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s' AND receiveTime BETWEEN '%s' AND '%s';" % (LstationID, startPlotTime, LTime)
		#print sqlString
		try:
			cursor.execute(sqlString)
		except Exception, e:
			print 'sqlString failure:' + str(e)
		sqlString = ''
		dosesForEachStation_t = cursor.fetchall()
		#
		# Populate time-restricted row array list
		# Panda code to get SQL tuples of tuples into panda df tables which are WAAAY easier to work with (and apparently are faster?)
		# I'm doing this purely because the other more manual method did not work
		# Pandas are easier to work with
		df_t = pd.DataFrame( [[ij for ij in i] for i in dosesForEachStation_t] )
		#sys.getsizeof(df_1d)
		df_t.rename(columns={0:'receiveTime', 1:'CPM', 2:'rem', 3:'rad'}, inplace=True)
		df_t = df_t.fillna(0) # with 0s rather than NaNs
		try:
			#Plot scatters Plot.ly to URLs
			#
			####### HOUR #########
			plength = 'Past_Hour' # plot length string
			# CPM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'CPM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['CPM'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' CPM '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='Counts per minute (CPM) [1/min]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot CPM failed'
			#
			# REM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'REM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rem'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' REM/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='REM/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot REM failed'
			#
			# RAD over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'RAD'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rad'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' RAD/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='RAD/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot RAD failed'
			try:
				#feature = Feature(geometry=point,properties={"Name": stationID, "Latest dose (CPM)": LCPM, "Latest dose (mrem)": Lrem, "Latest dose (mrad)": Lrad,"Latest measurement": str(LTime), "1 day URL":plot_url})				
				feature = Feature(geometry=point,properties={"URL":plot_url, "Latest dose (CPM)": LCPM ,"Latest measurement": str(LTime), "Name": stationID})
				feature_list.append(feature)
			except Exception as e:
				print "Iterative feature creation failed"
				print e
			plot_url = ""
		except Exception as e:
			print 'Plotting failed'
			print e
		#
		#
		#
		##### DAY #######
		#
		#
		if LTime=='':
			print 'Finished list?'
			pass
		else:
			try:
				startPlotTime = LTime + datetime.timedelta(seconds=-secondsInDay)
			except Exception, e:
				print "There probably isn't any data from this time.\n" + str(e)
		sqlString = "SELECT receiveTime, cpm, rem, rad FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s' AND receiveTime BETWEEN '%s' AND '%s';" % (LstationID, startPlotTime, LTime)

		try:
			cursor.execute(sqlString)
		except Exception, e:
			print 'sqlString failure:' + str(e)
		sqlString = ''
		dosesForEachStation_t = cursor.fetchall()
		df_t = pd.DataFrame( [[ij for ij in i] for i in dosesForEachStation_t] )
		#sys.getsizeof(df_1d)
		df_t.rename(columns={0:'receiveTime', 1:'CPM', 2:'rem', 3:'rad'}, inplace=True)
		df_t = df_t.fillna(0) # with 0s rather than NaNs
		try:
			#Plot scatters Plot.ly to URLs
			#
			####### HOUR #########
			plength = 'Past_Day' # plot length string
			# CPM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'CPM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['CPM'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' CPM '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='Counts per minute (CPM) [1/min]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot CPM failed'
			#
			# REM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'REM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rem'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' REM/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='REM/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot REM failed'
			#
			# RAD over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'RAD'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rad'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' RAD/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='RAD/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot RAD failed'
			plot_url = ""
		except Exception as e:
			print 'Plotting failed'
			print e
		#
		#
		###### MONTH ########
		#
		#
		if LTime=='':
			print 'Finished list?'
			pass
		else:
			try:
				startPlotTime = LTime + datetime.timedelta(seconds=-secondsInMonth)
			except Exception, e:
				print "There probably isn't any data from this time.\n" + str(e)
		sqlString = "SELECT receiveTime, cpm, rem, rad FROM dosnet INNER JOIN stations ON dosnet.stationID=stations.ID WHERE `stations`.`Name`='%s' AND receiveTime BETWEEN '%s' AND '%s';" % (LstationID, startPlotTime, LTime)
		try:
			cursor.execute(sqlString)
		except Exception, e:
			print 'sqlString failure:' + str(e)
		sqlString = ''
		dosesForEachStation_t = cursor.fetchall()
		df_t = pd.DataFrame( [[ij for ij in i] for i in dosesForEachStation_t] )
		df_t.rename(columns={0:'receiveTime', 1:'CPM', 2:'rem', 3:'rad'}, inplace=True)
		df_t = df_t.fillna(0) # with 0s rather than NaNs
		try:
			#Plot scatters Plot.ly to URLs
			####### MONTH #########
			plength = 'Past_Month' # plot length string
			# CPM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'CPM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['CPM'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' CPM '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='Counts per minute (CPM) [1/min]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot CPM failed'
			#
			# REM over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'REM'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rem'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' REM/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='REM/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot REM failed'
			#
			# RAD over numberOfSeconds for all stations
			try:
				fname = (str(LstationID)+'_'+'RAD'+'_'+plength)
				trace = Scatter(
					x=df_t['receiveTime'],
					y=df_t['rad'],
					mode='markers'
				)
				layout = Layout(title=(str(LstationID)+' RAD/hr '+plength),xaxis=XAxis(title='Time'),yaxis=YAxis( type='log',title='RAD/hr [1/hr]'))
				data = Data([trace]) # Plot.ly export
				trace = Scatter(x='',y='',mode='markers') # Reset stuff
				fig = Figure(data=data, layout=layout)
				layout = Layout(xaxis=XAxis(title=''),yaxis=YAxis( type='log',title='')) # Reset stuff
				plot_url = (py.plot(fig, filename=fname))
			except:
				print 'plot RAD failed'
			plot_url = ""
		except Exception as e:
			print 'Plotting failed'
			print e
featurecollection = FeatureCollection(feature_list)
##########################################################
# matplotlib plotting to screen

'''# PLOT REM VS TIME
title = fname
plt.plot(df['receiveTime'],df['rem'])
#plt.plot(dosesForEachStation_1d_list[0][:],dosesForEachStation_1d[2][:]) #Plot CPM (y) vs Time (x)
plt.xlabel(r'$Time$',fontsize=24) # X Y labels
plt.ylabel(r'$REM/hour$',fontsize=24)
plt.savefig(title+'.svg', format='svg')'''


#title = (t[4]+' starting at '+str(startPlotTime)) # t[4] --> stationID
#plt.plot(df_1d['receiveTime'],df_1d['CPM'])
###plt.plot(dosesForEachStation_1d_list[0][:],dosesForEachStation_1d_list[1][:]) #Plot CPM (y) vs Time (x)
#plt.xlabel(r'$Time$',fontsize=24) # X Y labels
#plt.ylabel(r'$CPM$',fontsize=24)
#plt.savefig(title+'.svg', format='svg')
#plt.show()


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

print("Finished execution!!")
# disconnect from server
db.close()