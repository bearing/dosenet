#!/usr/bin/python

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sat 21/02/15
# Last updated: Wed 08/04/15

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
#from joblib import Parallel, delayed  
#import multiprocessing


def initialise():
	#global num_cores = multiprocessing.cpu_count()
	# Open database connection
	global db; db = mdb.connect("localhost",
								"ne170group",
								"ne170groupSpring2015",
								"dosimeter_network")
	# prepare a cursor object using cursor() method
	global cursor; cursor = db.cursor()
	# Plot.ly sign in using
	py.sign_in('ne170','ilo0p1671e')
	global stnRowArrayList; stnRowArrayList = []
	global G_cpmTitle; G_cpmTitle = 'Counts per minute (CPM) [1/min]'
	global G_remtitle; G_remtitle = 'mREM/hr [J/kg/hr]'
	global G_usvTitle; G_usvTitle = 'uSv/hr [J/kg/hr]'

def getStationInfoFromDB():
	# Get # of stations, station name, longitude, latitude
	cursor.execute("SELECT `ID`, `Name`, `Lat`, `Long` FROM dosimeter_network.stations;")
	global station_rows; station_rows = cursor.fetchall()

def setStationInfo(i):
	stnRowArrayList.append((i[0], i[1], i[2], i[3]))
	return

def setStationInfoForAll():
	for i in station_rows:
		setStationInfo(i)

"""
Parallel(n_jobs=num_cores)(delayed(setStationInfo(i)) for i in station_rows)
"""

def setConstants():
	global secondsInYear; secondsInYear = 31557600 #365.23 days
	global secondsInMonth; secondsInMonth = 2592000 #30 days
	global secondsInWeek; secondsInWeek = 604800 #7 days
	global secondsInDay; secondsInDay = 86400 #24 hours
	global secondsInHour; secondsInHour = 3600 #60 minutes
	global secondsInMinute; secondsInMinute = 60 # Duh

def initVariables():
	global feature_list; feature_list = []
	global url_list; url_list = []
	global plot_url_cpm; plot_url_cpm = ''
	global plot_url_rem; plot_url_rem = ''
	global plot_url_usv; plot_url_usv = ''
	global dtRow_rowarray_list; dtRow_rowarray_list = []
	global dosesForEachStation_1d_list; dosesForEachStation_1d_list =[]
	global station_row; station_row = 0
	global fname; fname = ''

def sqlForPlot(stationID,startTime,endTime):
	sqlString = "SELECT receiveTime, cpm, rem, usv \
				FROM dosnet \
				INNER JOIN stations \
				ON dosnet.stationID=stations.ID \
				WHERE `stations`.`Name`='%s' \
					AND receiveTime BETWEEN '%s' \
					AND '%s';" % (stationID, startTime, endTime)
	#print sqlString
	try:
		cursor.execute(sqlString)
	except Exception, e:
		print 'sqlString failure:' + str(e)
	dosePerStation = cursor.fetchall()
	#
	# Populate time-restricted row array list
	# Panda code to get SQL tuples of tuples into panda df tables which are 
	# WAAAY easier to work with (and apparently are faster?)
	# I'm doing this purely because the other more manual method did not work
	# Pandas are easier to work with
	df = pd.DataFrame( [[ij for ij in i] for i in dosePerStation] )
	#sys.getsizeof(df_1d)
	df.rename(columns={0:'receiveTime', 1:'CPM', 2:'REM', 3:'USV'}, 
				inplace=True)
	return df

def makePlot(stationID,unit,dfunit,plotTitle,df,plength):
	# [unit] over numberOfSeconds for a specific named station [stationID]
	try:
		# make the filename for Plot.ly export
		# current station ID
		fname = (str(stationID)+'_'+unit+'_'+plength)
		# setup the plot.ly plot type as a Scatter with points with panda DataFrames (df)
		trace = Scatter( 
			x=df['receiveTime'],
			y=df[dfunit], # changes depending on which units you are plotting
			mode='markers'
			)
		fontPref = Font(family='Arial, monospace',
						size=16,
						color='#000000')
		layout = Layout(title=(str(stationID)+' '+unit+' '+plength),
						xaxis=XAxis(title='Time',
						rangemode='nonzero',
						autorange=True),
						yaxis=YAxis(type='log',title=plotTitle),
						font=fontPref)
		# Plot.ly export, doesn't open a firefox window on the server
		plot_url = py.plot(Figure(data=Data([trace]), 
								layout=layout), 
								filename=fname, 
								auto_open=False)
		return plot_url
	except:
		print 'This '+unit+' plot failed - '+fname

def resetURLs():
	plot_url_cpm = ''
	plot_url_rem = ''
	plot_url_usv = ''

def printPlotFail(error):
	print 'Plotting failed'
	print error

def printFeatureFail(error):
	print 'Iterative feature creation failed'
	print error

def setFeature(point,stationID,plength,LCPM,LTime,URLlist):
	# Will have to iterate through here to go through a 3x3 --> plength x URLlist
	feature = Feature(geometry=point,
					properties={
						'Name': stationID, 
						('URL_CPM_'+plength[0]): URLlist[0][0], 
						('URL_REM_'+plength[0]): URLlist[0][1], 
						('URL_USV_'+plength[0]): URLlist[0][2],
						('URL_CPM_'+plength[1]): URLlist[1][0], 
						('URL_REM_'+plength[1]): URLlist[1][1], 
						('URL_USV_'+plength[1]): URLlist[1][2], 
						('URL_CPM_'+plength[2]): URLlist[2][0], 
						('URL_REM_'+plength[2]): URLlist[2][1], 
						('URL_USV_'+plength[2]): URLlist[2][2],  
						'Latest dose (CPM)': LCPM, 
						'Latest measurement': str(LTime)
						}
					)
	feature_list.append(feature)

def getNumberOfStations():
	return range(len(station_rows))

def plotOverTime(latestTime,latestStationID,latestCPM,pointLatLong,plotLengthString,time):
	plotLengthString = 'Past_' + plotLengthString
	if latestTime=='':
		print 'Finished list?'
		pass
	else:
		try:
			startPlotTime = latestTime + datetime.timedelta(seconds=-time)
		except Exception, e:
			print "There probably isn't any data from this time.\n" + str(e)
	df = sqlForPlot(latestStationID,startPlotTime,latestTime)
	try:
		#Plot scatters Plot.ly to URLs
		# CPM over numberOfSeconds for all stations
		plot_url_cpm = makePlot(latestStationID,'CPM','CPM',G_cpmTitle,df,plotLengthString)
		# Rem over numberOfSeconds for all stations
		plot_url_rem = makePlot(latestStationID,'REM','REM',G_remtitle,df,plotLengthString)
		# uSv over numberOfSeconds for all stations
		plot_url_usv = makePlot(latestStationID,'USV','USV',G_usvTitle,df,plotLengthString)
		t_url_list = (plot_url_cpm, plot_url_rem, plot_url_usv)
		url_list.append(t_url_list)
		resetURLs()
	except Exception as e:
		printPlotFail(e)

def setGeoJSONandCloseDB():
	featurecollection = FeatureCollection(feature_list)
	# Export dump to GeoJSON file
	dump = geojson.dumps(featurecollection)
	geojson_file = 'output.geojson'
	f = open(geojson_file, 'w')
	print >> f, dump
	print("Finished execution!!")
	# disconnect from server
	db.close()

def main():
	#######################################################
	## Iterate through to define all features (stations) ##
	#######################################################
	for station_row in getNumberOfStations():
		# builds up a tuple coordinates of the stations, longitude & latitude
		longlat = [ stnRowArrayList[station_row][3], stnRowArrayList[station_row][2]]
		point = Point(longlat)
		# gets the stationID for insertion into the features
		stationID = stnRowArrayList[station_row][1]
		# get latest dose (cpm, rem, usv) and time for that measurement in the loop so we can display in exported GeoJSON file
		sqlString = "SELECT MAX(dosnet.receiveTime) \
					AS mostRecent, dosnet.cpm, dosnet.rem, dosnet.usv, stations.Name \
					FROM dosnet \
					INNER JOIN stations ON dosnet.stationID=stations.ID \
					WHERE `stations`.`Name`='%s';" % stationID
		cursor.execute(sqlString)
		sqlString = ''
		# dtRows --> Dose & time rows
		dtRows = cursor.fetchall()
		for dtRow in dtRows:
			print stationID
			# L --> Latest ...
			LTime = dtRow[0]
			LCPM  = dtRow[1]
			#Lrem  = dtRow[2]
			#Lusv  = dtRow[3]
			LstationID = dtRow[4]
			pLengthString = ('Hour','Day','Month')
			plotOverTime(LTime,LstationID,LCPM,point,pLengthString[0],secondsInHour)
			plotOverTime(LTime,LstationID,LCPM,point,pLengthString[1],secondsInDay)
			plotOverTime(LTime,LstationID,LCPM,point,pLengthString[2],secondsInMonth)
			# Make feature - iterate through?
			try:
				setFeature(point,LstationID,pLengthString,LCPM,LTime,url_list)
			except Exception as e:
				printFeatureFail(e)

initialise()
getStationInfoFromDB()
setStationInfoForAll()
setConstants()
initVariables()
main()
setGeoJSONandCloseDB()