#!/usr/bin/python

# Navrit Bal
# DoseNet
# Nuclear Engineering 170A: Nuclear Design
# University of California, Berkeley, U.S.A.
# Created: Sat 21/02/15
# Last updated: Mon 04/05/15

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
	global calibrationFactor_cpm_to_rem; calibrationFactor_cpm_to_rem = 0.01886792;  # COMPLETELY RANDOM (1/53)
	global calibrationFactor_cpm_to_usv; calibrationFactor_cpm_to_usv = 0.00980392; # COMPLETELY RANDOM (1/102)
	#global num_cores; num_cores = multiprocessing.cpu_count()

def getStationInfoFromDB():
	# Get number of stations, station name, longitude, latitude, CPM to mRem and uSv conversion calibration factors
	cursor.execute("SELECT ID, `Name`, Lat, `Long`, cpmtorem, cpmtousv \
					FROM dosimeter_network.stations;") # Name & Long are reserved words apparently, need `...`
	global station_rows; station_rows = cursor.fetchall()

def setStationInfo(i):
	stnRowArrayList.append((i[0], i[1], i[2], i[3], i[4], i[5]))
	return

def setStationInfoForAll():
	for i in station_rows:
		setStationInfo(i)

'''
Parallel(n_jobs=num_cores)(delayed(setStationInfo)(i)for i in station_rows)
'''
'''Parallel(n_jobs=num_cores)(delayed(FUNCTION)(ARGUMENT) for ARGUMENT in NUMBEROFITERATIONS)'''

def setConstants():
	global secondsInYear; secondsInYear = 31557600 #365.23 days
	global secondsInMonth; secondsInMonth = 2592000 #30 days
	global secondsInWeek; secondsInWeek = 604800 #7 days
	global secondsInDay; secondsInDay = 86400 #24 hours
	global secondsInHour; secondsInHour = 3600 #60 minutes
	global secondsInMinute; secondsInMinute = 60 # Duh

def initVariables():
	global feature_list; feature_list = []
	#global url_list; url_list = []
	global dtRow_rowarray_list; dtRow_rowarray_list = []
	global dosesForEachStation_1d_list; dosesForEachStation_1d_list =[]
	global station_row; station_row = 0
	global fname; fname = ''

def sqlForPlot(stationID,startTime,endTime):
	sqlString = "SELECT receiveTime, cpm, cpmError \
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
	df.rename(columns={0:'receiveTime', 1:'CPM', 2:'cpmError'}, 
				inplace=True)
	i = 0;
	while len(df.index) > 200: # Reduce data for plotting
		i += 1
		df = df[::4]
	if i!=0:
		print 'Data was quartered' ,i,'times'
	return df

def makePlot(stationID,unit,error,plotTitle,df,plength):
	# [unit] over numberOfSeconds for a specific named station [stationID]
	try:
		t0 = time.clock()
		# make the filename for Plot.ly export
		# current station ID
		fname = (str(stationID)+'_'+unit+'_'+plength)
		# setup the plot.ly plot type as a Scatter with points with panda DataFrames (df)
		trace = Scatter( 
			x=df['receiveTime'],
			y=df[unit], # changes depending on which units you are plotting
			mode='lines+markers',
			error_y=ErrorY(
				type='data',
				array=df[error],
				visible=True
				)
			)
		fontPref = Font(family='Arial, monospace',
						size=16,
						color='#000000')
		layout = Layout(title=(str(stationID)+' '+unit+' '+plength),
						xaxis=XAxis(title='Time (PDT)',
						rangemode='nonzero',
						autorange=True),
						yaxis=YAxis(type='log',title=plotTitle),
						font=fontPref)
		# Plot.ly export, doesn't open a firefox window on the server
		plot_url = py.plot(Figure(data=Data([trace]), 
								layout=layout), 
								filename=fname, 
								auto_open=False)
		print fname, 'Plot.ly:',time.clock() - t0, '(s)'
		return plot_url
	except:
		print 'This '+unit+' plot failed - '+fname

def printPlotFail(error):
	print 'Plotting failed'
	print error

def printFeatureFail(error):
	print 'Iterative feature creation failed'
	print error

def setFeature(geometry,name,plength,latestDose,latestTime,URLlist):
	feature = Feature(geometry=geometry,
					properties={
						'Name': name, 
						('URL_CPM_'+plength[0]): URLlist[0][0], 
						('URL_REM_'+plength[0]): URLlist[0][1], 
						('URL_USV_'+plength[0]): URLlist[0][2],
						('URL_CPM_'+plength[1]): URLlist[1][0], 
						('URL_REM_'+plength[1]): URLlist[1][1], 
						('URL_USV_'+plength[1]): URLlist[1][2], 
						('URL_CPM_'+plength[2]): URLlist[2][0], 
						('URL_REM_'+plength[2]): URLlist[2][1], 
						('URL_USV_'+plength[2]): URLlist[2][2],  
						'Latest dose (CPM)': latestDose[0],
						'Latest dose (mREM/hr)': latestDose[1],
						'Latest dose (&microSv/hr)': latestDose[2], 
						'Latest measurement': str(latestTime)
						}
					)
	feature_list.append(feature)

def getNumberOfStations():
	return range(len(station_rows))

def plotOverTime(latestTime,latestStationID,calibrationCPMtoREM,calibrationCPMtoUSV,pointLatLong,plotLengthString,time):
	plotLengthString = 'Past_' + plotLengthString
	if latestTime == '':
		print 'Finished list?'
		pass
	else:
		try:
			# Note the negative --> means that we're looking at the past relative to the latest measurement stored in the DB
			startPlotTime = latestTime + datetime.timedelta(seconds=-time)
		except Exception, e:
			print "There probably isn't any data from this time.\n" + str(e)
	df = sqlForPlot(latestStationID,startPlotTime,latestTime)
	# Expands dataframe to include other units and their associated errors
	df['REM'] = df['CPM'] * calibrationCPMtoREM
	df['USV'] = df['CPM'] * calibrationCPMtoUSV
	df['remError'] = df['cpmError'] * calibrationCPMtoREM    # CHECK WITH JOEY ETC
	df['usvError'] = df['cpmError'] * calibrationCPMtoUSV    # PRETTY SURE THIS IS INCORRECT
	try:
		# Initialise & reset variables
		urlA = ''
		urlB = ''
		urlC = ''
		urlList = []
		# Plot markers & lines - Plot.ly to URLs
		# CPM over numberOfSeconds for all stations
		urlA = makePlot(latestStationID,'CPM','cpmError',G_cpmTitle,df,plotLengthString)
		# mREM/hr over numberOfSeconds for all stations
		urlB = makePlot(latestStationID,'REM','remError',G_remtitle,df,plotLengthString)
		# uSv/hr over numberOfSeconds for all stations
		urlC = makePlot(latestStationID,'USV','usvError',G_usvTitle,df,plotLengthString)
		tempRow = urlA,urlB,urlC
		urlList.extend(tempRow)
	except Exception as e:
		printPlotFail(e)
	return urlList

def setGeoJSONandCloseDB():
	featurecollection = FeatureCollection(feature_list)
	# Export dump to GeoJSON file
	dump = geojson.dumps(featurecollection)
	geojson_file = 'output.geojson'
	f = open(geojson_file, 'w')
	print >> f, dump
	print('Navrit Bal - time is '+str(datetime.datetime.now()))
	# disconnect from server
	db.close()

def main():
	plotLength = ('Hour','Day','Month')
	#######################################################
	## Iterate through to define all features (stations) ##
	#######################################################
	for station_row in getNumberOfStations():
		# builds up a tuple coordinates of the stations, longitude & latitude
		longlat = [ stnRowArrayList[station_row][3], stnRowArrayList[station_row][2]]
		point = Point(longlat)
		# gets the stationID for insertion into the features
		stationID = stnRowArrayList[station_row][1]
		# get latest dose (CPM) and time for that measurement in the loop so we can display in exported GeoJSON file
		# Don't need cpmError in this query
		sqlString ="SELECT dosnet.stationID, stations.Name, dosnet.receiveTime, dosnet.cpm, stations.cpmtorem, stations.cpmtousv \
					FROM dosnet \
					INNER JOIN stations ON dosnet.stationID=stations.ID \
					WHERE dosnet.receiveTime = \
					(SELECT MAX(dosnet.receiveTime) \
						FROM dosnet \
						INNER JOIN stations \
						ON dosnet.stationID=stations.ID  \
						WHERE stations.Name='%s') AND stations.Name='%s';" % (stationID, stationID)
		cursor.execute(sqlString)
		sqlString = ''
		# dtRows --> Dose & time rows
		dtRows = cursor.fetchall()
		for i in dtRows:
			#print stationID
			# L --> Latest ...
			LName = i[1]
			LTime = i[2]
			Lcpmtorem = i[4]
			Lcpmtousv = i[5]
			LDose  = i[3], i[3]*Lcpmtorem, i[3]*Lcpmtousv
			urlList = []
			urlA = plotOverTime(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[0],secondsInHour)
			urlB = plotOverTime(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[1],secondsInDay)
			urlC = plotOverTime(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[2],secondsInMonth)
			urlRow = (urlA,urlB,urlC)
			urlList.extend(urlRow)
			# Make feature - iterating through - each 
			try:
				setFeature(point,LName,plotLength,LDose,LTime,urlList)
			except Exception as e:
				printFeatureFail(e)

initialise()
getStationInfoFromDB()
setStationInfoForAll()
setConstants()
initVariables()
main()
setGeoJSONandCloseDB()