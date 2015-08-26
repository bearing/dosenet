#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dev_makeGeoJSON.py (functional) Sat 09/05/15
# Created: 		Sun 7/06/15
# Last updated: Tue 25/08/15
###########################
## Run on DoseNet server ##
###########################
import os
import MySQLdb as mdb
import sys
import geojson
from geojson import Point, Feature, FeatureCollection
import plotly.plotly as py
from plotly.graph_objs import *
import pandas as pd
import time
import datetime
from datetime import timedelta
import numpy as np

class Plot(object):
	def __init__(self):
		self.db = mdb.connect("127.0.0.1",
						"ne170group",
						"ne170groupSpring2015",
						"dosimeter_network")
		self.cursor = self.db.cursor() # prepare a cursor object using cursor() method
		# Titles of plots on Plot.ly
		self.cpmTitle = 'Counts per minute (CPM)'
		self.remTitle = 'mREM/hr'
		self.usvTitle = 'uSv/hr'
		# Other global variables
		self.stationRows = ''
		self.stationRowArrayList = []
		self.featureList = []
		self.dataRowArrayList = []
		self.dataForEachStationList =[]
		self.stationRow = 0
		self.filename = ''
		# Plot.ly sign in using ne170 login details
		py.sign_in('ne170','ilo0p1671e')
		############################
		#### ~~~ ATTENTION ~~~ #####
		self.KEPLER_USERNAME = 'nav'
		#### ~~~~~~~~~~~~~~~~~ #####
		############################

	def getStationInfo(self):
		# Get number of stations, station name, longitude, latitude, CPM to mRem and uSv conversion calibration factors
		self.cursor.execute("SELECT ID, `Name`, Lat, `Long`, cpmtorem, cpmtousv \
							FROM dosimeter_network.stations;") # Name & Long are reserved words apparently, need `...`
		self.stationRows = self.cursor.fetchall()

	def setStationInfo(self,i):
		self.stationRowArrayList.append((i[0], i[1], float(i[2]), float(i[3]), float(i[4]), float(i[5])))

	def setStationInfoForAll(self):
		for i in self.stationRows:
			self.setStationInfo(i)

	def getDataFromDB(self,stationID,startTime,endTime):
		try:
			self.cursor.execute("SELECT receiveTime, cpm, cpmError \
								FROM dosnet \
								INNER JOIN stations \
								ON dosnet.stationID=stations.ID \
								WHERE `stations`.`Name`='%s' \
								AND receiveTime BETWEEN '%s' \
								AND '%s';" % (stationID, startTime, endTime))
			dosePerStation = self.cursor.fetchall()
		except Exception as e:
			print 'Could not get data from DB:' + str(e)
		# Populate time-restricted row array list of data
		# Panda code to get SQL tuples of tuples into panda df tables
		df = pd.DataFrame( [[ij for ij in i] for i in dosePerStation] )
		df.rename(columns={0:'receiveTime', 1:'CPM', 2:'cpmError'},
					inplace=True)
		df.set_index(df['receiveTime'], inplace=True)
		del df['receiveTime']
		return df

	# [unit] over numberOfSeconds for a specific named station [stationID]
	def makePlot(self, stationID, unit, error, plotTitle, df, plength):
		try:
			t0 = time.time()
			# make the filename for Plot.ly export
			# current station ID
			fname = (str(stationID)+'_'+unit+'_'+plength)
			# setup the plot.ly plot type as a Scatter with points with panda DataFrames (df)
			trace = Scatter(
				x = df.index,
				y = df[unit], # changes depending on which units you are plotting
				mode = 'lines+markers',
				error_y = ErrorY(
					type = 'data',
					array = df[error],
					visible = True
					)
				)
			fontPref = Font(family = 'Arial, monospace',
							size = 16,
							color = '#000000')
			layout = Layout(title = (str(stationID)+' '+unit+' '+plength),
							xaxis = XAxis(title = 'Time (PDT)',
							rangemode = 'nonzero',
							autorange = True),
							yaxis = YAxis(title = plotTitle),
							font = fontPref)
			# Plot.ly export, doesn't open a firefox window on the server
			plotURL = py.plot(Figure(data = Data([trace]),
									layout = layout),
									filename = fname,
									auto_open = False)
			print fname, 'Plot.ly:',("%.2f" % (time.time() - t0)), 's'
			return plotURL
		except (KeyboardInterrupt, SystemExit):
			sys.exit(0)
		except (Exception) as e:
			raise e
			print ('This '+unit+' plot failed - '+fname)

	def printPlotFail(self,error):
		print 'Plotting failed'
		print str(error)

	def printFeatureFail(self,error):
		print 'Iterative feature creation failed'
		print str(error)

	def setFeature(self,point,name,plength,latestDose,latestTime,URLlist):
		properties = {	'Name': name,
						'Latest dose (CPM)': latestDose[0],
						'Latest dose (mREM/hr)': latestDose[1],
						'Latest dose (&microSv/hr)': latestDose[2],
						'Latest measurement': str(latestTime),
						('URL_CPM_'+plength[0]): URLlist[0][0],
						('URL_REM_'+plength[0]): URLlist[0][1],
						('URL_USV_'+plength[0]): URLlist[0][2],
						('URL_CPM_'+plength[1]): URLlist[1][0],
						('URL_REM_'+plength[1]): URLlist[1][1],
						('URL_USV_'+plength[1]): URLlist[1][2],
						('URL_CPM_'+plength[2]): URLlist[2][0],
						('URL_REM_'+plength[2]): URLlist[2][1],
						('URL_USV_'+plength[2]): URLlist[2][2],
						('URL_CPM_'+plength[3]): URLlist[3][0],
						('URL_REM_'+plength[3]): URLlist[3][1],
						('URL_USV_'+plength[3]): URLlist[3][2]
					}
		feature = Feature(geometry = point, properties = properties)
		self.featureList.append(feature)

	def getNumberOfStations(self):
		return range(len(self.stationRows))

	def makeUnitPlots(self,latestTime,latestStationID,calibrationCPMtoREM,calibrationCPMtoUSV,pointLatLong,plotLengthString,time):
		if latestTime == '':
			print '~~~~ WARNING: Finished list? - No data for this station.'
		else:
			try:
				# Note the negative --> means that we're looking at the past relative to the latest measurement stored in the DB
				startPlotTime = latestTime + datetime.timedelta(seconds = -time)
			except Exception as e:
				print 'There probably isn\'t any data from this time.\n' + str(e)
		df = self.getDataFromDB(latestStationID,startPlotTime,latestTime)
		# Data reduction algorithm
		resample_method = {'Year':'D',
							'Month': '6H',
							'Day': '5min',
							'Hour': '5min'}
		df_avg = df.resample(resample_method[plotLengthString], how="mean")
		# Expands dataframe to include other units and their associated errors
		df_avg['REM'] = df_avg['CPM'] * calibrationCPMtoREM
		df_avg['USV'] = df_avg['CPM'] * calibrationCPMtoUSV
		df_avg['remError'] = df_avg['cpmError'] * calibrationCPMtoREM
		df_avg['usvError'] = df_avg['cpmError'] * calibrationCPMtoUSV
		plotLengthString = 'Last ' + plotLengthString
		try:
			urlList = []
			# Plot markers & lines - Plot.ly to URLs
			# CPM over numberOfSeconds for all stations
			urlA = self.makePlot(latestStationID,'CPM','cpmError',self.cpmTitle,df_avg,plotLengthString)
			# mREM/hr over numberOfSeconds for all stations
			urlB = self.makePlot(latestStationID,'REM','remError',self.remTitle,df_avg,plotLengthString)
			# uSv/hr over numberOfSeconds for all stations
			urlC = self.makePlot(latestStationID,'USV','usvError',self.usvTitle,df_avg,plotLengthString)
			tempRow = urlA,urlB,urlC
			urlList.extend(tempRow)
		except Exception as e:
			self.printPlotFail(e)
		return urlList

	def plotAll(self): # main()
		# Constants used for plotting clarity
		secondsInYear =  31557600 	# 365.23 days
		secondsInMonth = 2592000 	# 30 days
		secondsInWeek =  604800 	# 7 days
		secondsInDay = 	 86400 		# 24 hours
		secondsInHour =  3600 		# 60 minutes
		secondsInMinute= 60 		# Duh
		plotLength = ('Hour','Day','Month','Year')
		station = 0
		for station in self.getNumberOfStations():
			# builds up a tuple coordinates of the stations, longitude & latitude
			longlat = [ self.stationRowArrayList[station][3], self.stationRowArrayList[station][2]]
			point = Point(longlat)
			# gets the stationID for insertion into the features
			stationID = self.stationRowArrayList[station][1]
			# get latest dose (CPM) and time for that measurement in the loop so we can display in exported GeoJSON file
			self.cursor.execute("SELECT Name, receiveTime, cpm, cpmtorem, cpmtousv \
								FROM dosnet \
								INNER JOIN stations \
								ON dosnet.stationID = stations.ID \
								WHERE receiveTime = \
									(SELECT MAX(receiveTime) \
									FROM dosnet \
									INNER JOIN stations \
									ON dosnet.stationID = stations.ID  \
									WHERE Name='%s') AND Name='%s';" \
									% (stationID, stationID))
			dtRows = self.cursor.fetchall() # dtRows --> Data & time rows
			for i in dtRows:
				(LName, LTime, LDose, Lcpmtorem, Lcpmtousv) = i # L --> Latest ...
				LDose = LDose, LDose*Lcpmtorem, LDose*Lcpmtousv
				urlList = []
				urlA = self.makeUnitPlots(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[0],secondsInHour)
				urlB = self.makeUnitPlots(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[1],secondsInDay)
				urlC = self.makeUnitPlots(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[2],secondsInMonth)
				urlD = self.makeUnitPlots(LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[3],secondsInYear)
				urlRow = (urlA, urlB, urlC, urlD)
				urlList.extend(urlRow)
				# Make feature - iterating through each
				try:
					self.setFeature(point,LName,plotLength,LDose,LTime,urlList)
				except Exception as e:
					self.printFeatureFail(e)

	def makeGeoJSON(self):
		featureCollection = FeatureCollection(self.featureList)
		# Export dump to GeoJSON file
		dump = str(geojson.dumps(featureCollection))
		openfile = open('output.geojson','w')
		try:
			print >> openfile, dump
		except Exception as e:
			print (str(e))
		finally:
			openfile.close()

	def closeDB(self):
		self.db.close() # Disconnect from DB server

	def scpToWebServer(self): # copy to webserver - DECF Kepler
		# Must be run under 'dosenet' linux user so that the SSH keypair setup between GRIM & DECF Kepler works without login
		# Not ideal: uses Joey's account for the SCP
		# Will be: $ scp ... jcurtis@kepler/.../
		#			         =======
		outputLocation = " output.geojson "
		webServerLocation = " '%s'@kepler.berkeley.edu:/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/ " % (self.KEPLER_USERNAME)
		command = "scp" + outputLocation + webServerLocation
		try:
			os.system(command)
		except Exception as e:
			print 'Network Error: Cannot SCP to Kepler'
			raise e

	def printEndMessage(self):
		print ' Navrit Bal - time is '+ getDateTime()

def getDateTime():
	return str(datetime.datetime.now())

if __name__ == '__main__':
	"""Main makeGeoJSON function 

	Returns:
		Plot.ly graphs - Updates dose over time graphs on plot.ly for ALL stations
		output.geojson - GeoJSON file for the web page >> copied to Kepler (web server) via SCP (SSH CP) command
	"""
	t0 = time.time()
	plot = Plot()
	plot.getStationInfo()
	plot.setStationInfoForAll()
	plot.plotAll()
	plot.closeDB()
	plot.makeGeoJSON()
	plot.scpToWebServer()
	plot.printEndMessage()
	print 'Total run time:', ("%.1f" % (time.time() - t0)), 's'
