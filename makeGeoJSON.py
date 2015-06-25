#!/usr/bin/python
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dev_makeGeoJSON.py (functional) Sat 09/05/15
# Created: 		Sun 7/06/15
# Last updated: Thu 35/06/15
#################
## Run on GRIM ##
#################

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
#arg = []; arg.extend(sys.argv)

class Plot(object):
	def __init__(self):
		#------------------------#
		# Global class variables #
		#------------------------#
		# Open database connection
		self.db = mdb.connect("localhost",
						"ne170group",
						"ne170groupSpring2015",
						"dosimeter_network")
		# prepare a cursor object using cursor() method
		self.cursor = db.cursor()
		# Titles of plots on Plot.ly
		self.cpmTitle = 'Counts per minute (CPM)'
		self.remTitle = 'mREM/hr'
		self.usvTitle = 'uSv/hr'
		# Should be retrieved from DB
		self.calibrationFactor_cpm_to_rem = 0.01886792; # COMPLETELY RANDOM (1/53)
		self.calibrationFactor_cpm_to_usv = 0.00980392; # COMPLETELY RANDOM (1/102)
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
	def getStationInfo(self):
		# Get number of stations, station name, longitude, latitude, CPM to mRem and uSv conversion calibration factors
		self.cursor.execute("SELECT ID, `Name`, Lat, `Long`, cpmtorem, cpmtousv \
					FROM dosimeter_network.stations;") # Name & Long are reserved words apparently, need `...`
		self.stationRows = Plot.cursor.fetchall()
	def setStationInfo(self,i):
		#self.i = i
		self.stationRowArrayList.append((i[0], i[1], i[2], i[3], i[4], i[5]))
		return
	def setStationInfoForAll(self):
		for i in self.stationRows:
			self.setStationInfo(self,i)
	def getDataFromDB(self,stationID,startTime,endTime):
		try:
			self.cursor.execute("SELECT receiveTime, cpm, cpmError \
								FROM dosnet \
								INNER JOIN stations \
								ON dosnet.stationID=stations.ID \
								WHERE `stations`.`Name`='%s' \
								AND receiveTime BETWEEN '%s' \
								AND '%s';" % (stationID, startTime, endTime))
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception as e:
			print ('Could not get data from DB:' + str(e))
		dosePerStation = self.cursor.fetchall()
		#
		# Populate time-restricted row array list of data
		# Panda code to get SQL tuples of tuples into panda df tables which are 
		# WAAAY easier to work with (and apparently are faster?)
		df = pd.DataFrame( [[ij for ij in i] for i in dosePerStation] )
		df.rename(columns={0:'receiveTime', 1:'CPM', 2:'cpmError'}, 
					inplace=True)
		return df
	def reduceData(self,df):
		t0 = time.time()
		i = 0;
		while len(df.index) > 200: # Reduce data for plotting
			i += 1
			df = df[::4]
		if i!=0:
			print ('Data was quartered' ,i,'times - ', ("%.4f" % (time.time() - t0)),'s')
		return df
	# [unit] over numberOfSeconds for a specific named station [stationID]
	def makePlot(self,stationID,unit,error,plotTitle,df,plength):
		try:
			t0 = time.time()
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
			plotURL = py.plot(Figure(data=Data([trace]), 
									layout=layout), 
									filename=fname, 
									auto_open=False)
			print (fname, 'Plot.ly:',("%.2f" % (time.time() - t0)), 's')
			return plotURL
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			print ('This '+unit+' plot failed - '+fname)
	def printPlotFail(self,error):
		print ('Plotting failed')
		print (error)
	def printFeatureFail(self,error):
		print ('Iterative feature creation failed')
		print (error)
	def setFeature(self,point,name,plength,latestDose,latestTime,URLlist):
		properties = {	'Name': name, 
						'Latest dose (CPM)': latestDose[0],
						'Latest dose (mREM/hr)': latestDose[1],
						'Latest dose (&microSv/hr)': latestDose[2], 
						'Latest measurement': str(self.latestTime),
						('URL_CPM_'+plength[0]): URLlist[0][0], 
						('URL_REM_'+plength[0]): URLlist[0][1], 
						('URL_USV_'+plength[0]): URLlist[0][2],
						('URL_CPM_'+plength[1]): URLlist[1][0], 
						('URL_REM_'+plength[1]): URLlist[1][1], 
						('URL_USV_'+plength[1]): URLlist[1][2], 
						('URL_CPM_'+plength[2]): URLlist[2][0], 
						('URL_REM_'+plength[2]): URLlist[2][1], 
						('URL_USV_'+plength[2]): URLlist[2][2]
					}
		feature = Feature(geometry=point, properties=properties)
		self.featureList.append(feature)
	def getNumberOfStations(self):
		return range(len(self.stationRows))
	def makeAllPlots(self,latestTime,latestStationID,calibrationCPMtoREM,calibrationCPMtoUSV,pointLatLong,plotLengthString,time):
		plotLengthString = 'Past_' + plotLengthString
		if latestTime == '':
			print ('Finished list?')
		else:
			try:
				# Note the negative --> means that we're looking at the past relative to the latest measurement stored in the DB
				startPlotTime = latestTime + datetime.timedelta(seconds=-time)
			except (KeyboardInterrupt, SystemExit) as e:
				raise e
				sys.exit(0)
			except Exception as e:
				print ('There probably isn\'t any data from this time.\n' + str(e))
		df = self.getDataFromDB(self,latestStationID,startPlotTime,latestTime)
		df = self.reduceData(self,df)
		# Expands dataframe to include other units and their associated errors
		df['REM'] = df['CPM'] * calibrationCPMtoREM
		df['USV'] = df['CPM'] * calibrationCPMtoUSV
		df['remError'] = df['cpmError'] * calibrationCPMtoREM    # CHECK WITH JOEY ETC
		df['usvError'] = df['cpmError'] * calibrationCPMtoUSV    # PRETTY SURE THIS IS INCORRECT
		try:
			urlList = []
			# Plot markers & lines - Plot.ly to URLs
			# CPM over numberOfSeconds for all stations
			urlA = self.makePlot(self,latestStationID,'CPM','cpmError',self.cpmTitle,df,plotLengthString)
			# mREM/hr over numberOfSeconds for all stations
			urlB = self.makePlot(self,latestStationID,'REM','remError',self.remTitle,df,plotLengthString)
			# uSv/hr over numberOfSeconds for all stations
			urlC = self.makePlot(self,latestStationID,'USV','usvError',self.usvTitle,df,plotLengthString)
			tempRow = urlA,urlB,urlC
			urlList.extend(tempRow)
		except (KeyboardInterrupt, SystemExit):
			sys.exit(0)
		except Exception as e:
			self.printPlotFail(self,e)
		return urlList
	# Used to be main()
	def plotAll(self):
		# Constants used for plotting clarity
		secondsInYear =  31557600 	# 365.23 days
		secondsInMonth = 2592000 	# 30 days
		secondsInWeek =  604800 	# 7 days
		secondsInDay = 	 86400 		# 24 hours
		secondsInHour =  3600 		# 60 minutes
		secondsInMinute= 60 		# Duh
		plotLength = ('Hour','Day','Month')
		#######################################################
		## Iterate through to define all features (stations) ##
		#######################################################
		for station in self.getNumberOfStations(self):
			# builds up a tuple coordinates of the stations, longitude & latitude
			longlat = [ self.stationRowArrayList[station][3], self.stationRowArrayList[station][2]]
			point = Point(longlat)
			# gets the stationID for insertion into the features
			stationID = self.stationRowArrayList[station][1]
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
			self.cursor.execute(sqlString)
			sqlString = ''
			# dtRows --> Data & time rows
			dtRows = self.cursor.fetchall()
			for i in dtRows:
				# L --> Latest ...
				LName, LTime, LDose, Lcpmtorem, Lcpmtousv = i
				LDose = LDose, LDose*Lcpmtorem, LDose*Lcpmtousv
				urlList = []
				urlA = self.makeAllPlots(self,LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[0],secondsInHour)
				urlB = self.makeAllPlots(self,LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[1],secondsInDay)
				urlC = self.makeAllPlots(self,LTime,LName,Lcpmtorem,Lcpmtousv,point,plotLength[2],secondsInMonth)
				urlRow = (urlA,urlB,urlC)
				urlList.extend(urlRow)
				# Make feature - iterating through each 
				try:
					self.setFeature(self,point,LName,plotLength,LDose,LTime,urlList)
				except (KeyboardInterrupt, SystemExit):
					sys.exit(0)
				except Exception as e:
					self.printFeatureFail(self,e)
	def makeGeoJSON(self):
		featureCollection = FeatureCollection(self.featureList)
		# Export dump to GeoJSON file
		dump = geojson.dumps(featureCollection)
		f = open('output.geojson', 'w')
		try:
			print >> (f, dump)
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception as e:
			print (str(e))
	def closeDB(self):
		# Disconnect from DB server
		Plot.db.close()
	def scpToWebServer(self):
		# copy to webserver - DECF Kepler
		# Must be run under 'dosenet' linux user so that the SSH keypair setup between GRIM & DECF Kepler works without login
		# Not ideal: uses Joey's account for the SCP
		# Will be: $ scp ... jcurtis@kepler/.../
		#			         =======
		outputLocation = " output.geojson "
		webServerLocation = " nav@kepler.berkeley.edu:/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/ "
		command = "scp" + outputLocation + webServerLocation
		try:
			os.system(command)
		except Exception as e:
			print ('Network Error: Cannot SCP to Kepler')
			raise e
	def printEndMessage(self):
		print( u'\u00A9' +' Navrit Bal - time is '+ getDateTime())

def getDateTime():
	return str(datetime.datetime.now())

def main(argv):
	'''
	Main makeGeoJSON function 

	Parameters:
		argv - command line arguments. Could be used for choosing which timeframes to plot, eg. $ python makeGeoJSON.py month year
	Returns:
		Plot.ly graphs - Updates dose over time graphs on plot.ly for ALL stations
		output.geojson - GeoJSON file for the web page >> copied to Kepler (web server) via SCP (SSH CP) command
	'''
	t0 = time.time()
	plot = Plot()
	plot.getStationInfo()
	plot.setStationInfoForAll()	
	plot.plotAll()
	plot.closeDB()
	plot.makeGeoJSON()
	plot.scpToWebServer()
	plot.printEndMessage()
	print ('Total run time:', ("%.2f" % (time.time() - t0)), 's')

if __name__ == "__main__":
	main(sys.argv[1:])