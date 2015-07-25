#!/usr/bin/env python
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created: 		Mon 15/06/15
# Last updated: Fri 24/06/15
#################
## Run on GRIM ##
#################

import sys
import MySQLdb as mdb
import argparse


class DBTool(object):
	def parseArguments(self):
		parser = argparse.ArgumentParser()
		parser.add_argument('--ID',type=int,nargs=1,required=False,\
			help='Auto generated if not manually set. Does not compensate for \
			collisions that you may make.')
		parser.add_argument('--name',type=str,nargs=1,required=True,\
			help='')
		parser.add_argument('--latlong',type=float,nargs=2,required=True,\
			help='')
		parser.add_argument('--conv',type=float,nargs=2,required=True,\
			help='')
		self.args = parser.parse_args()
		print 'ID: '
		ID = parse.args.ID
		print ID
		name = parse.args.name
		print name
		lat = parse.args.latlong[0]
		lon = parse.args.latlong[1]
		cpmtorem = parse.args.conv[0]
		cpmtousv = parse.args.conv[1]
		self.start()
	def start(self,ID,name,lat,lon,cpmtorem,cpmtousv):
		self.db = mdb.connect("localhost", # Open database connection
						"ne170group",
						"ne170groupSpring2015",
						"dosimeter_network")
		self.cursor = db.cursor() # prepare a cursor object using cursor() method
		self.ID = ID
		self.name = name
		self.lat = lat
		self.lon = lon
		self.cpmtorem = cpmtorem
		self.cpmtousv = cpmtousv
		self.md5hash = ''
		if parse.args.ID:
			dbTool.addDosimeterWithID()
		else:
			dbTool.addDosimeter()
	def addDosimeter(self): # Adds a row to dosimeter_network.stations
		sql = "INSERT INTO stations (`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s');" \
				% (self.name, self.lat, self.lon, self.cpmtorem, self.cpmtousv)
		self.runSQL(sql)
		self.getID(self.name)
		self.getHash()
		self.setHash()
	def addDosimeterWithID(self):
		sql = "INSERT INTO stations (`ID`,`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s','%s');" \
				% (self.ID, self.name, self.lat, self.lon, self.cpmtorem, self.cpmtousv)
		self.runSQL(sql)
		self.getID(self.name)
		self.getHash()
		self.setHash()
	def getID(self,name):
		# The database uses auto-incremented ID numbers so we need to get
		# the ID from the `dosimeter_network.stations` table for when we
		# add the hash
		# RUN "SELECT ID  FROM stations WHERE name = 'SOME NAME';"
		sql = "SELECT ID FROM stations WHERE name = '%s';" % (self.name)
		self.ID = self.runSQL(sql)
		if self.ID <= 3:
			print 'Check the DB (stations) - there\'s probably an ID collision'
	def getHash(self):
		# RUN "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`))
		# 		FROM stations
		# 		WHERE `ID` = $$$ ;"
		sql = "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`)) FROM stations \
				WHERE `ID` = '%s' ;" % (self.ID)
		self.md5hash = self.runSQL(sql)
	def setHash(self): # Sets a MD5 hash of the ID, Latitude & for security reasons...
		# RUN "UPDATE stations
		#		SET IDLatLongHash = 'SOME MD5 HASH'
		# 		WHERE ID = $$$ ;"
		sql = "UPDATE stations SET IDLatLongHash = '%s' \
		 		WHERE ID = '%s';" % (self.md5hash, self.ID)
		self.runSQL(sql)
	def runSQL(self,sql):
		try:
			self.cursor.execute(sql)
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception, e:
			raise e

if __name__ == "main":
	dbtool = DBTool()
	dbtool.parseArguments()
	dbtool.start()
