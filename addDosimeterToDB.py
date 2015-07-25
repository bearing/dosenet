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
import itertools

class Parser:
	def __init__(self):
		parser = argparse.ArgumentParser()
		parser.add_argument('--ID',type=int,nargs=1,required=False,
			help='Auto generated if not manually set. Does not compensate for \
			collisions that you may make.')
		parser.add_argument('--name',type=str,nargs=1,required=True,
			help='')
		parser.add_argument('--latlong',type=float,nargs=2,required=True,
			help='')
		parser.add_argument('--conv',type=float,nargs=2,required=True,
			help='')
		self.args = parser.parse_args()

class DBTool:
	def __init__(self,name,lat,lon,cpmtorem,cpmtousv,*ID):
		self.db = mdb.connect("localhost", # Open database connection
						"ne170group",
						"ne170groupSpring2015",
						"dosimeter_network")
		try:
			self.ID = ID
		except Exception as e:
			print 'Auto generating ID, good choice.'
		self.name = name
		self.lat = lat
		self.lon = lon
		self.cpmtorem = cpmtorem
		self.cpmtousv = cpmtousv
		self.cursor = self.db.cursor() # prepare a cursor object using cursor() method
		self.md5hash = ''
		if ID:
			self.addDosimeterWithID()
		else:
			self.addDosimeter()

	def addDosimeter(self): # Adds a row to dosimeter_network.stations
		sql = "INSERT INTO stations (`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`,IDLatLongHash) \
				VALUES ('%s','%s','%s','%s','%s','This should not be here :(');" \
				% (self.name, self.lat, self.lon, self.cpmtorem, self.cpmtousv)
		self.runSQL(sql)
		self.main()

	def addDosimeterWithID(self):
		sql = "INSERT INTO stations (`ID`,`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`,IDLatLongHash) \
				VALUES ('%s','%s','%s','%s','%s','%s','This should not be here :(');" \
				% (self.ID, self.name, self.lat, self.lon, self.cpmtorem, self.cpmtousv)
		self.runSQL(sql)
		self.main()

	def getID(self,name):
		# The database uses auto-incremented ID numbers so we need to get
		# the ID from the `dosimeter_network.stations` table for when we
		# add the hash
		# RUN "SELECT ID  FROM stations WHERE name = 'SOME NAME';"
		sql = "SELECT ID FROM stations WHERE name = '%s';" % (self.name)
		self.ID = self.runSQL(sql,firstelement=True)
		if 1 <= self.ID <= 3:
			print 'Check the DB (stations) - there\'s probably an ID collision'
		elif self.ID <= 0:
			print 'ID less than 0?? There\'s a problem afoot'
		elif self.ID is None:
			print 'ID is None... Byyeeeeeeee'
			sys.exit(1)
		else:
			print 'ID looks good'

	def getHash(self):
		# RUN "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`))
		# 		FROM stations
		# 		WHERE `ID` = $$$ ;"
		sql = "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`)) FROM stations \
				WHERE `ID` = '%s' ;" % (self.ID)
		self.md5hash = self.runSQL(sql,firstelement=True)

	def setHash(self): # Sets a MD5 hash of the ID, Latitude & for security reasons...
		# RUN "UPDATE stations
		#		SET IDLatLongHash = 'SOME MD5 HASH'
		# 		WHERE ID = $$$ ;"
		sql = "UPDATE stations SET IDLatLongHash = '%s' \
		 		WHERE ID = '%s';" % (self.md5hash, self.ID)
		self.runSQL(sql)

	def getNewStation(self):
		sql = "SELECT * FROM stations WHERE ID = '%s';" % (self.ID)
		return self.runSQL(sql,getNewStation=True)

	def checkIfDuplicate(self):
		sql = ";"
		warning_msg = 'This should not be here :('
		if any(str(warning_msg) in i for i in self.new_station):
			print 'ERROR: Duplicate detected, not commiting changes. Byyeeeeeeee'
			return True
		else:
			print 'Good news: no duplicates'
			return False

	def runSQL(self,sql,firstelement=False,getNewStation=False):
		print '\t\t\t SQL: ',sql
		try:
			self.cursor.execute(sql)
			if firstelement:
				result = self.cursor.fetchall()[0][0]
				return result
			if getNewStation:
				result = self.cursor.fetchall()[0]
				return result
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception, e:
			print sql
			raise e

	def main(self):
		self.duplicate = True
		try:
			self.getID(self.name)
			self.getHash()
			self.setHash()
			self.new_station = self.getNewStation()
			print self.new_station
			if not self.checkIfDuplicate():
				self.db.commit()
		except Exception as e:
			print '\t ~~~~ FAILED ~~~~'
			raise e

if __name__=="__main__":
	parse = Parser()
	name = parse.args.name[0]
	print name
	lat = parse.args.latlong[0]
	lon = parse.args.latlong[1]
	cpmtorem = parse.args.conv[0]
	cpmtousv = parse.args.conv[1]
	if not parse.args.ID:
		dbtool = DBTool(name,lat,lon,cpmtorem,cpmtousv)
	else:
		ID = parse.args.ID[0]
		print 'Forced ID: ', ID
		dbtool = DBTool(name,lat,lon,cpmtorem,cpmtousv,ID)
