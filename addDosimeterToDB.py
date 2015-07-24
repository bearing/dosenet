#!/usr/bin/env python
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created: 		Mon 15/06/15
# Last updated: Tue 16/06/15
#################
## Run on GRIM ##
#################

import sys
import MySQLdb as mdb
import argparse

class DBTool(object):
	def __init__(self):
		self.db = mdb.connect("localhost", # Open database connection
						"ne170group",
						"ne170groupSpring2015",
						"dosimeter_network")
		self.cursor = db.cursor() # prepare a cursor object using cursor() method
		self.ID = ''
		self.name = ''
		self.md5hash = ''

	def addDosimeter(self,name,lat,lon,cpmtorem,cpmtousv):
		# Adds a row to dosimeter_network.stations
		self.name = name
		sql = "INSERT INTO stations (`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s');" % (name, lat, lon, cpmtorem, cpmtousv)
		runSQL(sql)

	def addDosimeterWithID(self,ID,name,lat,lon,cpmtorem,cpmtousv):
		sql = "INSERT INTO stations (`ID`,`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s','%s');" % (ID, name, lat, lon, cpmtorem, cpmtousv)
		runSQL(sql)

	def getID(self,name):
		# The database uses auto-incremented ID numbers so we need to get
		# the ID from the `dosimeter_network.stations` table for when we
		# add the hash
		# RUN "SELECT ID  FROM stations WHERE name = 'SOME NAME';"
		sql = "SELECT ID FROM stations WHERE name = '%s';" % (name)
		self.ID = runSQL(sql)
		if ID <= 3:
			print 'Check the DB (stations) - there\'s probably an ID collision'

	def getHash(self):
		# RUN "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`))
		# 		FROM stations
		# 		WHERE `ID` = $$$ ;"
		sql = "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`)) FROM stations \
				WHERE `ID` = '%s' ;" % (DBTools.ID)
		self.md5hash = runSQL(sql)

	def setHash(self):
		# Sets a MD5 hash of the ID, Latitude & for security reasons...
		# RUN "UPDATE stations
		#		SET IDLatLongHash = 'SOME MD5 HASH'
		# 		WHERE ID = $$$ ;"
		sql = "UPDATE stations SET IDLatLongHash = '%s' \
		 		WHERE ID = '%s';" % (self.md5hash, self.ID)
		runSQL(sql)

	def runSQL(self,sql):
		try:
			self.cursor.execute(sql)
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception, e:
			raise e

def main(argv):
	# Parse arguments into variables
	print arg
	if arg[1] == 'v':
		# Verbose mode
		name = arg[2]
		lat = arg[3]
		lon = arg[4]
		cpmtorem = arg[5]
		cpmtousv = arg[6]
	else:
		# Normal mode
		name = arg[1]
		lat = arg[2]
		lon = arg[3]
		cpmtorem = arg[4]
		cpmtousv = arg[5]

	dbTool = DBTool()
	dbTool.addDosimeter(name,lat,lon,cpmtorem,cpmtousv)
	dbTool.getID(name)
	dbTool.getHash()
	dbTool.setHash()

if __name__ == "__main__":
	main(sys.argv[1:])
