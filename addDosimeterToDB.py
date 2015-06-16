#!/usr/bin/python
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created: 		Mon 15/06/15
# Last updated: Mon 15/06/15
#################
## Run on GRIM ##
#################

import sys
import MySQLdb as mdb
arg = []; arg.extend(sys.argv)

class DBTool(object):
	# Open database connection
	db = mdb.connect("localhost",
					"ne170group",
					"ne170groupSpring2015",
					"dosimeter_network")
	# prepare a cursor object using cursor() method
	cursor = db.cursor()
	ID = ''
	name = ''
	md5hash = ''

	def __init__(self):
		pass

	def addDosimeter(self,name,lat,lon,cpmtorem,cpmtousv):
		# Run some MySQL script
		# Adds a row to dosimeter_network.stations
		self.name = name
		sql = "INSERT INTO stations (`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s');"
				% (name, lat, lon, cpmtorem, cpmtousv)
		runSQL(sql)

	def addDosimeterWithID(self,ID):
		self.name = name
		sql = "INSERT INTO stations (`ID`,`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`) \
				VALUES ('%s','%s','%s','%s','%s','%s');"
				% (ID, name, lat, lon, cpmtorem, cpmtousv)
		runSQL(sql)

	def removeDosimeterDataByID(self,ID):


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
		# Adds a MD5 hash of the ID, Latitude & Longitude
		# for security reasons...
		# RUN "UPDATE stations
		#		SET IDLatLongHash = 'SOME MD5 HASH'
		# 		WHERE ID = $$$ ;"
		sql = "UPDATE stations SET IDLatLongHash = '%s' \
		 		WHERE ID = '%s';" % (self.md5hash, self.ID)
		runSQL(sql)
		import datetime
		print 'Time is: ' + str(datetime.datetime.now())

	def runSQL(self,sql):
		try:
			DBTool.cursor.execute(sql)
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