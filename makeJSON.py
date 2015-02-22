#!/usr/bin/python2.7

import MySQLdb as mdb
import sys
import json
import collections 

# Open database connection
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
#cursor.execute("SELECT VERSION()")
# Fetch a single row using fetchone() method.
#data = cursor.fetchone()
#print "Database version : %s " % data
 
        
cursor.execute(" SELECT receiveTime, stationID, doseRate, errorFlag FROM dosnet; ")

rows = cursor.fetchall()
  
#for rows in rows:
#  data = rows

#print data[0], data[1], data[2], data[3]


# Convert query to row arrays
rowarray_list = []
for row in rows:
    t = (str(row[0]), row[1], row[2], row[3])
    rowarray_list.append(t)

j = json.dumps(rowarray_list)
rowarrays_file = 'rowarray.geojson'
f = open(rowarrays_file, 'w')
print >> f, j

# Convert query to objects of key-value pairs
objects_list = []
for row in rows:
    d = collections.OrderedDict()
    d['receiveTime'] = str(row[0])
    d['stationID'] = row[1]
    d['doseRate'] = row[2]
    d['errorFlag'] = row[3]
    objects_list.append(d)

j = json.dumps(objects_list)
objects_file = 'student_objects.geojson'
f = open(objects_file, 'w')
print >> f, j



print("Succuss!! closing connection betches")
# disconnect from server
db.close()