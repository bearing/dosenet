from __future__ import print_function
import MySQLdb as mdb
import socket
from random import randint
#connect to db
#db = mdb.connect("localhost","root","password","testdb" )
#db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )

#setup cursor
#cursor = db.cursor()

UDP_IP = "192.168.1.101"
UDP_PORT = 5005

sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))

data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
print("received message:", data)
new = data.split(",")
#stationID = new[0]
#use station id 1 for testing purposes
stationID = new[0]
cpm = float(new[1])
cpm_err = float(new[2])
time = new[3]
errorFlag = new[4]

print(stationID, cpm, cpm_err, time, errorFlag)
#cursor.execute("""INSERT INTO dosnet(stationID, cpm, rem, usv, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(stationID,cpm,rem,usv,errorFlag))

#db.commit()
#db.close()

# PARSE 'data' STRING into a 'tuple' using commas --> receiveTime, stationID, cpm (the dose, no conversion required), rem (cpm*8.76), usv (usv=rem), errorFlag (set # to 0)
# a  = hsdhfsdv , jbfdv , ahdjas

