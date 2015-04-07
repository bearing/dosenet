import MySQLdb as mdb
import socket
from random import randint
#connect to db
#db = mdb.connect("localhost","root","password","testdb" )
db = mdb.connect("localhost","ne170group","ne170groupSpring2015","dosimeter_network" )

#setup cursor
cursor = db.cursor()

UDP_IP = "192.168.1.101"
UDP_PORT = 5005

sock = socket.socket(socket.AF_INET, # Internet
                      socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))
errorFlag = 1;

while True:
     data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
     #print "received message:", data
     new = data.split(",")
     stationID = new[0]
     time = new[1].replace(microsecond=0)
     cpm = float(new[2])
     rem = cpm * 8.76
     rad = rem
     errorFlag = randint(0,1)
     cursor.execute("""INSERT INTO dosnet(receiveTime, stationID, cpm, rem, rad, errorFlag) VALUES (%s,%s,%s,%s,%s,%s);""",(time, stationID,cpm,rem,rad,errorFlag))



db.close() 

     # PARSE 'data' STRING into a 'tuple' using commas --> receiveTime, stationID, cpm (the dose, no conversion required), rem (cpm*8.76), rad (rad=rem), errorFlag (set to 0)
     #a  = hsdhfsdv , jbfdv , ahdjas 
     
