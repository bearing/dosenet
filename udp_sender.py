import socket
import time

UDP_IP = "192.168.1.101"
UDP_PORT = 5005


print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT
print "message:", MESSAGE

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP

while True:
	cpm = random.uniform(1,2)
	time = time.strftime("%H:%M:%S")
	package = "1" + "," + str(cpm) + "," + time	
    sock.sendto(package, (UDP_IP, UDP_PORT))
    print "Package sent"
    time.sleep(1)