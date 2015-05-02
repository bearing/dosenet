import MySQLdb as mdb
import socket
import time
UDP_IP = "192.168.1.101"
UDP_PORT = 5005
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))
while True:
	mess, addr = sock.recvfrom(1024)
	print mess, addr
	time.sleep(0.1)